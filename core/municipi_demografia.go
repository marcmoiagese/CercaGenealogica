package core

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type demografiaSummaryView struct {
	HasData         bool
	AnyMin          int
	AnyMax          int
	TotalNatalitat  int
	TotalMatrimonis int
	TotalDefuncions int
	Total           int
	UpdatedAt       string
}

const (
	demografiaYearMin = 1200
	demografiaYearMax = 2100
)

func (a *App) MunicipiDemografiaPage(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKeyIfLogged(w, r, permKeyTerritoriMunicipisView); !ok {
		return
	}
	munID := municipiIDFromPath(r.URL.Path)
	if munID <= 0 {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(munID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	user, _ := a.VerificarSessio(r)
	perms := db.PolicyPermissions{}
	if user != nil {
		perms = a.getPermissionsForUser(user.ID)
	}
	canManageTerritory := user != nil && a.hasPerm(perms, permTerritory)
	canModerate := user != nil && a.hasPerm(perms, permModerate)
	if mun.ModeracioEstat != "" && mun.ModeracioEstat != "publicat" && !(canManageTerritory || canModerate) {
		http.NotFound(w, r)
		return
	}
	canViewLlibres := user != nil && a.hasAnyPermissionKey(user.ID, permKeyDocumentalsLlibresView)
	canCreateLlibre := user != nil && a.hasAnyPermissionKey(user.ID, permKeyDocumentalsLlibresCreate)

	meta, err := a.DB.GetMunicipiDemografiaMeta(munID)
	if err != nil {
		Errorf("Error carregant demografia municipi %d: %v", munID, err)
	}
	summary := buildDemografiaSummary(meta)
	llibresURL := ""
	if canViewLlibres {
		llibresURL = "/documentals/llibres?municipi_id=" + strconv.Itoa(mun.ID)
	}

	data := map[string]interface{}{
		"Municipi":        mun,
		"Summary":         summary,
		"CanViewLlibres":  canViewLlibres,
		"CanCreateLlibre": canCreateLlibre,
		"LlibresURL":      llibresURL,
		"DemografiaMetaAPI":   "/api/municipis/" + strconv.Itoa(mun.ID) + "/demografia/meta",
		"DemografiaSeriesAPI": "/api/municipis/" + strconv.Itoa(mun.ID) + "/demografia/series",
		"CanRebuildDemografia": user != nil && a.hasPerm(perms, permModerate),
		"DemografiaRebuildAPI": "/api/admin/municipis/" + strconv.Itoa(mun.ID) + "/demografia/rebuild",
	}
	if user != nil {
		RenderPrivateTemplate(w, r, "municipi-demografia.html", data)
		return
	}
	RenderTemplate(w, r, "municipi-demografia.html", data)
}

func (a *App) municipiDemografiaMetaJSON(w http.ResponseWriter, r *http.Request, municipiID int) {
	if _, ok := a.requirePermissionKeyIfLogged(w, r, permKeyTerritoriMunicipisView); !ok {
		return
	}
	meta, err := a.DB.GetMunicipiDemografiaMeta(municipiID)
	if err != nil {
		http.Error(w, "failed to load meta", http.StatusInternalServerError)
		return
	}
	anyMin := 0
	anyMax := 0
	totalNat := 0
	totalMat := 0
	totalDef := 0
	updatedAt := ""
	if meta != nil {
		if meta.AnyMin.Valid {
			anyMin = int(meta.AnyMin.Int64)
		}
		if meta.AnyMax.Valid {
			anyMax = int(meta.AnyMax.Int64)
		}
		totalNat = meta.TotalNatalitat
		totalMat = meta.TotalMatrimonis
		totalDef = meta.TotalDefuncions
		if meta.UpdatedAt.Valid {
			updatedAt = meta.UpdatedAt.Time.Format(time.RFC3339)
		}
	}
	writeJSON(w, map[string]interface{}{
		"municipi_id": municipiID,
		"any_min":     anyMin,
		"any_max":     anyMax,
		"total": map[string]int{
			"natalitat":  totalNat,
			"matrimonis": totalMat,
			"defuncions": totalDef,
		},
		"updated_at": updatedAt,
	})
}

func (a *App) municipiDemografiaSeriesJSON(w http.ResponseWriter, r *http.Request, municipiID int) {
	if _, ok := a.requirePermissionKeyIfLogged(w, r, permKeyTerritoriMunicipisView); !ok {
		return
	}
	bucket := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("bucket")))
	if bucket != "decade" && bucket != "year" {
		bucket = "year"
	}
	from := parseFormInt(r.URL.Query().Get("from"))
	to := parseFormInt(r.URL.Query().Get("to"))
	if (from == 0 || to == 0) && municipiID > 0 {
		if meta, err := a.DB.GetMunicipiDemografiaMeta(municipiID); err == nil && meta != nil {
			if from == 0 && meta.AnyMin.Valid {
				from = int(meta.AnyMin.Int64)
			}
			if to == 0 && meta.AnyMax.Valid {
				to = int(meta.AnyMax.Int64)
			}
		}
	}
	if from > 0 && to > 0 && from > to {
		from, to = to, from
	}
	var rows []db.MunicipiDemografiaAny
	var err error
	if bucket == "decade" {
		rows, err = a.DB.ListMunicipiDemografiaDecades(municipiID, from, to)
	} else {
		rows, err = a.DB.ListMunicipiDemografiaAny(municipiID, from, to)
	}
	if err != nil {
		http.Error(w, "failed to load series", http.StatusInternalServerError)
		return
	}
	payloadRows := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		payloadRows = append(payloadRows, map[string]interface{}{
			"key":        row.Any,
			"natalitat":  row.Natalitat,
			"matrimonis": row.Matrimonis,
			"defuncions": row.Defuncions,
		})
	}
	writeJSON(w, map[string]interface{}{
		"bucket": bucket,
		"from":   from,
		"to":     to,
		"rows":   payloadRows,
	})
}

func (a *App) MunicipiDemografiaAdminAPI(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 6 || parts[0] != "api" || parts[1] != "admin" || parts[2] != "municipis" || parts[5] != "rebuild" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	_, _, ok := a.requirePermission(w, r, permModerate)
	if !ok {
		return
	}
	munID, err := strconv.Atoi(parts[3])
	if err != nil || munID < 0 {
		http.NotFound(w, r)
		return
	}
	switch parts[4] {
	case "demografia":
		if munID <= 0 {
			http.NotFound(w, r)
			return
		}
		if err := a.DB.RebuildMunicipiDemografia(munID); err != nil {
			http.Error(w, "failed to rebuild", http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]interface{}{"ok": true})
	case "stats":
		if munID <= 0 {
			if strings.TrimSpace(r.URL.Query().Get("all")) != "1" {
				http.NotFound(w, r)
				return
			}
			muns, err := a.DB.ListMunicipis(db.MunicipiFilter{})
			if err != nil {
				http.Error(w, "failed to list", http.StatusInternalServerError)
				return
			}
			processed := 0
			for _, mun := range muns {
				if mun.ID <= 0 {
					continue
				}
				if _, err := a.rebuildMunicipiNomCognomStats(mun.ID); err != nil {
					http.Error(w, "failed to rebuild", http.StatusInternalServerError)
					return
				}
				processed++
			}
			writeJSON(w, map[string]interface{}{
				"ok":                 true,
				"municipis_processed": processed,
			})
			return
		}
		processed, err := a.rebuildMunicipiNomCognomStats(munID)
		if err != nil {
			http.Error(w, "failed to rebuild", http.StatusInternalServerError)
			return
		}
		nomsDistinct, _ := a.DB.CountNomTotalsByMunicipi(munID)
		cognomsDistinct, _ := a.DB.CountCognomTotalsByMunicipi(munID)
		writeJSON(w, map[string]interface{}{
			"ok":              true,
			"registres":       processed,
			"nomsDistinct":    nomsDistinct,
			"cognomsDistinct": cognomsDistinct,
		})
	default:
		http.NotFound(w, r)
	}
}

func buildDemografiaSummary(meta *db.MunicipiDemografiaMeta) demografiaSummaryView {
	summary := demografiaSummaryView{}
	if meta == nil {
		return summary
	}
	if meta.AnyMin.Valid {
		summary.AnyMin = int(meta.AnyMin.Int64)
	}
	if meta.AnyMax.Valid {
		summary.AnyMax = int(meta.AnyMax.Int64)
	}
	summary.TotalNatalitat = meta.TotalNatalitat
	summary.TotalMatrimonis = meta.TotalMatrimonis
	summary.TotalDefuncions = meta.TotalDefuncions
	summary.Total = meta.TotalNatalitat + meta.TotalMatrimonis + meta.TotalDefuncions
	summary.HasData = summary.Total > 0
	if meta.UpdatedAt.Valid {
		summary.UpdatedAt = meta.UpdatedAt.Time.Format(time.RFC3339)
	}
	return summary
}

func demografiaYearFromRegistre(reg *db.TranscripcioRaw) int {
	if reg == nil {
		return 0
	}
	if reg.AnyDoc.Valid {
		year := int(reg.AnyDoc.Int64)
		if year < demografiaYearMin || year > demografiaYearMax {
			return 0
		}
		return year
	}
	if reg.DataActeISO.Valid {
		year := yearFromDate(reg.DataActeISO)
		if year < demografiaYearMin || year > demografiaYearMax {
			return 0
		}
		return year
	}
	return 0
}

func demografiaBucketFromTipus(tipus string) string {
	switch strings.ToLower(strings.TrimSpace(tipus)) {
	case "baptisme":
		return "natalitat"
	case "matrimoni":
		return "matrimonis"
	case "obit":
		return "defuncions"
	default:
		return ""
	}
}

func demografiaDeltaFromStatus(oldStatus, newStatus string) int {
	oldStatus = strings.TrimSpace(oldStatus)
	newStatus = strings.TrimSpace(newStatus)
	if newStatus == "publicat" && oldStatus != "publicat" {
		return 1
	}
	if oldStatus == "publicat" && newStatus != "publicat" {
		return -1
	}
	return 0
}

func demografiaMunicipiIDFromRegistre(reg *db.TranscripcioRaw, llibre *db.Llibre) int {
	if reg == nil {
		return 0
	}
	if llibre != nil && llibre.ID == reg.LlibreID && llibre.MunicipiID > 0 {
		return llibre.MunicipiID
	}
	return 0
}

func demografiaDeltaFromRegistre(reg *db.TranscripcioRaw, llibre *db.Llibre) (int, int, string, bool) {
	if reg == nil {
		return 0, 0, "", false
	}
	munID := demografiaMunicipiIDFromRegistre(reg, llibre)
	if munID <= 0 {
		return 0, 0, "", false
	}
	year := demografiaYearFromRegistre(reg)
	if year <= 0 {
		return 0, 0, "", false
	}
	tipus := demografiaBucketFromTipus(reg.TipusActe)
	if tipus == "" {
		return 0, 0, "", false
	}
	return munID, year, tipus, true
}

func (a *App) loadLlibreForRegistre(reg *db.TranscripcioRaw) (*db.Llibre, error) {
	if reg == nil || reg.LlibreID <= 0 {
		return nil, sql.ErrNoRows
	}
	return a.DB.GetLlibre(reg.LlibreID)
}

func (a *App) applyDemografiaDeltaForRegistre(reg *db.TranscripcioRaw, delta int) {
	if reg == nil || delta == 0 {
		return
	}
	llibre, err := a.loadLlibreForRegistre(reg)
	if err != nil || llibre == nil {
		return
	}
	munID, year, tipus, ok := demografiaDeltaFromRegistre(reg, llibre)
	if !ok {
		return
	}
	if err := a.DB.ApplyMunicipiDemografiaDelta(munID, year, tipus, delta); err != nil {
		Errorf("Error actualitzant demografia municipi %d: %v", munID, err)
		return
	}
	a.applyNivellDemografiaDeltaForMunicipi(munID, year, tipus, delta)
}
