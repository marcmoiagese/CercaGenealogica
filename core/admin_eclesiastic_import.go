package core

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type eclesiasticExportPayload struct {
	Version    int                        `json:"version"`
	ExportedAt string                     `json:"exported_at"`
	Entitats   []eclesiasticExportEntitat `json:"entitats"`
	Municipis  []eclesiasticExportRel     `json:"municipis"`
}

type eclesiasticExportEntitat struct {
	ID           int    `json:"id"`
	Nom          string `json:"nom"`
	TipusEntitat string `json:"tipus_entitat"`
	PaisISO2     string `json:"pais_iso2,omitempty"`
	Nivell       *int   `json:"nivell,omitempty"`
	ParentID     *int   `json:"parent_id,omitempty"`
	AnyInici     *int   `json:"any_inici,omitempty"`
	AnyFi        *int   `json:"any_fi,omitempty"`
	Web          string `json:"web"`
	WebArxiu     string `json:"web_arxiu"`
	WebWikipedia string `json:"web_wikipedia"`
	Territori    string `json:"territori"`
	Observacions string `json:"observacions"`
}

type eclesiasticExportRel struct {
	EntitatID         int    `json:"entitat_id"`
	EntitatNom        string `json:"entitat_nom"`
	MunicipiNom       string `json:"municipi_nom"`
	MunicipiPaisISO2  string `json:"municipi_pais_iso2,omitempty"`
	AnyInici          *int   `json:"any_inici,omitempty"`
	AnyFi             *int   `json:"any_fi,omitempty"`
	Motiu             string `json:"motiu"`
	Font              string `json:"font"`
}

func (a *App) AdminEclesiasticImport(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminEclesImport, PermissionTarget{}); !ok {
		return
	}
	q := r.URL.Query()
	importRun := q.Get("import") == "1"
	msg := ""
	if q.Get("err") != "" {
		msg = T(ResolveLang(r), "common.error")
	}
	RenderPrivateTemplate(w, r, "admin-eclesiastic-import.html", map[string]interface{}{
		"ImportRun":          importRun,
		"EntitatsTotal":      parseIntQuery(q.Get("entitats_total")),
		"EntitatsCreated":    parseIntQuery(q.Get("entitats_created")),
		"EntitatsSkipped":    parseIntQuery(q.Get("entitats_skipped")),
		"EntitatsErrors":     parseIntQuery(q.Get("entitats_errors")),
		"RelacionsTotal":     parseIntQuery(q.Get("relacions_total")),
		"RelacionsCreated":   parseIntQuery(q.Get("relacions_created")),
		"RelacionsSkipped":   parseIntQuery(q.Get("relacions_skipped")),
		"RelacionsErrors":    parseIntQuery(q.Get("relacions_errors")),
		"Msg":                msg,
	})
}

func (a *App) AdminEclesiasticExport(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminEclesExport, PermissionTarget{}); !ok {
		return
	}
	entRows, err := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	if err != nil {
		http.NotFound(w, r)
		return
	}
	paisByID := map[int]string{}
	paisos, _ := a.DB.ListPaisos()
	for _, p := range paisos {
		if p.ID > 0 {
			paisByID[p.ID] = strings.ToUpper(strings.TrimSpace(p.CodiISO2))
		}
	}
	levelISO := a.levelISOMap()
	munRows, _ := a.DB.ListMunicipis(db.MunicipiFilter{})

	payload := eclesiasticExportPayload{
		Version:    1,
		ExportedAt: time.Now().Format(time.RFC3339),
	}
	for _, row := range entRows {
		ent, err := a.DB.GetArquebisbat(row.ID)
		if err != nil || ent == nil {
			continue
		}
		var parent *int
		if ent.ParentID.Valid {
			v := int(ent.ParentID.Int64)
			parent = &v
		}
		var nivell *int
		if ent.Nivell.Valid {
			v := int(ent.Nivell.Int64)
			nivell = &v
		}
		var anyInici *int
		if ent.AnyInici.Valid {
			v := int(ent.AnyInici.Int64)
			anyInici = &v
		}
		var anyFi *int
		if ent.AnyFi.Valid {
			v := int(ent.AnyFi.Int64)
			anyFi = &v
		}
		iso2 := ""
		if ent.PaisID.Valid {
			iso2 = paisByID[int(ent.PaisID.Int64)]
		}
		payload.Entitats = append(payload.Entitats, eclesiasticExportEntitat{
			ID:           ent.ID,
			Nom:          ent.Nom,
			TipusEntitat: ent.TipusEntitat,
			PaisISO2:     iso2,
			Nivell:       nivell,
			ParentID:     parent,
			AnyInici:     anyInici,
			AnyFi:        anyFi,
			Web:          ent.Web,
			WebArxiu:     ent.WebArxiu,
			WebWikipedia: ent.WebWikipedia,
			Territori:    ent.Territori,
			Observacions: ent.Observacions,
		})
	}
	for _, row := range munRows {
		mun, err := a.DB.GetMunicipi(row.ID)
		if err != nil || mun == nil {
			continue
		}
		iso2 := municipiISO2(mun, levelISO)
		rels, err := a.DB.ListArquebisbatMunicipis(mun.ID)
		if err != nil {
			continue
		}
		for _, rel := range rels {
			var anyInici *int
			if rel.AnyInici.Valid {
				v := int(rel.AnyInici.Int64)
				anyInici = &v
			}
			var anyFi *int
			if rel.AnyFi.Valid {
				v := int(rel.AnyFi.Int64)
				anyFi = &v
			}
			payload.Municipis = append(payload.Municipis, eclesiasticExportRel{
				EntitatID:        rel.ArquebisbatID,
				EntitatNom:       rel.NomEntitat,
				MunicipiNom:      mun.Nom,
				MunicipiPaisISO2: iso2,
				AnyInici:         anyInici,
				AnyFi:            anyFi,
				Motiu:            rel.Motiu,
				Font:             rel.Font,
			})
		}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=eclesiastic-export.json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func (a *App) AdminEclesiasticImportRun(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyAdminEclesImport, PermissionTarget{})
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Redirect(w, r, "/admin/eclesiastic/import?err=1", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Redirect(w, r, "/admin/eclesiastic/import?err=1", http.StatusSeeOther)
		return
	}
	file, _, err := r.FormFile("import_file")
	if err != nil {
		http.Redirect(w, r, "/admin/eclesiastic/import?err=1", http.StatusSeeOther)
		return
	}
	defer file.Close()

	var payload eclesiasticExportPayload
	if err := json.NewDecoder(file).Decode(&payload); err != nil {
		http.Redirect(w, r, "/admin/eclesiastic/import?err=1", http.StatusSeeOther)
		return
	}

	entTotal := len(payload.Entitats)
	entCreated, entSkipped, entErrors := 0, 0, 0
	relTotal := len(payload.Municipis)
	relCreated, relSkipped, relErrors := 0, 0, 0

	existingRows, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	existingMap := map[string]int{}
	existingNameMap := map[string]int{}
	for _, row := range existingRows {
		key := normalizeKey(row.Nom, row.TipusEntitat)
		if key != "" {
			existingMap[key] = row.ID
		}
		nameKey := normalizeKey(row.Nom)
		if nameKey != "" {
			existingNameMap[nameKey] = row.ID
		}
	}
	paisByISO := a.paisIDByISO()
	idMap := map[int]int{}
	pending := make([]eclesiasticExportEntitat, 0, len(payload.Entitats))
	pending = append(pending, payload.Entitats...)

	for len(pending) > 0 {
		progress := false
		next := []eclesiasticExportEntitat{}
		for _, ent := range pending {
			if ent.ParentID != nil {
				if _, ok := idMap[*ent.ParentID]; !ok {
					next = append(next, ent)
					continue
				}
			}
			key := normalizeKey(ent.Nom, ent.TipusEntitat)
			if existingID, ok := existingMap[key]; ok {
				idMap[ent.ID] = existingID
				entSkipped++
				progress = true
				continue
			}
			var parent sql.NullInt64
			if ent.ParentID != nil {
				if pid, ok := idMap[*ent.ParentID]; ok {
					parent = sql.NullInt64{Int64: int64(pid), Valid: true}
				}
			}
			var nivell sql.NullInt64
			if ent.Nivell != nil {
				nivell = sql.NullInt64{Int64: int64(*ent.Nivell), Valid: true}
			}
			var anyInici sql.NullInt64
			if ent.AnyInici != nil {
				anyInici = sql.NullInt64{Int64: int64(*ent.AnyInici), Valid: true}
			}
			var anyFi sql.NullInt64
			if ent.AnyFi != nil {
				anyFi = sql.NullInt64{Int64: int64(*ent.AnyFi), Valid: true}
			}
			var paisID sql.NullInt64
			if ent.PaisISO2 != "" {
				if pid, ok := paisByISO[strings.ToUpper(ent.PaisISO2)]; ok {
					paisID = sql.NullInt64{Int64: int64(pid), Valid: true}
				}
			}
			entitat := &db.Arquebisbat{
				Nom:            ent.Nom,
				TipusEntitat:   ent.TipusEntitat,
				PaisID:         paisID,
				Nivell:         nivell,
				ParentID:       parent,
				AnyInici:       anyInici,
				AnyFi:          anyFi,
				Web:            ent.Web,
				WebArxiu:       ent.WebArxiu,
				WebWikipedia:   ent.WebWikipedia,
				Territori:      ent.Territori,
				Observacions:   ent.Observacions,
				CreatedBy:      sqlNullIntFromInt(user.ID),
				ModeracioEstat: "pendent",
				ModeratedBy:    sql.NullInt64{},
				ModeratedAt:    sql.NullTime{},
				ModeracioMotiu: "",
			}
			newID, err := a.DB.CreateArquebisbat(entitat)
			if err != nil {
				entErrors++
				continue
			}
			idMap[ent.ID] = newID
			existingMap[key] = newID
			entCreated++
			progress = true
			if user != nil {
				_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleEclesiasticCreate, "crear", "eclesiastic", &newID, "pendent", nil, "")
			}
		}
		if !progress {
			break
		}
		pending = next
	}

	munMap := a.municipiNameMap()
	for _, rel := range payload.Municipis {
		entID := 0
		if rel.EntitatID != 0 {
			entID = idMap[rel.EntitatID]
		}
		if entID == 0 && rel.EntitatNom != "" {
			if id, ok := existingNameMap[normalizeKey(rel.EntitatNom)]; ok {
				entID = id
			}
		}
		munID := 0
		if rel.MunicipiNom != "" {
			key := normalizeKey(rel.MunicipiNom, strings.ToUpper(rel.MunicipiPaisISO2))
			if id, ok := munMap[key]; ok {
				munID = id
			}
		}
		if entID == 0 || munID == 0 {
			relErrors++
			continue
		}
		var anyInici sql.NullInt64
		if rel.AnyInici != nil {
			anyInici = sql.NullInt64{Int64: int64(*rel.AnyInici), Valid: true}
		}
		var anyFi sql.NullInt64
		if rel.AnyFi != nil {
			anyFi = sql.NullInt64{Int64: int64(*rel.AnyFi), Valid: true}
		}
		_, err := a.DB.SaveArquebisbatMunicipi(&db.ArquebisbatMunicipi{
			MunicipiID:    munID,
			ArquebisbatID: entID,
			AnyInici:      anyInici,
			AnyFi:         anyFi,
			Motiu:         rel.Motiu,
			Font:          rel.Font,
		})
		if err != nil {
			relErrors++
			continue
		}
		relCreated++
	}

	redirect := "/admin/eclesiastic/import?import=1" +
		"&entitats_total=" + strconv.Itoa(entTotal) +
		"&entitats_created=" + strconv.Itoa(entCreated) +
		"&entitats_skipped=" + strconv.Itoa(entSkipped) +
		"&entitats_errors=" + strconv.Itoa(entErrors) +
		"&relacions_total=" + strconv.Itoa(relTotal) +
		"&relacions_created=" + strconv.Itoa(relCreated) +
		"&relacions_skipped=" + strconv.Itoa(relSkipped) +
		"&relacions_errors=" + strconv.Itoa(relErrors)
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}

func normalizeKey(parts ...string) string {
	trimmed := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		trimmed = append(trimmed, strings.ToLower(part))
	}
	return strings.Join(trimmed, "|")
}

func (a *App) paisIDByISO() map[string]int {
	paisos, _ := a.DB.ListPaisos()
	res := map[string]int{}
	for _, p := range paisos {
		key := strings.ToUpper(strings.TrimSpace(p.CodiISO2))
		if key != "" {
			res[key] = p.ID
		}
	}
	return res
}

func (a *App) levelISOMap() map[int]string {
	nivells, _ := a.DB.ListNivells(db.NivellAdminFilter{})
	res := map[int]string{}
	for _, n := range nivells {
		if n.PaisISO2.Valid {
			res[n.ID] = strings.ToUpper(strings.TrimSpace(n.PaisISO2.String))
		}
	}
	return res
}

func municipiISO2(m *db.Municipi, levelISO map[int]string) string {
	for i := 0; i < len(m.NivellAdministratiuID); i++ {
		if m.NivellAdministratiuID[i].Valid {
			if iso, ok := levelISO[int(m.NivellAdministratiuID[i].Int64)]; ok {
				return iso
			}
		}
	}
	return ""
}

func (a *App) municipiNameMap() map[string]int {
	levelISO := a.levelISOMap()
	rows, _ := a.DB.ListMunicipis(db.MunicipiFilter{})
	res := map[string]int{}
	for _, row := range rows {
		m, err := a.DB.GetMunicipi(row.ID)
		if err != nil || m == nil {
			continue
		}
		iso2 := municipiISO2(m, levelISO)
		key := normalizeKey(m.Nom, iso2)
		if key != "" {
			res[key] = m.ID
		}
	}
	return res
}
