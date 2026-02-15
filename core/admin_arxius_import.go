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

type arxiusExportPayload struct {
	Version    int                 `json:"version"`
	ExportedAt string              `json:"exported_at"`
	Arxius     []arxiuExportRecord `json:"arxius"`
}

type arxiuExportRecord struct {
	ID               int    `json:"id"`
	Nom              string `json:"nom"`
	Tipus            string `json:"tipus"`
	Acces            string `json:"acces"`
	Adreca           string `json:"adreca"`
	Ubicacio         string `json:"ubicacio"`
	What3Words       string `json:"what3words,omitempty"`
	Web              string `json:"web"`
	Notes            string `json:"notes"`
	AcceptaDonacions bool   `json:"accepta_donacions,omitempty"`
	DonacionsURL     string `json:"donacions_url,omitempty"`
	MunicipiNom      string `json:"municipi_nom,omitempty"`
	MunicipiPaisISO2 string `json:"municipi_pais_iso2,omitempty"`
	EntitatNom       string `json:"entitat_nom,omitempty"`
}

func (a *App) AdminArxiusImport(w http.ResponseWriter, r *http.Request) {
	http.NotFound(w, r)
}

func (a *App) AdminArxiusExport(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminArxiusExport, PermissionTarget{}); !ok {
		return
	}
	levelISO := a.levelISOMap()
	arxius, err := a.DB.ListArxius(db.ArxiuFilter{})
	if err != nil {
		http.NotFound(w, r)
		return
	}
	payload := arxiusExportPayload{
		Version:    3,
		ExportedAt: time.Now().Format(time.RFC3339),
	}
	for _, row := range arxius {
		var iso2 string
		if row.MunicipiNom.Valid && row.MunicipiID.Valid {
			if mun, err := a.DB.GetMunicipi(int(row.MunicipiID.Int64)); err == nil && mun != nil {
				iso2 = municipiISO2(mun, levelISO)
			}
		}
		payload.Arxius = append(payload.Arxius, arxiuExportRecord{
			ID:               row.ID,
			Nom:              row.Nom,
			Tipus:            row.Tipus,
			Acces:            row.Acces,
			Adreca:           row.Adreca,
			Ubicacio:         row.Ubicacio,
			What3Words:       row.What3Words,
			Web:              row.Web,
			Notes:            row.Notes,
			AcceptaDonacions: row.AcceptaDonacions,
			DonacionsURL:     row.DonacionsURL,
			MunicipiNom:      row.MunicipiNom.String,
			MunicipiPaisISO2: iso2,
			EntitatNom:       row.EntitatNom.String,
		})
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=arxius-export.json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func (a *App) AdminArxiusImportRun(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyAdminArxiusImport, PermissionTarget{})
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		a.logAdminImportRun(r, "arxius", adminImportStatusError, user.ID)
		http.Redirect(w, r, withQueryParams("/admin/arxius/import", map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	returnTo := safeReturnTo(r.FormValue("return_to"), "/admin/arxius/import")
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		a.logAdminImportRun(r, "arxius", adminImportStatusError, user.ID)
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	file, _, err := r.FormFile("import_file")
	if err != nil {
		a.logAdminImportRun(r, "arxius", adminImportStatusError, user.ID)
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	defer file.Close()

	var payload arxiusExportPayload
	if err := json.NewDecoder(file).Decode(&payload); err != nil {
		a.logAdminImportRun(r, "arxius", adminImportStatusError, user.ID)
		http.Redirect(w, r, "/admin/arxius/import?err=1", http.StatusSeeOther)
		return
	}
	total := len(payload.Arxius)
	created, skipped, errors := 0, 0, 0

	entitats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	entMap := map[string]int{}
	entNameMap := map[string]int{}
	for _, ent := range entitats {
		key := normalizeKey(ent.Nom, ent.TipusEntitat)
		if key != "" {
			entMap[key] = ent.ID
		}
		nameKey := normalizeKey(ent.Nom)
		if nameKey != "" {
			entNameMap[nameKey] = ent.ID
		}
	}
	munMap := a.municipiNameMap()
	munNameMap := map[string]int{}
	if rows, err := a.DB.ListMunicipis(db.MunicipiFilter{}); err == nil {
		for _, row := range rows {
			nameKey := normalizeKey(row.Nom)
			if nameKey != "" {
				munNameMap[nameKey] = row.ID
			}
		}
	}
	for _, row := range payload.Arxius {
		hasMunicipi := strings.TrimSpace(row.MunicipiNom) != ""
		munID := 0
		if hasMunicipi {
			key := normalizeKey(row.MunicipiNom, strings.ToUpper(row.MunicipiPaisISO2))
			munID = munMap[key]
			if munID == 0 {
				munID = munNameMap[normalizeKey(row.MunicipiNom)]
			}
			if munID == 0 {
				errors++
				Errorf("Arxius import: municipi no trobat (%s, %s) per arxiu %s", row.MunicipiNom, row.MunicipiPaisISO2, row.Nom)
				continue
			}
		}
		entID := 0
		if row.EntitatNom != "" {
			entID = entNameMap[normalizeKey(row.EntitatNom)]
			if entID == 0 {
				entID = entMap[normalizeKey(row.EntitatNom, row.Tipus)]
			}
		}
		filter := db.ArxiuFilter{Text: row.Nom, MunicipiID: munID}
		exists, _ := a.DB.ListArxius(filter)
		if len(exists) > 0 {
			skipped++
			continue
		}
		arxiu := &db.Arxiu{
			Nom:              row.Nom,
			Tipus:            row.Tipus,
			Acces:            row.Acces,
			Adreca:           row.Adreca,
			Ubicacio:         row.Ubicacio,
			What3Words:       row.What3Words,
			Web:              row.Web,
			Notes:            row.Notes,
			AcceptaDonacions: row.AcceptaDonacions && strings.TrimSpace(row.DonacionsURL) != "",
			DonacionsURL:     strings.TrimSpace(row.DonacionsURL),
			CreatedBy:        sqlNullIntFromInt(user.ID),
			ModeracioEstat:   "pendent",
			ModeratedBy:      sql.NullInt64{},
			ModeratedAt:      sql.NullTime{},
			ModeracioMotiu:   "",
		}
		if munID > 0 {
			arxiu.MunicipiID = sql.NullInt64{Int64: int64(munID), Valid: true}
		}
		if entID > 0 {
			arxiu.EntitatEclesiasticaID = sql.NullInt64{Int64: int64(entID), Valid: true}
		}
		newID, err := a.DB.CreateArxiu(arxiu)
		if err != nil {
			errors++
			Errorf("Arxius import: error creant arxiu %s: %v", row.Nom, err)
			continue
		}
		created++
		if user != nil {
			_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleArxiuCreate, "crear", "arxiu", &newID, "pendent", nil, "")
		}
	}
	redirect := withQueryParams(returnTo, map[string]string{
		"import":         "1",
		"arxius_total":   strconv.Itoa(total),
		"arxius_created": strconv.Itoa(created),
		"arxius_skipped": strconv.Itoa(skipped),
		"arxius_errors":  strconv.Itoa(errors),
	})
	status := adminImportStatusOK
	if errors > 0 {
		status = adminImportStatusError
	}
	a.logAdminImportRun(r, "arxius", status, user.ID)
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}
