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

type llibresExportPayload struct {
	Version    int                  `json:"version"`
	ExportedAt string               `json:"exported_at"`
	Llibres    []llibreExportRecord `json:"llibres"`
}

type llibreExportRecord struct {
	ID                int                 `json:"id"`
	Titol             string              `json:"titol,omitempty"`
	NomEsglesia       string              `json:"nom_esglesia,omitempty"`
	TipusLlibre       string              `json:"tipus_llibre,omitempty"`
	Cronologia        string              `json:"cronologia,omitempty"`
	Volum             string              `json:"volum,omitempty"`
	Abat              string              `json:"abat,omitempty"`
	Contingut         string              `json:"contingut,omitempty"`
	Llengua           string              `json:"llengua,omitempty"`
	Requeriments      string              `json:"requeriments_tecnics,omitempty"`
	UnitatCatalogacio string              `json:"unitat_catalogacio,omitempty"`
	UnitatInstalacio  string              `json:"unitat_instalacio,omitempty"`
	Pagines           *int                `json:"pagines,omitempty"`
	URLBase           string              `json:"url_base,omitempty"`
	URLImatgePrefix   string              `json:"url_imatge_prefix,omitempty"`
	Pagina            string              `json:"pagina,omitempty"`
	IndexacioCompleta bool                `json:"indexacio_completa,omitempty"`
	CodiDigital       string              `json:"codi_digital,omitempty"`
	CodiFisic         string              `json:"codi_fisic,omitempty"`
	MunicipiNom       string              `json:"municipi_nom"`
	MunicipiPaisISO2  string              `json:"municipi_pais_iso2,omitempty"`
	ArquebisbatNom    string              `json:"arquebisbat_nom,omitempty"`
	ArquebisbatTipus  string              `json:"arquebisbat_tipus,omitempty"`
	Arxius            []llibreExportArxiu `json:"arxius,omitempty"`
	URLs              []llibreExportURL   `json:"urls,omitempty"`
}

type llibreExportArxiu struct {
	Nom         string `json:"nom"`
	Signatura   string `json:"signatura,omitempty"`
	URLOverride string `json:"url_override,omitempty"`
}

type llibreExportURL struct {
	URL        string `json:"url"`
	Tipus      string `json:"tipus,omitempty"`
	Descripcio string `json:"descripcio,omitempty"`
	ArxiuNom   string `json:"arxiu_nom,omitempty"`
}

func (a *App) AdminLlibresImport(w http.ResponseWriter, r *http.Request) {
	http.NotFound(w, r)
}

func (a *App) AdminLlibresExport(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresExport, PermissionTarget{}); !ok {
		return
	}
	llibres, err := a.DB.ListLlibres(db.LlibreFilter{})
	if err != nil {
		http.NotFound(w, r)
		return
	}
	entitats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	entMap := map[int]db.Arquebisbat{}
	for _, ent := range entitats {
		entMap[ent.ID] = db.Arquebisbat{Nom: ent.Nom, TipusEntitat: ent.TipusEntitat}
	}
	municipiISO := a.municipiISOMapByID()
	payload := llibresExportPayload{
		Version:    1,
		ExportedAt: time.Now().Format(time.RFC3339),
	}
	for _, row := range llibres {
		rec := llibreExportRecord{
			ID:                row.ID,
			Titol:             strings.TrimSpace(row.Titol),
			NomEsglesia:       strings.TrimSpace(row.NomEsglesia),
			TipusLlibre:       strings.TrimSpace(row.TipusLlibre),
			Cronologia:        strings.TrimSpace(row.Cronologia),
			Volum:             strings.TrimSpace(row.Volum),
			Abat:              strings.TrimSpace(row.Abat),
			Contingut:         strings.TrimSpace(row.Contingut),
			Llengua:           strings.TrimSpace(row.Llengua),
			Requeriments:      strings.TrimSpace(row.Requeriments),
			UnitatCatalogacio: strings.TrimSpace(row.UnitatCatalogacio),
			UnitatInstalacio:  strings.TrimSpace(row.UnitatInstalacio),
			URLBase:           strings.TrimSpace(row.URLBase),
			URLImatgePrefix:   strings.TrimSpace(row.URLImatgePrefix),
			Pagina:            strings.TrimSpace(row.Pagina),
			IndexacioCompleta: row.IndexacioCompleta,
			CodiDigital:       strings.TrimSpace(row.CodiDigital),
			CodiFisic:         strings.TrimSpace(row.CodiFisic),
		}
		if row.Pagines.Valid {
			v := int(row.Pagines.Int64)
			rec.Pagines = &v
		}
		if row.MunicipiNom.Valid {
			rec.MunicipiNom = strings.TrimSpace(row.MunicipiNom.String)
		}
		if iso2, ok := municipiISO[row.MunicipiID]; ok {
			rec.MunicipiPaisISO2 = iso2
		}
		if ent, ok := entMap[row.ArquebisbatID]; ok {
			rec.ArquebisbatNom = strings.TrimSpace(ent.Nom)
			rec.ArquebisbatTipus = strings.TrimSpace(ent.TipusEntitat)
		}
		arxius, _ := a.DB.ListLlibreArxius(row.ID)
		for _, link := range arxius {
			rec.Arxius = append(rec.Arxius, llibreExportArxiu{
				Nom:         strings.TrimSpace(link.ArxiuNom.String),
				Signatura:   strings.TrimSpace(link.Signatura.String),
				URLOverride: strings.TrimSpace(link.URLOverride.String),
			})
		}
		urls, _ := a.DB.ListLlibreURLs(row.ID)
		for _, link := range urls {
			typ := ""
			if link.Tipus.Valid {
				typ = strings.TrimSpace(link.Tipus.String)
			}
			desc := ""
			if link.Descripcio.Valid {
				desc = strings.TrimSpace(link.Descripcio.String)
			}
			arxiuNom := ""
			if link.ArxiuNom.Valid {
				arxiuNom = strings.TrimSpace(link.ArxiuNom.String)
			}
			rec.URLs = append(rec.URLs, llibreExportURL{
				URL:        strings.TrimSpace(link.URL),
				Tipus:      typ,
				Descripcio: desc,
				ArxiuNom:   arxiuNom,
			})
		}
		payload.Llibres = append(payload.Llibres, rec)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=llibres-export.json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func (a *App) AdminLlibresImportRun(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresImport, PermissionTarget{})
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Redirect(w, r, withQueryParams("/admin/llibres/import", map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	returnTo := safeReturnTo(r.FormValue("return_to"), "/admin/llibres/import")
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	file, _, err := r.FormFile("import_file")
	if err != nil {
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	defer file.Close()

	var payload llibresExportPayload
	if err := json.NewDecoder(file).Decode(&payload); err != nil {
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}

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
	arxius, _ := a.DB.ListArxius(db.ArxiuFilter{})
	arxiuMap := map[string]int{}
	for _, arxiu := range arxius {
		key := normalizeKey(arxiu.Nom)
		if key != "" {
			arxiuMap[key] = arxiu.ID
		}
	}

	total := len(payload.Llibres)
	created, skipped, errors := 0, 0, 0
	for _, row := range payload.Llibres {
		munID := 0
		if strings.TrimSpace(row.MunicipiNom) != "" {
			munID = munMap[normalizeKey(row.MunicipiNom, strings.ToUpper(row.MunicipiPaisISO2))]
			if munID == 0 {
				munID = munNameMap[normalizeKey(row.MunicipiNom)]
			}
		}
		if munID == 0 {
			errors++
			continue
		}
		entID := 0
		if strings.TrimSpace(row.ArquebisbatNom) != "" {
			entID = entMap[normalizeKey(row.ArquebisbatNom, row.ArquebisbatTipus)]
			if entID == 0 {
				entID = entNameMap[normalizeKey(row.ArquebisbatNom)]
			}
		}
		if entID == 0 {
			errors++
			continue
		}
		dup, err := a.DB.HasLlibreDuplicate(munID, row.TipusLlibre, row.Cronologia, row.CodiDigital, row.CodiFisic, 0)
		if err != nil {
			errors++
			continue
		}
		if dup {
			skipped++
			continue
		}
		var pagines sql.NullInt64
		if row.Pagines != nil && *row.Pagines > 0 {
			pagines = sql.NullInt64{Int64: int64(*row.Pagines), Valid: true}
		}
		llibre := &db.Llibre{
			ArquebisbatID:     entID,
			MunicipiID:        munID,
			NomEsglesia:       strings.TrimSpace(row.NomEsglesia),
			CodiDigital:       strings.TrimSpace(row.CodiDigital),
			CodiFisic:         strings.TrimSpace(row.CodiFisic),
			Titol:             strings.TrimSpace(row.Titol),
			TipusLlibre:       strings.TrimSpace(row.TipusLlibre),
			Cronologia:        strings.TrimSpace(row.Cronologia),
			Volum:             strings.TrimSpace(row.Volum),
			Abat:              strings.TrimSpace(row.Abat),
			Contingut:         strings.TrimSpace(row.Contingut),
			Llengua:           strings.TrimSpace(row.Llengua),
			Requeriments:      strings.TrimSpace(row.Requeriments),
			UnitatCatalogacio: strings.TrimSpace(row.UnitatCatalogacio),
			UnitatInstalacio:  strings.TrimSpace(row.UnitatInstalacio),
			Pagines:           pagines,
			URLBase:           strings.TrimSpace(row.URLBase),
			URLImatgePrefix:   strings.TrimSpace(row.URLImatgePrefix),
			Pagina:            strings.TrimSpace(row.Pagina),
			IndexacioCompleta: row.IndexacioCompleta,
			CreatedBy:         sqlNullIntFromInt(user.ID),
			ModeracioEstat:    "pendent",
			ModeratedBy:       sql.NullInt64{},
			ModeratedAt:       sql.NullTime{},
			ModeracioMotiu:    "",
		}
		newID, err := a.DB.CreateLlibre(llibre)
		if err != nil {
			errors++
			continue
		}
		created++
		if user != nil {
			_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleLlibreCreate, "crear", "llibre", &newID, "pendent", nil, "import")
		}
		for _, link := range row.Arxius {
			if strings.TrimSpace(link.Nom) == "" {
				continue
			}
			arxiuID := arxiuMap[normalizeKey(link.Nom)]
			if arxiuID <= 0 {
				continue
			}
			_ = a.DB.AddArxiuLlibre(arxiuID, newID, strings.TrimSpace(link.Signatura), strings.TrimSpace(link.URLOverride))
		}
		for _, link := range row.URLs {
			if strings.TrimSpace(link.URL) == "" {
				continue
			}
			arxiuID := sql.NullInt64{}
			if strings.TrimSpace(link.ArxiuNom) != "" {
				if id := arxiuMap[normalizeKey(link.ArxiuNom)]; id > 0 {
					arxiuID = sql.NullInt64{Int64: int64(id), Valid: true}
				}
			}
			url := &db.LlibreURL{
				LlibreID:   newID,
				ArxiuID:    arxiuID,
				URL:        strings.TrimSpace(link.URL),
				Tipus:      sqlNullString(link.Tipus),
				Descripcio: sqlNullString(link.Descripcio),
				CreatedBy:  sqlNullIntFromInt(user.ID),
			}
			_ = a.DB.AddLlibreURL(url)
		}
	}
	redirect := withQueryParams(returnTo, map[string]string{
		"import":          "1",
		"llibres_total":   strconv.Itoa(total),
		"llibres_created": strconv.Itoa(created),
		"llibres_skipped": strconv.Itoa(skipped),
		"llibres_errors":  strconv.Itoa(errors),
	})
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}

func (a *App) municipiISOMapByID() map[int]string {
	levelISO := a.levelISOMap()
	rows, _ := a.DB.ListMunicipis(db.MunicipiFilter{})
	res := map[int]string{}
	for _, row := range rows {
		m, err := a.DB.GetMunicipi(row.ID)
		if err != nil || m == nil {
			continue
		}
		iso2 := municipiISO2(m, levelISO)
		if iso2 != "" {
			res[m.ID] = iso2
		}
	}
	return res
}

func sqlNullString(val string) sql.NullString {
	if strings.TrimSpace(val) == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: strings.TrimSpace(val), Valid: true}
}
