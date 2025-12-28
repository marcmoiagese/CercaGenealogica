package core

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type territoriExportPayload struct {
	Version    int                       `json:"version"`
	ExportedAt string                    `json:"exported_at"`
	Countries  []territoriExportCountry  `json:"countries"`
	Levels     []territoriExportLevel    `json:"levels"`
	Municipis  []territoriExportMunicipi `json:"municipis"`
}

type territoriExportCountry struct {
	ISO2 string `json:"iso2"`
	ISO3 string `json:"iso3"`
	Num  string `json:"num"`
}

type territoriExportLevel struct {
	ID       int    `json:"id"`
	PaisISO2 string `json:"pais_iso2"`
	Nivel    int    `json:"nivel"`
	Nom      string `json:"nom"`
	Tipus    string `json:"tipus"`
	Codi     string `json:"codi"`
	Altres   string `json:"altres"`
	ParentID *int   `json:"parent_id,omitempty"`
	AnyInici *int   `json:"any_inici,omitempty"`
	AnyFi    *int   `json:"any_fi,omitempty"`
	Estat    string `json:"estat"`
}

type territoriExportMunicipi struct {
	ID         int     `json:"id"`
	PaisISO2   string  `json:"pais_iso2,omitempty"`
	Nom        string  `json:"nom"`
	Tipus      string  `json:"tipus"`
	ParentID   *int    `json:"parent_id,omitempty"`
	Nivells    []int   `json:"nivells"`
	CodiPostal string  `json:"codi_postal"`
	Latitud    *float64 `json:"latitud,omitempty"`
	Longitud   *float64 `json:"longitud,omitempty"`
	What3Words string  `json:"what3words"`
	Web        string  `json:"web"`
	Wikipedia  string  `json:"wikipedia"`
	Altres     string  `json:"altres"`
	Estat      string  `json:"estat"`
}

type territoriImportMunicipi struct {
	row       territoriexportMunicipiRow
	municipi  db.Municipi
	oldParent int
}

type territoriexportMunicipiRow struct {
	ID         int
	ParentID   int
	PaisISO2   string
	Nom        string
	Tipus      string
	Nivells    []int
	CodiPostal string
	Latitud    *float64
	Longitud   *float64
	What3Words string
	Web        string
	Wikipedia  string
	Altres     string
	Estat      string
}

func (a *App) AdminTerritoriImport(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
		return
	}
	q := r.URL.Query()
	importRun := q.Get("import") == "1"
	msg := ""
	if q.Get("err") != "" {
		msg = T(ResolveLang(r), "common.error")
	}
	RenderPrivateTemplate(w, r, "admin-territori-import.html", map[string]interface{}{
		"ImportRun":        importRun,
		"CountriesCreated": parseIntQuery(q.Get("countries_created")),
		"LevelsTotal":      parseIntQuery(q.Get("levels_total")),
		"LevelsCreated":    parseIntQuery(q.Get("levels_created")),
		"LevelsSkipped":    parseIntQuery(q.Get("levels_skipped")),
		"LevelsErrors":     parseIntQuery(q.Get("levels_errors")),
		"MunicipisTotal":   parseIntQuery(q.Get("municipis_total")),
		"MunicipisCreated": parseIntQuery(q.Get("municipis_created")),
		"MunicipisSkipped": parseIntQuery(q.Get("municipis_skipped")),
		"MunicipisErrors":  parseIntQuery(q.Get("municipis_errors")),
		"Msg":              msg,
	})
}

func (a *App) AdminTerritoriExport(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
		return
	}
	paisos, err := a.DB.ListPaisos()
	if err != nil {
		http.NotFound(w, r)
		return
	}
	nivells, err := a.DB.ListNivells(db.NivellAdminFilter{})
	if err != nil {
		http.NotFound(w, r)
		return
	}
	levelISO := map[int]string{}
	for _, n := range nivells {
		if n.PaisISO2.Valid {
			levelISO[n.ID] = strings.ToUpper(n.PaisISO2.String)
		}
	}
	municipiRows, err := a.DB.ListMunicipis(db.MunicipiFilter{})
	if err != nil {
		http.NotFound(w, r)
		return
	}
	payload := territoriExportPayload{
		Version:    1,
		ExportedAt: time.Now().Format(time.RFC3339),
	}
	for _, p := range paisos {
		payload.Countries = append(payload.Countries, territoriExportCountry{
			ISO2: strings.ToUpper(strings.TrimSpace(p.CodiISO2)),
			ISO3: strings.ToUpper(strings.TrimSpace(p.CodiISO3)),
			Num:  strings.TrimSpace(p.CodiPaisNum),
		})
	}
	for _, n := range nivells {
		var parent *int
		if n.ParentID.Valid {
			v := int(n.ParentID.Int64)
			parent = &v
		}
		var anyInici *int
		if n.AnyInici.Valid {
			v := int(n.AnyInici.Int64)
			anyInici = &v
		}
		var anyFi *int
		if n.AnyFi.Valid {
			v := int(n.AnyFi.Int64)
			anyFi = &v
		}
		iso2 := ""
		if n.PaisISO2.Valid {
			iso2 = strings.ToUpper(strings.TrimSpace(n.PaisISO2.String))
		}
		payload.Levels = append(payload.Levels, territoriExportLevel{
			ID:       n.ID,
			PaisISO2: iso2,
			Nivel:    n.Nivel,
			Nom:      n.NomNivell,
			Tipus:    n.TipusNivell,
			Codi:     n.CodiOficial,
			Altres:   n.Altres,
			ParentID: parent,
			AnyInici: anyInici,
			AnyFi:    anyFi,
			Estat:    n.Estat,
		})
	}
	for _, row := range municipiRows {
		m, err := a.DB.GetMunicipi(row.ID)
		if err != nil {
			continue
		}
		nivells := make([]int, 7)
		for i := 0; i < 7; i++ {
			if m.NivellAdministratiuID[i].Valid {
				nivells[i] = int(m.NivellAdministratiuID[i].Int64)
			}
		}
		var parent *int
		if m.MunicipiID.Valid {
			v := int(m.MunicipiID.Int64)
			parent = &v
		}
		var lat *float64
		if m.Latitud.Valid {
			v := m.Latitud.Float64
			lat = &v
		}
		var lon *float64
		if m.Longitud.Valid {
			v := m.Longitud.Float64
			lon = &v
		}
		paisISO2 := ""
		if nivells[0] > 0 {
			paisISO2 = levelISO[nivells[0]]
		}
		payload.Municipis = append(payload.Municipis, territoriExportMunicipi{
			ID:         m.ID,
			PaisISO2:   paisISO2,
			Nom:        m.Nom,
			Tipus:      m.Tipus,
			ParentID:   parent,
			Nivells:    nivells,
			CodiPostal: m.CodiPostal,
			Latitud:    lat,
			Longitud:   lon,
			What3Words: m.What3Words,
			Web:        m.Web,
			Wikipedia:  m.Wikipedia,
			Altres:     m.Altres,
			Estat:      m.Estat,
		})
	}
	sort.Slice(payload.Levels, func(i, j int) bool {
		if payload.Levels[i].Nivel == payload.Levels[j].Nivel {
			return payload.Levels[i].ID < payload.Levels[j].ID
		}
		return payload.Levels[i].Nivel < payload.Levels[j].Nivel
	})
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=territori-export.json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func (a *App) AdminTerritoriImportRun(w http.ResponseWriter, r *http.Request) {
	user, _, ok := a.requirePermission(w, r, permAdmin)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Redirect(w, r, "/admin/territori/import?err=1", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Redirect(w, r, "/admin/territori/import?err=1", http.StatusSeeOther)
		return
	}
	file, _, err := r.FormFile("import_file")
	if err != nil {
		http.Redirect(w, r, "/admin/territori/import?err=1", http.StatusSeeOther)
		return
	}
	defer file.Close()
	var payload territoriExportPayload
	dec := json.NewDecoder(file)
	if err := dec.Decode(&payload); err != nil {
		http.Redirect(w, r, "/admin/territori/import?err=1", http.StatusSeeOther)
		return
	}
	paisos, err := a.DB.ListPaisos()
	if err != nil {
		http.Redirect(w, r, "/admin/territori/import?err=1", http.StatusSeeOther)
		return
	}
	paisByISO2 := map[string]db.Pais{}
	for _, p := range paisos {
		iso2 := strings.ToUpper(strings.TrimSpace(p.CodiISO2))
		if iso2 != "" {
			paisByISO2[iso2] = p
		}
	}
	countriesCreated := 0
	for _, c := range payload.Countries {
		iso2 := strings.ToUpper(strings.TrimSpace(c.ISO2))
		if iso2 == "" {
			continue
		}
		if _, ok := paisByISO2[iso2]; ok {
			continue
		}
		p := db.Pais{
			CodiISO2:    iso2,
			CodiISO3:    strings.ToUpper(strings.TrimSpace(c.ISO3)),
			CodiPaisNum: strings.TrimSpace(c.Num),
		}
		if _, err := a.DB.CreatePais(&p); err != nil {
			continue
		}
		paisByISO2[iso2] = p
		countriesCreated++
	}

	levelIDMap := map[int]int{}
	levelsTotal := len(payload.Levels)
	levelsCreated := 0
	levelsSkipped := 0
	levelsErrors := 0
	pending := make([]territoriExportLevel, 0, len(payload.Levels))
	for _, l := range payload.Levels {
		pending = append(pending, l)
	}
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].Nivel == pending[j].Nivel {
			return pending[i].ID < pending[j].ID
		}
		return pending[i].Nivel < pending[j].Nivel
	})
	for len(pending) > 0 {
		progressed := false
		next := make([]territoriExportLevel, 0, len(pending))
		for _, l := range pending {
			iso2 := strings.ToUpper(strings.TrimSpace(l.PaisISO2))
			pais, ok := paisByISO2[iso2]
			if !ok || iso2 == "" {
				levelsSkipped++
				continue
			}
			var parent sql.NullInt64
			if l.ParentID != nil && *l.ParentID > 0 {
				if pid, ok := levelIDMap[*l.ParentID]; ok {
					parent = sql.NullInt64{Int64: int64(pid), Valid: true}
				} else {
					next = append(next, l)
					continue
				}
			}
			n := db.NivellAdministratiu{
				PaisID:         pais.ID,
				Nivel:          l.Nivel,
				NomNivell:      strings.TrimSpace(l.Nom),
				TipusNivell:    strings.TrimSpace(l.Tipus),
				CodiOficial:    strings.TrimSpace(l.Codi),
				Altres:         l.Altres,
				ParentID:       parent,
				AnyInici:       intPtrToNull(l.AnyInici),
				AnyFi:          intPtrToNull(l.AnyFi),
				Estat:          strings.TrimSpace(l.Estat),
				CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
				ModeracioEstat: "pendent",
			}
			if n.Estat == "" {
				n.Estat = "actiu"
			}
			id, err := a.DB.CreateNivell(&n)
			if err != nil {
				levelsErrors++
				continue
			}
			levelIDMap[l.ID] = id
			levelsCreated++
			progressed = true
			_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleNivellCreate, "crear", "nivell", &id, "pendent", nil, "import")
		}
		if !progressed {
			levelsSkipped += len(next)
			break
		}
		pending = next
	}

	municipisTotal := len(payload.Municipis)
	municipisCreated := 0
	municipisSkipped := 0
	municipisErrors := 0
	munIDMap := map[int]int{}
	imported := []territoriImportMunicipi{}
	for _, mu := range payload.Municipis {
		if strings.TrimSpace(mu.Nom) == "" {
			municipisSkipped++
			continue
		}
		nivells := normalizeNivellSlice(mu.Nivells)
		var m db.Municipi
		m.Nom = strings.TrimSpace(mu.Nom)
		m.Tipus = strings.TrimSpace(mu.Tipus)
		m.CodiPostal = strings.TrimSpace(mu.CodiPostal)
		m.Latitud = floatPtrToNull(mu.Latitud)
		m.Longitud = floatPtrToNull(mu.Longitud)
		m.What3Words = strings.TrimSpace(mu.What3Words)
		m.Web = strings.TrimSpace(mu.Web)
		m.Wikipedia = strings.TrimSpace(mu.Wikipedia)
		m.Altres = mu.Altres
		m.Estat = strings.TrimSpace(mu.Estat)
		if m.Estat == "" {
			m.Estat = "actiu"
		}
		m.CreatedBy = sql.NullInt64{Int64: int64(user.ID), Valid: true}
		m.ModeracioEstat = "pendent"
		for i := 0; i < 7; i++ {
			if nivells[i] > 0 {
				if id, ok := levelIDMap[nivells[i]]; ok {
					m.NivellAdministratiuID[i] = sql.NullInt64{Int64: int64(id), Valid: true}
				}
			}
		}
		oldParent := 0
		if mu.ParentID != nil && *mu.ParentID > 0 {
			oldParent = *mu.ParentID
		}
		newID, err := a.DB.CreateMunicipi(&m)
		if err != nil {
			municipisErrors++
			continue
		}
		m.ID = newID
		municipisCreated++
		munIDMap[mu.ID] = newID
		imported = append(imported, territoriImportMunicipi{
			row: territoriexportMunicipiRow{
				ID:         mu.ID,
				ParentID:   oldParent,
				PaisISO2:   strings.TrimSpace(mu.PaisISO2),
				Nom:        mu.Nom,
				Tipus:      mu.Tipus,
				Nivells:    nivells,
				CodiPostal: mu.CodiPostal,
				Latitud:    mu.Latitud,
				Longitud:   mu.Longitud,
				What3Words: mu.What3Words,
				Web:        mu.Web,
				Wikipedia:  mu.Wikipedia,
				Altres:     mu.Altres,
				Estat:      mu.Estat,
			},
			municipi:  m,
			oldParent: oldParent,
		})
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleMunicipiCreate, "crear", "municipi", &newID, "pendent", nil, "import")
	}
	for _, imp := range imported {
		if imp.oldParent <= 0 {
			continue
		}
		if newParent, ok := munIDMap[imp.oldParent]; ok {
			imp.municipi.MunicipiID = sql.NullInt64{Int64: int64(newParent), Valid: true}
			_ = a.DB.UpdateMunicipi(&imp.municipi)
		}
	}

	redirect := "/admin/territori/import?import=1" +
		"&countries_created=" + strconv.Itoa(countriesCreated) +
		"&levels_total=" + strconv.Itoa(levelsTotal) +
		"&levels_created=" + strconv.Itoa(levelsCreated) +
		"&levels_skipped=" + strconv.Itoa(levelsSkipped) +
		"&levels_errors=" + strconv.Itoa(levelsErrors) +
		"&municipis_total=" + strconv.Itoa(municipisTotal) +
		"&municipis_created=" + strconv.Itoa(municipisCreated) +
		"&municipis_skipped=" + strconv.Itoa(municipisSkipped) +
		"&municipis_errors=" + strconv.Itoa(municipisErrors)
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}

func intPtrToNull(v *int) sql.NullInt64 {
	if v == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(*v), Valid: true}
}

func floatPtrToNull(v *float64) sql.NullFloat64 {
	if v == nil {
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{Float64: *v, Valid: true}
}

func normalizeNivellSlice(v []int) []int {
	res := make([]int, 7)
	copy(res, v)
	return res
}
