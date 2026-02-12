package core

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type indexerOption struct {
	Value string `json:"value"`
	Label string `json:"label"`
}

type indexerField struct {
	Key         string          `json:"key"`
	Label       string          `json:"label"`
	Input       string          `json:"input"`
	Options     []indexerOption `json:"options,omitempty"`
	Default     string          `json:"default,omitempty"`
	Target      string          `json:"target"`
	RawField    string          `json:"raw_field,omitempty"`
	AttrKey     string          `json:"attr_key,omitempty"`
	AttrType    string          `json:"attr_type,omitempty"`
	PersonKey   string          `json:"person_key,omitempty"`
	Role        string          `json:"role,omitempty"`
	PersonField string          `json:"person_field,omitempty"`
}

type indexerConfig struct {
	BookType string         `json:"book_type"`
	MaxRows  int            `json:"max_rows"`
	Fields   []indexerField `json:"fields"`
}

type indexerPayload struct {
	Rows []map[string]string `json:"rows"`
}

func (a *App) AdminIndexarLlibre(w http.ResponseWriter, r *http.Request) {
	lang := ResolveLang(r)
	llibreID := extractID(r.URL.Path)
	if llibreID == 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveLlibreTarget(llibreID)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresBulkIndex, target)
	if !ok {
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	canManageArxius := a.hasPerm(perms, permArxius)
	canManagePolicies := perms.CanManagePolicies || perms.Admin
	canModerate := perms.CanModerate || perms.Admin
	llibre, err := a.DB.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		http.NotFound(w, r)
		return
	}
	municipiNom := ""
	if llibre.MunicipiID != 0 {
		if mun, err := a.DB.GetMunicipi(llibre.MunicipiID); err == nil && mun != nil {
			municipiNom = mun.Nom
		}
	}
	cfg := buildIndexerConfig(lang, llibre)
	var draftRows []map[string]string
	if draft, err := a.DB.GetTranscripcioDraft(user.ID, llibreID); err == nil && draft != nil && draft.Payload != "" {
		var payload indexerPayload
		if json.Unmarshal([]byte(draft.Payload), &payload) == nil {
			draftRows = payload.Rows
		}
	}
	RenderPrivateTemplate(w, r, "admin-llibres-indexar.html", map[string]interface{}{
		"Llibre":            llibre,
		"Indexer":           cfg,
		"DraftRows":         draftRows,
		"TipusActe":         cfg.BookType,
		"MunicipiNom":       municipiNom,
		"User":              user,
		"CanManageArxius":   canManageArxius,
		"CanManagePolicies": canManagePolicies,
		"CanModerate":       canModerate,
		"ReturnTo":          "/documentals/llibres/" + strconv.Itoa(llibreID),
		"QualityLabel":      transcripcioQualitatLabels(lang),
	})
}

func (a *App) AdminIndexarDraft(w http.ResponseWriter, r *http.Request) {
	if !validateCSRF(r, r.Header.Get("X-CSRF-Token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	llibreID := extractID(r.URL.Path)
	if llibreID == 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveLlibreTarget(llibreID)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresBulkIndex, target)
	if !ok {
		return
	}
	payload, raw, err := parseIndexerPayload(r, 400)
	if err != nil {
		http.Error(w, "Payload invàlid", http.StatusBadRequest)
		return
	}
	if payload.Rows == nil {
		payload.Rows = []map[string]string{}
	}
	if err := a.DB.SaveTranscripcioDraft(user.ID, llibreID, string(raw)); err != nil {
		http.Error(w, "No s'ha pogut desar l'esborrany", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"ok": true})
}

func (a *App) AdminClearIndexerDraft(w http.ResponseWriter, r *http.Request) {
	if !validateCSRF(r, r.Header.Get("X-CSRF-Token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	llibreID := extractID(r.URL.Path)
	if llibreID == 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveLlibreTarget(llibreID)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresBulkIndex, target)
	if !ok {
		return
	}
	_ = a.DB.DeleteTranscripcioDraft(user.ID, llibreID)
	writeJSON(w, map[string]interface{}{"ok": true})
}

func (a *App) AdminCommitIndexer(w http.ResponseWriter, r *http.Request) {
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	llibreID := extractID(r.URL.Path)
	if llibreID == 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveLlibreTarget(llibreID)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresBulkIndex, target)
	if !ok {
		return
	}
	llibre, err := a.DB.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		http.NotFound(w, r)
		return
	}
	rawPayload := strings.TrimSpace(r.FormValue("payload"))
	if rawPayload == "" {
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"/indexar?error=empty", http.StatusSeeOther)
		return
	}
	var payload indexerPayload
	if err := json.Unmarshal([]byte(rawPayload), &payload); err != nil {
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"/indexar?error=invalid", http.StatusSeeOther)
		return
	}
	cfg := buildIndexerConfig(ResolveLang(r), llibre)
	if cfg.MaxRows > 0 && len(payload.Rows) > cfg.MaxRows {
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"/indexar?error=limit", http.StatusSeeOther)
		return
	}
	if err := a.checkIndexerPageLimits(llibreID, cfg, payload.Rows); err != nil {
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"/indexar?error=page_limit", http.StatusSeeOther)
		return
	}
	created := 0
	for _, row := range payload.Rows {
		if isIndexerRowEmpty(row) {
			continue
		}
		if err := a.createTranscripcioFromRow(cfg, llibreID, user.ID, row); err != nil {
			http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"/indexar?error=save", http.StatusSeeOther)
			return
		}
		created++
	}
	if created > 0 {
		_ = a.DB.DeleteTranscripcioDraft(user.ID, llibreID)
		_, _ = a.recalcLlibreIndexacioStats(llibreID)
	}
	returnURL := "/documentals/llibres/" + strconv.Itoa(llibreID) + "/registres?imported=" + strconv.Itoa(created) + "&failed=0"
	returnTo := safeReturnTo(r.FormValue("return_to"), returnURL)
	http.Redirect(w, r, returnTo, http.StatusSeeOther)
}

func (a *App) checkIndexerPageLimits(llibreID int, cfg indexerConfig, rows []map[string]string) error {
	pageKey, digitalKey := indexerPageKeys(cfg)
	if pageKey == "" && digitalKey == "" {
		return nil
	}
	stats, err := a.DB.ListTranscripcionsRawPageStats(llibreID)
	if err != nil || len(stats) == 0 {
		return nil
	}
	limits := map[string]int{}
	for _, stat := range stats {
		if stat.TotalRegistres <= 0 {
			continue
		}
		key := strings.TrimSpace(stat.NumPaginaText)
		if key == "" {
			continue
		}
		if _, ok := limits[key]; !ok {
			limits[key] = stat.TotalRegistres
		}
	}
	if len(limits) == 0 {
		return nil
	}
	newCounts := map[string]int{}
	for _, row := range rows {
		if isIndexerRowEmpty(row) {
			continue
		}
		key := indexerRowPageValue(row, pageKey, digitalKey)
		if key == "" {
			continue
		}
		newCounts[key]++
	}
	for key, newCount := range newCounts {
		limit, ok := limits[key]
		if !ok || limit <= 0 {
			continue
		}
		existing, err := a.DB.CountTranscripcionsRawByPageValue(llibreID, key)
		if err != nil {
			Errorf("Error comptant registres per pagina %d (%s): %v", llibreID, key, err)
			continue
		}
		if existing+newCount > limit {
			return errors.New("page limit exceeded")
		}
	}
	return nil
}

func indexerPageKeys(cfg indexerConfig) (string, string) {
	pageKey := ""
	digitalKey := ""
	for _, field := range cfg.Fields {
		if field.Target == "raw" && field.RawField == "num_pagina_text" {
			pageKey = field.Key
		}
		if field.Target == "attr" && field.AttrKey == "pagina_digital" {
			digitalKey = field.Key
		}
	}
	return pageKey, digitalKey
}

func indexerRowPageValue(row map[string]string, pageKey, digitalKey string) string {
	if digitalKey != "" {
		if val := strings.TrimSpace(row[digitalKey]); val != "" {
			return val
		}
	}
	if pageKey != "" {
		return strings.TrimSpace(row[pageKey])
	}
	return ""
}

func parseIndexerPayload(r *http.Request, maxRows int) (*indexerPayload, []byte, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 2*1024*1024))
	if err != nil {
		return nil, nil, err
	}
	if len(body) == 0 {
		return nil, nil, errors.New("empty")
	}
	var payload indexerPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, nil, err
	}
	if maxRows > 0 && len(payload.Rows) > maxRows {
		return nil, nil, errors.New("too many rows")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, err
	}
	return &payload, raw, nil
}

func buildIndexerConfig(lang string, llibre *db.Llibre) indexerConfig {
	bookType := normalizeIndexerBookType(llibre.TipusLlibre)
	fields := indexerSchema(bookType)
	withLabels := make([]indexerField, 0, len(fields))
	for _, f := range fields {
		f.Label = indexerFieldLabel(lang, f)
		if f.Input == "select" {
			if f.RawField == "data_acte_estat" {
				f.Options = []indexerOption{
					{Value: "incomplet", Label: T(lang, "records.indexing.partial")},
					{Value: "clar", Label: T(lang, "records.indexing.complete")},
				}
				if f.Default == "" {
					f.Default = "incomplet"
				}
			} else if f.Target == "person" && strings.HasSuffix(f.PersonField, "_estat") {
				if len(f.Options) == 0 {
					for _, opt := range transcripcioQualitat {
						f.Options = append(f.Options, indexerOption{
							Value: opt,
							Label: T(lang, "records.quality."+opt),
						})
					}
				}
				if f.Default == "" {
					f.Default = "clar"
				}
			} else if f.Target == "person" && f.PersonField == "sexe" && len(f.Options) == 0 {
				f.Options = []indexerOption{
					{Value: "masculi", Label: T(lang, "records.value.sex.masculi")},
					{Value: "femeni", Label: T(lang, "records.value.sex.femeni")},
				}
			} else if f.Target == "attr" && f.AttrType == "bool" && len(f.Options) == 0 {
				f.Options = []indexerOption{
					{Value: "1", Label: T(lang, "common.yes")},
					{Value: "0", Label: T(lang, "common.no")},
				}
			} else if f.Target == "attr" && f.AttrKey == "condicio_padro" && len(f.Options) == 0 {
				f.Options = []indexerOption{
					{Value: "cap_familia", Label: T(lang, "records.value.padron.cap_familia")},
					{Value: "nen", Label: T(lang, "records.value.padron.nen")},
					{Value: "no_consta", Label: T(lang, "records.value.padron.no_consta")},
				}
			}
		}
		withLabels = append(withLabels, f)
	}
	return indexerConfig{
		BookType: bookType,
		MaxRows:  400,
		Fields:   withLabels,
	}
}

func normalizeIndexerBookType(val string) string {
	val = strings.ToLower(strings.TrimSpace(val))
	val = strings.ReplaceAll(val, "_", "")
	switch val {
	case "baptismes", "baptisme", "baptismesllibre":
		return "baptismes"
	case "matrimonis", "matrimoni":
		return "matrimonis"
	case "obits", "obit":
		return "obits"
	case "confirmacions", "confirmacio":
		return "confirmacions"
	case "padrons", "padro", "censos", "cens":
		return "padrons"
	case "reclutaments", "reclutament":
		return "reclutaments"
	default:
		return "altres"
	}
}

func indexerSchema(bookType string) []indexerField {
	switch bookType {
	case "baptismes":
		return indexerSchemaBaptismes()
	case "matrimonis":
		return indexerSchemaMatrimonis()
	case "obits":
		return indexerSchemaObits()
	case "confirmacions":
		return indexerSchemaConfirmacions()
	case "padrons":
		return indexerSchemaPadrons()
	case "reclutaments":
		return indexerSchemaReclutaments()
	default:
		return indexerSchemaAltres()
	}
}

func indexerSchemaBaptismes() []indexerField {
	fields := []indexerField{
		personField("batejat", "batejat", "cognom1"),
		personFieldQualitat("batejat", "batejat", "cognom1"),
		personField("batejat", "batejat", "cognom2"),
		personFieldQualitat("batejat", "batejat", "cognom2"),
		personField("batejat", "batejat", "nom"),
		personFieldQualitat("batejat", "batejat", "nom"),
		personFieldInput("batejat", "batejat", "sexe", "select"),
		personFieldQualitat("batejat", "batejat", "sexe"),
		attrField("data_naixement", "data_naixement", "date"),
		attrField("data_bateig", "data_bateig", "date"),
		rawField("pagina_llibre", "num_pagina_text", "text"),
		attrField("pagina_digital", "pagina_digital", "text"),
		rawField("any", "any_doc", "number"),
		personField("pare", "pare", "cognom1"),
		personFieldQualitat("pare", "pare", "cognom1"),
		personField("pare", "pare", "cognom2"),
		personFieldQualitat("pare", "pare", "cognom2"),
		personField("pare", "pare", "nom"),
		personFieldQualitat("pare", "pare", "nom"),
		personField("pare", "pare", "ofici"),
		personFieldQualitat("pare", "pare", "ofici"),
		personField("mare", "mare", "cognom1"),
		personFieldQualitat("mare", "mare", "cognom1"),
		personField("mare", "mare", "cognom2"),
		personFieldQualitat("mare", "mare", "cognom2"),
		personField("mare", "mare", "nom"),
		personFieldQualitat("mare", "mare", "nom"),
		personField("avi_patern", "avi_patern", "cognom1"),
		personFieldQualitat("avi_patern", "avi_patern", "cognom1"),
		personField("avi_patern", "avi_patern", "cognom2"),
		personFieldQualitat("avi_patern", "avi_patern", "cognom2"),
		personField("avi_patern", "avi_patern", "nom"),
		personFieldQualitat("avi_patern", "avi_patern", "nom"),
		personField("avia_paterna", "avia_paterna", "cognom1"),
		personFieldQualitat("avia_paterna", "avia_paterna", "cognom1"),
		personField("avia_paterna", "avia_paterna", "cognom2"),
		personFieldQualitat("avia_paterna", "avia_paterna", "cognom2"),
		personField("avia_paterna", "avia_paterna", "nom"),
		personFieldQualitat("avia_paterna", "avia_paterna", "nom"),
		personField("avi_matern", "avi_matern", "cognom1"),
		personFieldQualitat("avi_matern", "avi_matern", "cognom1"),
		personField("avi_matern", "avi_matern", "cognom2"),
		personFieldQualitat("avi_matern", "avi_matern", "cognom2"),
		personField("avi_matern", "avi_matern", "nom"),
		personFieldQualitat("avi_matern", "avi_matern", "nom"),
		personField("avia_materna", "avia_materna", "cognom1"),
		personFieldQualitat("avia_materna", "avia_materna", "cognom1"),
		personField("avia_materna", "avia_materna", "cognom2"),
		personFieldQualitat("avia_materna", "avia_materna", "cognom2"),
		personField("avia_materna", "avia_materna", "nom"),
		personFieldQualitat("avia_materna", "avia_materna", "nom"),
		personField("padri", "padri", "cognom1"),
		personFieldQualitat("padri", "padri", "cognom1"),
		personField("padri", "padri", "cognom2"),
		personFieldQualitat("padri", "padri", "cognom2"),
		personField("padri", "padri", "nom"),
		personFieldQualitat("padri", "padri", "nom"),
		personField("padri", "padri", "notes"),
		personField("padrina", "padrina", "cognom1"),
		personFieldQualitat("padrina", "padrina", "cognom1"),
		personField("padrina", "padrina", "cognom2"),
		personFieldQualitat("padrina", "padrina", "cognom2"),
		personField("padrina", "padrina", "nom"),
		personFieldQualitat("padrina", "padrina", "nom"),
		personField("padrina", "padrina", "notes"),
		attrField("data_defuncio", "data_defuncio", "date"),
		rawField("qualitat_general", "data_acte_estat", "select"),
		rawField("notes_marginals", "notes_marginals", "textarea"),
		rawField("observacions", "observacions_paleografiques", "textarea"),
	}
	return fields
}

func indexerSchemaMatrimonis() []indexerField {
	fields := []indexerField{
		personField("nuvi", "nuvi", "cognom1"),
		personFieldQualitat("nuvi", "nuvi", "cognom1"),
		personField("nuvi", "nuvi", "cognom2"),
		personFieldQualitat("nuvi", "nuvi", "cognom2"),
		personField("nuvi", "nuvi", "nom"),
		personFieldQualitat("nuvi", "nuvi", "nom"),
		personField("nuvi", "nuvi", "ofici"),
		personFieldQualitat("nuvi", "nuvi", "ofici"),
		personField("nuvi", "nuvi", "municipi"),
		personFieldQualitat("nuvi", "nuvi", "municipi"),
		personField("novia", "novia", "cognom1"),
		personFieldQualitat("novia", "novia", "cognom1"),
		personField("novia", "novia", "cognom2"),
		personFieldQualitat("novia", "novia", "cognom2"),
		personField("novia", "novia", "nom"),
		personFieldQualitat("novia", "novia", "nom"),
		personField("novia", "novia", "ofici"),
		personFieldQualitat("novia", "novia", "ofici"),
		personField("novia", "novia", "municipi"),
		personFieldQualitat("novia", "novia", "municipi"),
		attrField("data_matrimoni", "data_matrimoni", "date"),
		rawField("pagina_llibre", "num_pagina_text", "text"),
		attrField("pagina_digital", "pagina_digital", "text"),
		rawField("any", "any_doc", "number"),
		personField("pare_nuvi", "pare_nuvi", "cognom1"),
		personFieldQualitat("pare_nuvi", "pare_nuvi", "cognom1"),
		personField("pare_nuvi", "pare_nuvi", "cognom2"),
		personFieldQualitat("pare_nuvi", "pare_nuvi", "cognom2"),
		personField("pare_nuvi", "pare_nuvi", "nom"),
		personFieldQualitat("pare_nuvi", "pare_nuvi", "nom"),
		personField("pare_nuvi", "pare_nuvi", "ofici"),
		personFieldQualitat("pare_nuvi", "pare_nuvi", "ofici"),
		personField("pare_nuvi", "pare_nuvi", "municipi"),
		personFieldQualitat("pare_nuvi", "pare_nuvi", "municipi"),
		personField("mare_nuvi", "mare_nuvi", "cognom1"),
		personFieldQualitat("mare_nuvi", "mare_nuvi", "cognom1"),
		personField("mare_nuvi", "mare_nuvi", "cognom2"),
		personFieldQualitat("mare_nuvi", "mare_nuvi", "cognom2"),
		personField("mare_nuvi", "mare_nuvi", "nom"),
		personFieldQualitat("mare_nuvi", "mare_nuvi", "nom"),
		personField("mare_nuvi", "mare_nuvi", "ofici"),
		personFieldQualitat("mare_nuvi", "mare_nuvi", "ofici"),
		personField("mare_nuvi", "mare_nuvi", "municipi"),
		personFieldQualitat("mare_nuvi", "mare_nuvi", "municipi"),
		personField("pare_novia", "pare_novia", "cognom1"),
		personFieldQualitat("pare_novia", "pare_novia", "cognom1"),
		personField("pare_novia", "pare_novia", "cognom2"),
		personFieldQualitat("pare_novia", "pare_novia", "cognom2"),
		personField("pare_novia", "pare_novia", "nom"),
		personFieldQualitat("pare_novia", "pare_novia", "nom"),
		personField("pare_novia", "pare_novia", "ofici"),
		personFieldQualitat("pare_novia", "pare_novia", "ofici"),
		personField("pare_novia", "pare_novia", "municipi"),
		personFieldQualitat("pare_novia", "pare_novia", "municipi"),
		personField("mare_novia", "mare_novia", "cognom1"),
		personFieldQualitat("mare_novia", "mare_novia", "cognom1"),
		personField("mare_novia", "mare_novia", "cognom2"),
		personFieldQualitat("mare_novia", "mare_novia", "cognom2"),
		personField("mare_novia", "mare_novia", "nom"),
		personFieldQualitat("mare_novia", "mare_novia", "nom"),
		personField("mare_novia", "mare_novia", "ofici"),
		personFieldQualitat("mare_novia", "mare_novia", "ofici"),
		personField("mare_novia", "mare_novia", "municipi"),
		personFieldQualitat("mare_novia", "mare_novia", "municipi"),
		personField("testimoni1", "testimoni", "cognom1"),
		personFieldQualitat("testimoni1", "testimoni", "cognom1"),
		personField("testimoni1", "testimoni", "cognom2"),
		personFieldQualitat("testimoni1", "testimoni", "cognom2"),
		personField("testimoni1", "testimoni", "nom"),
		personFieldQualitat("testimoni1", "testimoni", "nom"),
		personField("testimoni2", "testimoni", "cognom1"),
		personFieldQualitat("testimoni2", "testimoni", "cognom1"),
		personField("testimoni2", "testimoni", "cognom2"),
		personFieldQualitat("testimoni2", "testimoni", "cognom2"),
		personField("testimoni2", "testimoni", "nom"),
		personFieldQualitat("testimoni2", "testimoni", "nom"),
		rawField("qualitat_general", "data_acte_estat", "select"),
		rawField("notes_marginals", "notes_marginals", "textarea"),
	}
	return fields
}

func indexerSchemaObits() []indexerField {
	fields := []indexerField{
		personField("difunt", "difunt", "cognom1"),
		personFieldQualitat("difunt", "difunt", "cognom1"),
		personField("difunt", "difunt", "cognom2"),
		personFieldQualitat("difunt", "difunt", "cognom2"),
		personField("difunt", "difunt", "nom"),
		personFieldQualitat("difunt", "difunt", "nom"),
		personFieldInput("difunt", "difunt", "sexe", "select"),
		personFieldQualitat("difunt", "difunt", "sexe"),
		attrField("data_defuncio", "data_defuncio", "date"),
		attrField("data_enterrament", "data_enterrament", "date"),
		attrField("edat", "edat", "int"),
		attrField("causa", "causa", "text"),
		personField("parella", "parella", "cognom1"),
		personFieldQualitat("parella", "parella", "cognom1"),
		personField("parella", "parella", "cognom2"),
		personFieldQualitat("parella", "parella", "cognom2"),
		personField("parella", "parella", "nom"),
		personFieldQualitat("parella", "parella", "nom"),
		personField("pare", "pare", "cognom1"),
		personFieldQualitat("pare", "pare", "cognom1"),
		personField("pare", "pare", "cognom2"),
		personFieldQualitat("pare", "pare", "cognom2"),
		personField("pare", "pare", "nom"),
		personFieldQualitat("pare", "pare", "nom"),
		personField("mare", "mare", "cognom1"),
		personFieldQualitat("mare", "mare", "cognom1"),
		personField("mare", "mare", "cognom2"),
		personFieldQualitat("mare", "mare", "cognom2"),
		personField("mare", "mare", "nom"),
		personFieldQualitat("mare", "mare", "nom"),
		attrField("testament", "testament", "bool"),
		attrField("data_testament", "data_testament", "date"),
		attrField("testament_nom", "testament_nom", "text"),
		attrField("classe_enterrament", "classe_enterrament", "text"),
		rawField("pagina_llibre", "num_pagina_text", "text"),
		attrField("pagina_digital", "pagina_digital", "text"),
		rawField("any", "any_doc", "number"),
		rawField("qualitat_general", "data_acte_estat", "select"),
		rawField("notes_marginals", "notes_marginals", "textarea"),
	}
	return fields
}

func indexerSchemaConfirmacions() []indexerField {
	fields := []indexerField{
		personField("confirmat", "confirmat", "cognom1"),
		personFieldQualitat("confirmat", "confirmat", "cognom1"),
		personField("confirmat", "confirmat", "cognom2"),
		personFieldQualitat("confirmat", "confirmat", "cognom2"),
		personField("confirmat", "confirmat", "nom"),
		personFieldQualitat("confirmat", "confirmat", "nom"),
		personFieldInput("confirmat", "confirmat", "sexe", "select"),
		personFieldQualitat("confirmat", "confirmat", "sexe"),
		personField("confirmat", "confirmat", "ofici"),
		personFieldQualitat("confirmat", "confirmat", "ofici"),
		personField("confirmat", "confirmat", "casa"),
		personFieldQualitat("confirmat", "confirmat", "casa"),
		attrField("data_confirmacio", "data_confirmacio", "date"),
		personField("pare", "pare", "cognom1"),
		personFieldQualitat("pare", "pare", "cognom1"),
		personField("pare", "pare", "cognom2"),
		personFieldQualitat("pare", "pare", "cognom2"),
		personField("pare", "pare", "nom"),
		personFieldQualitat("pare", "pare", "nom"),
		personField("pare", "pare", "ofici"),
		personFieldQualitat("pare", "pare", "ofici"),
		personField("pare", "pare", "municipi"),
		personFieldQualitat("pare", "pare", "municipi"),
		personField("mare", "mare", "cognom1"),
		personFieldQualitat("mare", "mare", "cognom1"),
		personField("mare", "mare", "cognom2"),
		personFieldQualitat("mare", "mare", "cognom2"),
		personField("mare", "mare", "nom"),
		personFieldQualitat("mare", "mare", "nom"),
		personField("mare", "mare", "municipi"),
		personFieldQualitat("mare", "mare", "municipi"),
		rawField("pagina_llibre", "num_pagina_text", "text"),
		rawField("any", "any_doc", "number"),
		rawField("qualitat_general", "data_acte_estat", "select"),
		rawField("notes_marginals", "notes_marginals", "textarea"),
	}
	return fields
}

func indexerSchemaPadrons() []indexerField {
	fields := []indexerField{
		attrField("carrer", "carrer", "text"),
		attrField("numero_casa", "numero_casa", "text"),
		attrField("adreca", "adreca", "text"),
		attrField("localitat", "localitat", "text"),
		personField("cap_familia", "cap_familia", "cognom1"),
		personFieldQualitat("cap_familia", "cap_familia", "cognom1"),
		personField("cap_familia", "cap_familia", "cognom2"),
		personFieldQualitat("cap_familia", "cap_familia", "cognom2"),
		personField("cap_familia", "cap_familia", "nom"),
		personFieldQualitat("cap_familia", "cap_familia", "nom"),
		personFieldInput("cap_familia", "cap_familia", "sexe", "select"),
		personFieldQualitat("cap_familia", "cap_familia", "sexe"),
		attrField("data_naixement", "data_naixement", "date"),
		personField("cap_familia", "cap_familia", "edat"),
		personFieldQualitat("cap_familia", "cap_familia", "edat"),
		personField("cap_familia", "cap_familia", "estat_civil"),
		personFieldQualitat("cap_familia", "cap_familia", "estat_civil"),
		attrField("procedencia", "procedencia", "text"),
		personField("cap_familia", "cap_familia", "ofici"),
		personFieldQualitat("cap_familia", "cap_familia", "ofici"),
		personField("cap_familia", "cap_familia", "casa"),
		personFieldQualitat("cap_familia", "cap_familia", "casa"),
		attrField("condicio_padro", "condicio_padro", "text"),
		attrField("alfabetitzat", "alfabetitzat", "bool"),
		attrField("sap_llegir", "sap_llegir", "bool"),
		attrField("sap_escriure", "sap_escriure", "bool"),
		rawField("posicio", "posicio_pagina", "number"),
		rawField("any", "any_doc", "number"),
		rawField("pagina_llibre", "num_pagina_text", "text"),
		rawField("qualitat_general", "data_acte_estat", "select"),
		rawField("notes_marginals", "notes_marginals", "textarea"),
	}
	return fields
}

func indexerSchemaReclutaments() []indexerField {
	fields := []indexerField{
		personField("recluta", "recluta", "cognom1"),
		personFieldQualitat("recluta", "recluta", "cognom1"),
		personField("recluta", "recluta", "cognom2"),
		personFieldQualitat("recluta", "recluta", "cognom2"),
		personField("recluta", "recluta", "nom"),
		personFieldQualitat("recluta", "recluta", "nom"),
		attrField("edat", "edat", "int"),
		attrField("unitat", "unitat", "text"),
		rawField("any", "any_doc", "number"),
		rawField("pagina_llibre", "num_pagina_text", "text"),
		rawField("qualitat_general", "data_acte_estat", "select"),
		rawField("notes_marginals", "notes_marginals", "textarea"),
	}
	return fields
}

func indexerSchemaAltres() []indexerField {
	fields := []indexerField{
		personField("subjecte", "subjecte", "cognom1"),
		personFieldQualitat("subjecte", "subjecte", "cognom1"),
		personField("subjecte", "subjecte", "cognom2"),
		personFieldQualitat("subjecte", "subjecte", "cognom2"),
		personField("subjecte", "subjecte", "nom"),
		personFieldQualitat("subjecte", "subjecte", "nom"),
		attrField("data_acte", "data_acte", "date"),
		rawField("any", "any_doc", "number"),
		rawField("pagina_llibre", "num_pagina_text", "text"),
		rawField("qualitat_general", "data_acte_estat", "select"),
		rawField("notes_marginals", "notes_marginals", "textarea"),
	}
	return fields
}

func rawField(key, rawField, input string) indexerField {
	return indexerField{
		Key:      key,
		Input:    input,
		Target:   "raw",
		RawField: rawField,
	}
}

func attrField(key, attrKey, attrType string) indexerField {
	input := "text"
	if attrType == "date" {
		input = "date"
	} else if attrType == "int" {
		input = "number"
	} else if attrType == "bool" {
		input = "select"
	}
	if attrKey == "condicio_padro" {
		input = "select"
	}
	return indexerField{
		Key:      key,
		Input:    input,
		Target:   "attr",
		AttrKey:  attrKey,
		AttrType: attrType,
	}
}

func personField(personKey, role, field string) indexerField {
	return personFieldInput(personKey, role, field, "text")
}

func personFieldInput(personKey, role, field, input string) indexerField {
	return indexerField{
		Key:         personKey + "_" + field,
		Input:       input,
		Target:      "person",
		PersonKey:   personKey,
		Role:        role,
		PersonField: field,
	}
}

func personFieldQualitat(personKey, role, baseField string) indexerField {
	return personFieldInput(personKey, role, baseField+"_estat", "select")
}

func indexerFieldLabel(lang string, f indexerField) string {
	switch f.Target {
	case "person":
		field := f.PersonField
		if strings.HasSuffix(field, "_estat") {
			field = strings.TrimSuffix(field, "_estat")
			baseLabel := personFieldBaseLabel(lang, f.Role, field)
			return T(lang, "records.field.qualitat") + " " + baseLabel
		}
		return personFieldBaseLabel(lang, f.Role, field)
	case "attr":
		return T(lang, "records.field."+f.AttrKey)
	case "raw":
		switch f.RawField {
		case "num_pagina_text":
			return T(lang, "records.field.pagina_llibre")
		case "any_doc":
			return T(lang, "records.field.any")
		case "data_acte_estat":
			return T(lang, "records.field.estat_indexacio")
		case "posicio_pagina":
			return T(lang, "records.field.posicio")
		case "notes_marginals":
			return T(lang, "records.field.notes")
		case "observacions_paleografiques":
			return T(lang, "records.field.observacions")
		default:
			return T(lang, "records.field."+f.RawField)
		}
	default:
		return f.Key
	}
}

func personFieldBaseLabel(lang, role, field string) string {
	fieldLabel := T(lang, "records.field."+field)
	if role == "batejat" || role == "confirmat" || role == "difunt" {
		return fieldLabel
	}
	roleLabel := T(lang, "records.role."+role)
	parts := strings.Fields(fieldLabel)
	if len(parts) > 1 {
		last := parts[len(parts)-1]
		if last == "1" || last == "2" {
			base := strings.Join(parts[:len(parts)-1], " ")
			return base + " " + roleLabel + " " + last
		}
	}
	return fieldLabel + " " + roleLabel
}

func isIndexerRowEmpty(row map[string]string) bool {
	for k, v := range row {
		if strings.TrimSpace(v) == "" {
			continue
		}
		if k == "qualitat_general" || strings.HasSuffix(k, "_estat") {
			continue
		}
		return false
	}
	return true
}

func (a *App) createTranscripcioFromRow(cfg indexerConfig, llibreID int, userID int, row map[string]string) error {
	raw := db.TranscripcioRaw{
		LlibreID:       llibreID,
		TipusActe:      tipusActeFromBookType(cfg.BookType),
		ModeracioEstat: "pendent",
		CreatedBy:      sqlNullIntFromInt(userID),
		DataActeEstat:  "clar",
	}
	persones := map[string]*db.TranscripcioPersonaRaw{}
	atributs := map[string]*db.TranscripcioAtributRaw{}
	for _, field := range cfg.Fields {
		val := strings.TrimSpace(row[field.Key])
		if val == "" {
			continue
		}
		switch field.Target {
		case "raw":
			applyRawField(&raw, field.RawField, val)
		case "attr":
			attr := atributs[field.AttrKey]
			if attr == nil {
				attr = &db.TranscripcioAtributRaw{Clau: field.AttrKey, TipusValor: field.AttrType}
				atributs[field.AttrKey] = attr
			}
			applyAttrValue(attr, val)
		case "person":
			person := persones[field.PersonKey]
			if person == nil {
				person = &db.TranscripcioPersonaRaw{Rol: field.Role}
				persones[field.PersonKey] = person
			}
			applyPersonFieldIndex(person, field.PersonField, val)
		}
	}
	if !raw.DataActeISO.Valid {
		if date := inferActeDate(cfg.BookType, atributs); date.Valid {
			raw.DataActeISO = date
			raw.DataActeText = date.String
		}
	}
	if !raw.AnyDoc.Valid {
		if y := yearFromDate(raw.DataActeISO); y > 0 {
			raw.AnyDoc = sql.NullInt64{Int64: int64(y), Valid: true}
		}
	}
	rawID, err := a.DB.CreateTranscripcioRaw(&raw)
	if err != nil {
		return err
	}
	for _, p := range persones {
		if isEmptyPerson(p) {
			continue
		}
		p.TranscripcioID = rawID
		_, _ = a.DB.CreateTranscripcioPersona(p)
	}
	for _, attr := range atributs {
		if isEmptyAttr(attr) {
			continue
		}
		attr.TranscripcioID = rawID
		_, _ = a.DB.CreateTranscripcioAtribut(attr)
	}
	return nil
}

func tipusActeFromBookType(bookType string) string {
	switch bookType {
	case "baptismes":
		return "baptisme"
	case "matrimonis":
		return "matrimoni"
	case "obits":
		return "obit"
	case "confirmacions":
		return "confirmacio"
	case "padrons":
		return "padro"
	case "censos":
		return "padro"
	case "reclutaments":
		return "reclutament"
	default:
		return "altres"
	}
}

func applyRawField(raw *db.TranscripcioRaw, field string, val string) {
	switch field {
	case "num_pagina_text":
		raw.NumPaginaText = val
	case "posicio_pagina":
		if n, err := strconv.Atoi(val); err == nil {
			raw.PosicioPagina = sql.NullInt64{Int64: int64(n), Valid: true}
		}
	case "any_doc":
		if n, err := strconv.Atoi(val); err == nil {
			raw.AnyDoc = sql.NullInt64{Int64: int64(n), Valid: true}
		}
	case "data_acte_iso":
		raw.DataActeISO = parseNullString(val)
		raw.DataActeText = val
	case "data_acte_estat":
		if isValidQualitat(val) {
			raw.DataActeEstat = val
		}
	case "notes_marginals":
		raw.NotesMarginals = val
	case "observacions_paleografiques":
		raw.ObservacionsPaleografiques = val
	}
}

func applyPersonFieldIndex(person *db.TranscripcioPersonaRaw, field string, val string) {
	applyPersonField(person, field, val)
}

func isValidQualitat(val string) bool {
	for _, opt := range transcripcioQualitat {
		if opt == val {
			return true
		}
	}
	return false
}

func inferActeDate(bookType string, attrs map[string]*db.TranscripcioAtributRaw) sql.NullString {
	keys := []string{}
	switch bookType {
	case "baptismes":
		keys = []string{"data_bateig", "data_naixement"}
	case "matrimonis":
		keys = []string{"data_matrimoni"}
	case "obits":
		keys = []string{"data_defuncio", "data_enterrament"}
	case "confirmacions":
		keys = []string{"data_confirmacio"}
	case "padrons":
		keys = []string{"data_padro"}
	case "reclutaments":
		keys = []string{"data_reclutament"}
	default:
		keys = []string{"data_acte"}
	}
	for _, key := range keys {
		if attr, ok := attrs[key]; ok {
			if attr.ValorDate.Valid {
				return attr.ValorDate
			}
			if attr.ValorText != "" {
				return sql.NullString{String: attr.ValorText, Valid: true}
			}
		}
	}
	return sql.NullString{}
}

func yearFromDate(date sql.NullString) int {
	if !date.Valid {
		return 0
	}
	if len(date.String) < 4 {
		return 0
	}
	if n, err := strconv.Atoi(date.String[:4]); err == nil {
		return n
	}
	return 0
}

func isEmptyPerson(p *db.TranscripcioPersonaRaw) bool {
	return strings.TrimSpace(p.Nom) == "" &&
		strings.TrimSpace(p.Cognom1) == "" &&
		strings.TrimSpace(p.Cognom2) == "" &&
		strings.TrimSpace(p.CognomSoltera) == "" &&
		strings.TrimSpace(p.Notes) == ""
}

func isEmptyAttr(a *db.TranscripcioAtributRaw) bool {
	return strings.TrimSpace(a.ValorText) == "" &&
		!a.ValorInt.Valid &&
		!a.ValorDate.Valid &&
		!a.ValorBool.Valid
}

func writeJSON(w http.ResponseWriter, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}
