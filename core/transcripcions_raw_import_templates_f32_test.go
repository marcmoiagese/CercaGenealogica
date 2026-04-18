package core

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type f32ImportSnapshot struct {
	Registres []f32RegistreSnapshot
}

type f32RegistreSnapshot struct {
	NumPaginaText string
	AnyDoc        int64
	AnyDocValid   bool
	TipusActe     string
	DataActeText  string
	DataActeISO   string
	DataActeValid bool
	DataActeEstat string
	Persones      []f32PersonSnapshot
	Atributs      []f32AttrSnapshot
}

type f32PersonSnapshot struct {
	Rol           string
	Nom           string
	Cognom1       string
	Cognom2       string
	NomEstat      string
	Cognom1Estat  string
	Cognom2Estat  string
	MunicipiText  string
	MunicipiEstat string
	OficiText     string
	OficiEstat    string
	Notes         string
}

type f32AttrSnapshot struct {
	Clau       string
	TipusValor string
	ValorText  string
	ValorDate  string
	ValorInt   int64
	IntValid   bool
	Estat      string
}

func TestF321MarcmoiaTemplateEquivalentToStaticReference(t *testing.T) {
	csvContent := buildF32CSV(t, [][]string{
		{"llibre", "paginallibre", "paginareal", "any", "cognoms", "pare", "mare", "avispaterns", "avismaterns", "casat", "nascut", "padridbateig", "padrinetadebateig", "bateig", "ofici", "defuncio"},
		{"1890-1891", "12", "34", "1890", "Garcia Soler Joan (Arbeca)", "Pere Garcia Soler (Arbeca)", "Maria Puig Font", "Antoni Garcia i Teresa Soler", "Josep Puig i Rosa Font", "", "01/02/1890", "Jaume Padri Un", "Carme Padrina Una", "05/02/1890", "pagès", "31/12/1950"},
		{"1890-1891", "12", "34", "1890", "Garcia Soler Joan (Arbeca)", "Pere Garcia Soler (Arbeca)", "Maria Puig Font", "Antoni Garcia i Teresa Soler", "Josep Puig i Rosa Font", "", "01/02/1890", "Jaume Padri Dos", "Carme Padrina Dues", "05/02/1890", "pagès", "31/12/1950"},
		{"1900-1901", "1", "1", "1900", "Rovira Pere", "", "", "", "", "", "", "", "", "", "", ""},
	})

	staticResult, staticSnapshot := runF32StaticMarcmoiaImport(t, csvContent)
	templateResult, templateSnapshot := runF32TemplateMarcmoiaImport(t, csvContent)

	if staticResult.Created != 2 || staticResult.Failed != 1 {
		t.Fatalf("referència estàtica inesperada: created=%d failed=%d errors=%+v", staticResult.Created, staticResult.Failed, staticResult.Errors)
	}
	if templateResult.Created != staticResult.Created || templateResult.Failed != staticResult.Failed || templateResult.Updated != staticResult.Updated {
		t.Fatalf("comptadors no equivalents: static=%+v template=%+v", staticResult, templateResult)
	}
	if !reflect.DeepEqual(staticSnapshot, templateSnapshot) {
		t.Fatalf("snapshot funcional no equivalent\nstatic=%+v\ntemplate=%+v", staticSnapshot, templateSnapshot)
	}
}

func TestF321MarcmoiaTemplateKeepsImpossibleDatesAsRowData(t *testing.T) {
	csvContent := buildF32CSV(t, [][]string{
		{"llibre", "paginallibre", "paginareal", "any", "cognoms", "bateig", "nascut"},
		{"1890-1891", "1", "9", "1890", "Rovira Pere", "31/02/1890", "30/02/1890"},
	})
	result, snapshot := runF32TemplateMarcmoiaImport(t, csvContent)
	if result.Created != 1 || result.Failed != 0 {
		t.Fatalf("import amb data impossible hauria de continuar: %+v", result)
	}
	reg := snapshot.Registres[0]
	if reg.DataActeISO != "" || reg.DataActeValid || reg.DataActeText != "31/02/1890" || reg.DataActeEstat != "incomplet" {
		t.Fatalf("data acte impossible mal preservada: %+v", reg)
	}
	var foundNaixement bool
	for _, attr := range reg.Atributs {
		if attr.Clau == "data_naixement" {
			foundNaixement = true
			if attr.TipusValor != "text" || attr.ValorText != "30/02/1890" || attr.Estat != "incomplet" {
				t.Fatalf("data_naixement impossible mal preservada: %+v", attr)
			}
		}
	}
	if !foundNaixement {
		t.Fatalf("no s'ha creat atribut data_naixement per data impossible: %+v", reg.Atributs)
	}
}

func TestF321MarcmoiaPresetRunsThroughTemplateEngine(t *testing.T) {
	app, database := newModeracioBulkDiagnosticsApp(t)
	user := createModeracioBulkDiagnosticsUser(t, database, "f32_preset_template")
	llibreID := createF32Book(t, database, user.ID, "1890-1891", false)
	modelJSON := `{
  "version": 1,
  "kind": "transcripcions_raw",
  "preset_code": "baptismes_marcmoia_v2",
  "book_resolution": { "mode": "llibre_id", "column": "llibre_id" },
  "mapping": {
    "columns": [
      { "header": "llibre_id", "key": "llibre_id", "required": true, "map_to": [{ "target": "base.llibre_id" }] },
      { "header": "tipus_acte", "key": "tipus_acte", "required": true, "map_to": [{ "target": "base.tipus_acte" }] },
      { "header": "batejat", "key": "batejat", "map_to": [{ "target": "person.batejat", "transform": [{ "op": "parse_person_from_nom" }] }] }
    ]
  }
}`
	template := &db.CSVImportTemplate{Name: "F32 preset probe", ModelJSON: modelJSON}
	csvContent := buildF32CSV(t, [][]string{
		{"llibre_id", "tipus_acte", "batejat"},
		{intToStringF32(llibreID), "baptisme", "Pere Garcia"},
	})
	result := app.RunCSVTemplateImport(template, strings.NewReader(csvContent), ',', user.ID, importContext{}, 0)
	if result.Created != 1 || result.Failed != 0 {
		t.Fatalf("preset Marcmoia hauria de passar pel mapping de plantilla, resultat=%+v", result)
	}
}

func runF32StaticMarcmoiaImport(t *testing.T, csvContent string) (csvImportResult, f32ImportSnapshot) {
	t.Helper()
	app, database := newModeracioBulkDiagnosticsApp(t)
	user := createModeracioBulkDiagnosticsUser(t, database, "f32_static")
	llibreID := createF32Book(t, database, user.ID, "1890-1891", false)
	result := app.importBaptismesMarcmoiaCSV(strings.NewReader(csvContent), ',', user.ID, importContext{})
	return result, snapshotF32Import(t, database, llibreID)
}

func runF32TemplateMarcmoiaImport(t *testing.T, csvContent string) (csvImportResult, f32ImportSnapshot) {
	t.Helper()
	app, database := newModeracioBulkDiagnosticsApp(t)
	user := createModeracioBulkDiagnosticsUser(t, database, "f32_template")
	llibreID := createF32Book(t, database, user.ID, "1890-1891", false)
	if err := app.EnsureSystemImportTemplates(); err != nil {
		t.Fatalf("EnsureSystemImportTemplates ha fallat: %v", err)
	}
	template, err := app.getSystemImportTemplateByName(systemImportTemplateBaptismesMarcmoiaName)
	if err != nil || template == nil {
		t.Fatalf("plantilla Marcmoia system no trobada: %v", err)
	}
	result := app.RunCSVTemplateImport(template, strings.NewReader(csvContent), ',', user.ID, importContext{}, 0)
	return result, snapshotF32Import(t, database, llibreID)
}

func createF32Book(t *testing.T, database db.DB, userID int, cronologia string, indexed bool) int {
	t.Helper()
	munID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            "Municipi F32",
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	})
	if err != nil {
		t.Fatalf("CreateMunicipi ha fallat: %v", err)
	}
	bisbatID, err := database.CreateArquebisbat(&db.Arquebisbat{
		Nom:            "Bisbat F32",
		TipusEntitat:   "bisbat",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	})
	if err != nil {
		t.Fatalf("CreateArquebisbat ha fallat: %v", err)
	}
	llibreID, err := database.CreateLlibre(&db.Llibre{
		ArquebisbatID:     bisbatID,
		MunicipiID:        munID,
		Titol:             "Llibre F32",
		Cronologia:        cronologia,
		IndexacioCompleta: indexed,
		ModeracioEstat:    "publicat",
		CreatedBy:         sql.NullInt64{Int64: int64(userID), Valid: true},
	})
	if err != nil {
		t.Fatalf("CreateLlibre ha fallat: %v", err)
	}
	return llibreID
}

func snapshotF32Import(t *testing.T, database db.DB, llibreID int) f32ImportSnapshot {
	t.Helper()
	registres, err := database.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{Limit: -1})
	if err != nil {
		t.Fatalf("ListTranscripcionsRaw ha fallat: %v", err)
	}
	sort.Slice(registres, func(i, j int) bool {
		if registres[i].NumPaginaText == registres[j].NumPaginaText {
			return registres[i].ID < registres[j].ID
		}
		return registres[i].NumPaginaText < registres[j].NumPaginaText
	})
	out := f32ImportSnapshot{Registres: make([]f32RegistreSnapshot, 0, len(registres))}
	for _, reg := range registres {
		snap := f32RegistreSnapshot{
			NumPaginaText: reg.NumPaginaText,
			AnyDoc:        reg.AnyDoc.Int64,
			AnyDocValid:   reg.AnyDoc.Valid,
			TipusActe:     reg.TipusActe,
			DataActeText:  reg.DataActeText,
			DataActeISO:   reg.DataActeISO.String,
			DataActeValid: reg.DataActeISO.Valid,
			DataActeEstat: reg.DataActeEstat,
		}
		persones, _ := database.ListTranscripcioPersones(reg.ID)
		for _, p := range persones {
			snap.Persones = append(snap.Persones, f32PersonSnapshot{
				Rol:           p.Rol,
				Nom:           p.Nom,
				Cognom1:       p.Cognom1,
				Cognom2:       p.Cognom2,
				NomEstat:      p.NomEstat,
				Cognom1Estat:  p.Cognom1Estat,
				Cognom2Estat:  p.Cognom2Estat,
				MunicipiText:  p.MunicipiText,
				MunicipiEstat: p.MunicipiEstat,
				OficiText:     p.OficiText,
				OficiEstat:    p.OficiEstat,
				Notes:         p.Notes,
			})
		}
		sort.Slice(snap.Persones, func(i, j int) bool {
			if snap.Persones[i].Rol == snap.Persones[j].Rol {
				return snap.Persones[i].Nom < snap.Persones[j].Nom
			}
			return snap.Persones[i].Rol < snap.Persones[j].Rol
		})
		attrs, _ := database.ListTranscripcioAtributs(reg.ID)
		for _, attr := range attrs {
			snap.Atributs = append(snap.Atributs, f32AttrSnapshot{
				Clau:       attr.Clau,
				TipusValor: attr.TipusValor,
				ValorText:  attr.ValorText,
				ValorDate:  attr.ValorDate.String,
				ValorInt:   attr.ValorInt.Int64,
				IntValid:   attr.ValorInt.Valid,
				Estat:      attr.Estat,
			})
		}
		sort.Slice(snap.Atributs, func(i, j int) bool {
			return snap.Atributs[i].Clau < snap.Atributs[j].Clau
		})
		out.Registres = append(out.Registres, snap)
	}
	return out
}

func buildF32CSV(t *testing.T, rows [][]string) string {
	t.Helper()
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			t.Fatalf("csv write ha fallat: %v", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatalf("csv flush ha fallat: %v", err)
	}
	return buf.String()
}

func intToStringF32(v int) string {
	return strconv.Itoa(v)
}
