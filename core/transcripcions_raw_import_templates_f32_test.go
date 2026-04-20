package core

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
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

func TestF322MarcmoiaDoesNotMergeExistingWhenPageNotIndexed(t *testing.T) {
	app, database, userID, llibreID, template := setupF322MarcmoiaTemplate(t, "pendent")
	createF322ExistingBaptisme(t, database, llibreID, 12, "Joan", "Garcia", "Soler", "Pere", "Garcia", "Puig", "1890-02-05", "1890-02-01")

	result := app.RunCSVTemplateImport(template, strings.NewReader(buildF322MarcmoiaCSV(t, [][]string{
		{"12", "12", "Garcia Soler Joan", "Pere Garcia", "Maria Puig", "01/02/1890", "05/02/1890"},
	})), ',', userID, importContext{}, 0)
	if result.Created != 1 || result.Updated != 0 || result.Failed != 0 {
		t.Fatalf("pàgina no indexada no hauria de fusionar ni bloquejar: %+v", result)
	}
	if got := countF32Registres(t, database, llibreID); got != 2 {
		t.Fatalf("esperava 2 registres després d'importar en pàgina no indexada, got=%d", got)
	}
}

func TestF322MarcmoiaMergesStrongDuplicateOnIndexedPage(t *testing.T) {
	app, database, userID, llibreID, template := setupF322MarcmoiaTemplate(t, "indexada")
	createF322ExistingBaptisme(t, database, llibreID, 12, "Joan", "Garcia", "Soler", "Pere", "Garcia", "Puig", "1890-02-05", "1890-02-01")
	incoming, incomingPeople, incomingAttrs := buildF322IncomingStrongRow(llibreID, 12)
	if pageKey, indexed := app.templateIndexedPageKey(llibreID, incoming, incomingAttrs); !indexed {
		t.Fatalf("la pàgina 12 hauria de constar indexada, pageKey=%q", pageKey)
	}
	matchKey := buildTemplateStrongMatchKey(incoming, incomingPeople, incomingAttrs, templatePolicies{PrincipalRoles: []string{"batejat", "persona_principal"}})
	existingMap := app.loadExistingByStrongMatch(llibreID, incoming, incomingAttrs, templatePolicies{PrincipalRoles: []string{"batejat", "persona_principal"}})
	if matchKey == "" || existingMap[matchKey] == 0 {
		t.Fatalf("el setup de duplicat fort no és coherent: matchKey=%q existingMap=%+v", matchKey, existingMap)
	}

	result := app.RunCSVTemplateImport(template, strings.NewReader(buildF322MarcmoiaCSV(t, [][]string{
		{"12", "12", "Garcia Soler Joan", "Pere Garcia", "Maria Puig", "01/02/1890", "05/02/1890"},
	})), ',', userID, importContext{}, 0)
	if result.Created != 0 || result.Updated != 1 || result.Failed != 0 {
		t.Fatalf("duplicat fort en pàgina indexada s'hauria de fusionar amb existent: %+v", result)
	}
	if got := countF32Registres(t, database, llibreID); got != 1 {
		t.Fatalf("esperava 1 registre després de fusionar duplicat fort, got=%d", got)
	}
}

func TestF322MarcmoiaDoesNotDedupSameSurnamesOrWeakSameName(t *testing.T) {
	app, database, userID, llibreID, template := setupF322MarcmoiaTemplate(t, "indexada")
	createF322ExistingBaptisme(t, database, llibreID, 12, "Joan", "Garcia", "Soler", "", "", "", "", "")

	result := app.RunCSVTemplateImport(template, strings.NewReader(buildF322MarcmoiaCSV(t, [][]string{
		{"12", "12", "Garcia Soler Pere", "", "", "", ""},
		{"12", "12", "Garcia Soler Joan", "", "", "", ""},
	})), ',', userID, importContext{}, 0)
	if result.Created != 2 || result.Updated != 0 || result.Failed != 0 {
		t.Fatalf("cognoms iguals o nom+cognoms sense senyals forts no han de duplicar: %+v", result)
	}
	if got := countF32Registres(t, database, llibreID); got != 3 {
		t.Fatalf("esperava conservar els registres legítims amb dades parcials, got=%d", got)
	}
}

func TestF322MarcmoiaKeepsSiblingsTwinsWithSharedContext(t *testing.T) {
	app, database, userID, llibreID, template := setupF322MarcmoiaTemplate(t, "indexada")

	result := app.RunCSVTemplateImport(template, strings.NewReader(buildF322MarcmoiaCSV(t, [][]string{
		{"12", "12", "Garcia Soler Joan", "Pere Garcia", "Maria Soler", "01/02/1890", "05/02/1890"},
		{"12", "12", "Garcia Soler Josep", "Pere Garcia", "Maria Soler", "01/02/1890", "05/02/1890"},
	})), ',', userID, importContext{}, 0)
	if result.Created != 2 || result.Updated != 0 || result.Failed != 0 {
		t.Fatalf("germans/bessons amb mateix context però nom diferent no han de col·lapsar: %+v", result)
	}
	if got := countF32Registres(t, database, llibreID); got != 2 {
		t.Fatalf("esperava 2 registres de germans/bessons, got=%d", got)
	}
}

func TestF322MarcmoiaWithinFileDoesNotDedupWhenPrincipalNameMissing(t *testing.T) {
	app, database, userID, llibreID, template := setupF322MarcmoiaTemplate(t, "pendent")

	result := app.RunCSVTemplateImport(template, strings.NewReader(buildF322MarcmoiaCSV(t, [][]string{
		{"12", "12", "Garcia Soler", "", "", "", ""},
		{"12", "12", "Garcia Soler", "", "", "", ""},
	})), ',', userID, importContext{}, 0)
	if result.Created != 2 || result.Updated != 0 || result.Failed != 0 {
		t.Fatalf("files amb només cognoms no han de deduplicar-se dins el fitxer: %+v", result)
	}
	if got := countF32Registres(t, database, llibreID); got != 2 {
		t.Fatalf("esperava 2 registres amb només cognoms, got=%d", got)
	}
}

func TestF322MarcmoiaWithinFileStillDedupsExactRowsWithPrincipalName(t *testing.T) {
	app, database, userID, llibreID, template := setupF322MarcmoiaTemplate(t, "pendent")

	result := app.RunCSVTemplateImport(template, strings.NewReader(buildF322MarcmoiaCSV(t, [][]string{
		{"12", "12", "Garcia Soler Joan", "Pere Garcia", "Maria Soler", "01/02/1890", "05/02/1890"},
		{"12", "12", "Garcia Soler Joan", "Pere Garcia", "Maria Soler", "01/02/1890", "05/02/1890"},
	})), ',', userID, importContext{}, 0)
	if result.Created != 1 || result.Updated != 0 || result.Failed != 1 {
		t.Fatalf("files idèntiques amb nom principal present han de continuar deduplicant-se: %+v", result)
	}
	if got := countF32Registres(t, database, llibreID); got != 1 {
		t.Fatalf("esperava 1 registre després de dedup exacte, got=%d", got)
	}
}

type f323NoBulkDB struct {
	db.DB
}

func TestF323TemplateImportWriteMetricsAndBulkEquivalent(t *testing.T) {
	SetLogLevel("debug")
	defer SetLogLevel("error")

	rows := make([][]string, 0, 220)
	for i := 0; i < 220; i++ {
		rows = append(rows, []string{
			"12",
			"12",
			fmt.Sprintf("Garcia%d Soler%d Joan%d", i, i, i),
			fmt.Sprintf("Pere%d Garcia", i),
			fmt.Sprintf("Maria%d Soler", i),
			fmt.Sprintf("%02d/01/1890", 1+(i%28)),
			fmt.Sprintf("%02d/02/1890", 1+(i%28)),
		})
	}
	csvContent := buildF322MarcmoiaCSV(t, rows)

	fallbackApp, fallbackDB, fallbackUserID, fallbackLlibreID, fallbackTemplate := setupF322MarcmoiaTemplate(t, "indexada")
	fallbackApp.DB = f323NoBulkDB{DB: fallbackDB}
	fallbackResult := fallbackApp.RunCSVTemplateImport(fallbackTemplate, strings.NewReader(csvContent), ',', fallbackUserID, importContext{}, 0)
	if fallbackResult.Created != len(rows) || fallbackResult.Failed != 0 {
		t.Fatalf("fallback import inesperat: created=%d failed=%d errors=%+v", fallbackResult.Created, fallbackResult.Failed, fallbackResult.Errors)
	}
	if fallbackResult.Debug.WriteBulkBatches != 0 || fallbackResult.Debug.WriteBulkRows != 0 {
		t.Fatalf("fallback no hauria d'usar bulk: %+v", fallbackResult.Debug)
	}

	bulkApp, bulkDB, bulkUserID, bulkLlibreID, bulkTemplate := setupF322MarcmoiaTemplate(t, "indexada")
	bulkResult := bulkApp.RunCSVTemplateImport(bulkTemplate, strings.NewReader(csvContent), ',', bulkUserID, importContext{}, 0)
	if bulkResult.Created != len(rows) || bulkResult.Failed != 0 {
		t.Fatalf("bulk import inesperat: created=%d failed=%d errors=%+v", bulkResult.Created, bulkResult.Failed, bulkResult.Errors)
	}
	if bulkResult.Debug.WriteBulkBatches == 0 || bulkResult.Debug.WriteBulkRows != len(rows) || bulkResult.Debug.WriteBulkFallbacks != 0 {
		t.Fatalf("bulk import no ha usat el camí esperat: %+v", bulkResult.Debug)
	}
	if bulkResult.Debug.WriteTranscripcioInsertDur <= 0 || bulkResult.Debug.WritePersonaPersistDur <= 0 || bulkResult.Debug.WriteLinksPersistDur <= 0 || bulkResult.Debug.WriteCommitDur <= 0 {
		t.Fatalf("mètriques write F32-3 incompletes: %+v", bulkResult.Debug)
	}
	if !reflect.DeepEqual(snapshotF32Import(t, fallbackDB, fallbackLlibreID), snapshotF32Import(t, bulkDB, bulkLlibreID)) {
		t.Fatalf("bulk i fallback no produeixen el mateix snapshot funcional")
	}
	t.Logf("fallback write=%s insert=%s persona=%s links=%s commit=%s batches=%d; bulk write=%s insert=%s persona=%s links=%s commit=%s batches=%d",
		fallbackResult.Debug.WriteDur,
		fallbackResult.Debug.WriteTranscripcioInsertDur,
		fallbackResult.Debug.WritePersonaPersistDur,
		fallbackResult.Debug.WriteLinksPersistDur,
		fallbackResult.Debug.WriteCommitDur,
		fallbackResult.Debug.WriteBulkBatches,
		bulkResult.Debug.WriteDur,
		bulkResult.Debug.WriteTranscripcioInsertDur,
		bulkResult.Debug.WritePersonaPersistDur,
		bulkResult.Debug.WriteLinksPersistDur,
		bulkResult.Debug.WriteCommitDur,
		bulkResult.Debug.WriteBulkBatches,
	)
}

func TestF323MarcmoiaLargeImportCreatedFailedCount(t *testing.T) {
	if os.Getenv("CG_F323_LARGE_IMPORT") != "1" {
		t.Skip("validació gran F32-3 només s'executa explícitament amb CG_F323_LARGE_IMPORT=1")
	}
	SetLogLevel("debug")
	defer SetLogLevel("error")

	const totalRows = 19578
	rows := make([][]string, 0, totalRows)
	for i := 0; i < totalRows; i++ {
		rows = append(rows, []string{
			"12",
			"12",
			fmt.Sprintf("Garcia%d Soler%d Joan%d", i, i, i),
			fmt.Sprintf("Pere%d Garcia", i),
			fmt.Sprintf("Maria%d Soler", i),
			fmt.Sprintf("%02d/01/%04d", 1+(i%28), 1890+(i%20)),
			fmt.Sprintf("%02d/02/%04d", 1+(i%28), 1890+(i%20)),
		})
	}
	app, _, userID, _, template := setupF322MarcmoiaTemplate(t, "pendent")
	result := app.RunCSVTemplateImport(template, strings.NewReader(buildF322MarcmoiaCSV(t, rows)), ',', userID, importContext{}, 0)
	if result.Created != totalRows || result.Updated != 0 || result.Failed != 0 {
		t.Fatalf("validació gran F32-3 inesperada: created=%d updated=%d failed=%d errors=%+v", result.Created, result.Updated, result.Failed, result.Errors)
	}
	t.Logf("large import rows=%d created=%d updated=%d failed=%d write=%s insert=%s persona=%s links=%s commit=%s batches=%d fallbacks=%d",
		result.Debug.Rows,
		result.Created,
		result.Updated,
		result.Failed,
		result.Debug.WriteDur,
		result.Debug.WriteTranscripcioInsertDur,
		result.Debug.WritePersonaPersistDur,
		result.Debug.WriteLinksPersistDur,
		result.Debug.WriteCommitDur,
		result.Debug.WriteBulkBatches,
		result.Debug.WriteBulkFallbacks,
	)
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

func setupF322MarcmoiaTemplate(t *testing.T, pageStatus string) (*App, db.DB, int, int, *db.CSVImportTemplate) {
	t.Helper()
	app, database := newModeracioBulkDiagnosticsApp(t)
	user := createModeracioBulkDiagnosticsUser(t, database, "f322_"+pageStatus)
	llibreID := createF32Book(t, database, user.ID, "1890-1891", true)
	_, err := database.SaveLlibrePagina(&db.LlibrePagina{
		LlibreID:  llibreID,
		NumPagina: 12,
		Estat:     pageStatus,
	})
	if err != nil {
		t.Fatalf("SaveLlibrePagina ha fallat: %v", err)
	}
	if err := app.EnsureSystemImportTemplates(); err != nil {
		t.Fatalf("EnsureSystemImportTemplates ha fallat: %v", err)
	}
	template, err := app.getSystemImportTemplateByName(systemImportTemplateBaptismesMarcmoiaName)
	if err != nil || template == nil {
		t.Fatalf("plantilla Marcmoia system no trobada: %v", err)
	}
	return app, database, user.ID, llibreID, template
}

func createF322ExistingBaptisme(t *testing.T, database db.DB, llibreID, page int, nom, cognom1, cognom2, pareNom, pareCognom, mareCognom, dataBateigISO, dataNaixementISO string) int {
	t.Helper()
	reg := &db.TranscripcioRaw{
		LlibreID:       llibreID,
		NumPaginaText:  strconv.Itoa(page),
		TipusActe:      "baptisme",
		DataActeEstat:  "clar",
		ModeracioEstat: "pendent",
	}
	if dataBateigISO != "" {
		reg.DataActeISO = parseNullString(dataBateigISO)
		reg.DataActeText = strings.ReplaceAll(dataBateigISO, "-", "/")
	}
	regID, err := database.CreateTranscripcioRaw(reg)
	if err != nil {
		t.Fatalf("CreateTranscripcioRaw ha fallat: %v", err)
	}
	_, _ = database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: regID,
		Rol:            "batejat",
		Nom:            nom,
		Cognom1:        cognom1,
		Cognom2:        cognom2,
	})
	if pareNom != "" || pareCognom != "" {
		_, _ = database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
			TranscripcioID: regID,
			Rol:            "pare",
			Nom:            pareNom,
			Cognom1:        pareCognom,
		})
	}
	if mareCognom != "" {
		_, _ = database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
			TranscripcioID: regID,
			Rol:            "mare",
			Nom:            "Maria",
			Cognom1:        mareCognom,
		})
	}
	_, _ = database.CreateTranscripcioAtribut(&db.TranscripcioAtributRaw{
		TranscripcioID: regID,
		Clau:           "pagina_digital",
		TipusValor:     "text",
		ValorText:      strconv.Itoa(page),
		Estat:          "clar",
	})
	if dataBateigISO != "" {
		_, _ = database.CreateTranscripcioAtribut(&db.TranscripcioAtributRaw{
			TranscripcioID: regID,
			Clau:           "data_bateig",
			TipusValor:     "date",
			ValorDate:      parseNullString(dataBateigISO),
			Estat:          "clar",
		})
	}
	if dataNaixementISO != "" {
		_, _ = database.CreateTranscripcioAtribut(&db.TranscripcioAtributRaw{
			TranscripcioID: regID,
			Clau:           "data_naixement",
			TipusValor:     "date",
			ValorDate:      parseNullString(dataNaixementISO),
			Estat:          "clar",
		})
	}
	return regID
}

func buildF322IncomingStrongRow(llibreID, page int) (*db.TranscripcioRaw, map[string]*db.TranscripcioPersonaRaw, map[string]*db.TranscripcioAtributRaw) {
	reg := &db.TranscripcioRaw{
		LlibreID:       llibreID,
		NumPaginaText:  strconv.Itoa(page),
		TipusActe:      "baptisme",
		DataActeISO:    parseNullString("1890-02-05"),
		DataActeText:   "05/02/1890",
		DataActeEstat:  "clar",
		ModeracioEstat: "pendent",
	}
	people := map[string]*db.TranscripcioPersonaRaw{
		"batejat": &db.TranscripcioPersonaRaw{Rol: "batejat", Nom: "Joan", Cognom1: "Garcia", Cognom2: "Soler"},
		"pare":    &db.TranscripcioPersonaRaw{Rol: "pare", Nom: "Pere", Cognom1: "Garcia"},
		"mare":    &db.TranscripcioPersonaRaw{Rol: "mare", Nom: "Maria", Cognom1: "Puig"},
	}
	attrs := map[string]*db.TranscripcioAtributRaw{
		"pagina_digital": &db.TranscripcioAtributRaw{Clau: "pagina_digital", TipusValor: "text", ValorText: strconv.Itoa(page), Estat: "clar"},
		"data_bateig":    &db.TranscripcioAtributRaw{Clau: "data_bateig", TipusValor: "date", ValorDate: parseNullString("1890-02-05"), Estat: "clar"},
		"data_naixement": &db.TranscripcioAtributRaw{Clau: "data_naixement", TipusValor: "date", ValorDate: parseNullString("1890-02-01"), Estat: "clar"},
	}
	return reg, people, attrs
}

func buildF322MarcmoiaCSV(t *testing.T, rows [][]string) string {
	t.Helper()
	all := [][]string{{"llibre", "paginallibre", "paginareal", "any", "cognoms", "pare", "mare", "nascut", "bateig"}}
	for _, row := range rows {
		all = append(all, []string{"1890-1891", row[0], row[1], "1890", row[2], row[3], row[4], row[5], row[6]})
	}
	return buildF32CSV(t, all)
}

func countF32Registres(t *testing.T, database db.DB, llibreID int) int {
	t.Helper()
	registres, err := database.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{Limit: -1})
	if err != nil {
		t.Fatalf("ListTranscripcionsRaw ha fallat: %v", err)
	}
	return len(registres)
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
