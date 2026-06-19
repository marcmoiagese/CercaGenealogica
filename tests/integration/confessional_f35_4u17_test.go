package integration

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func f354U17ImportSession(t *testing.T, dbFile, slug string) (*core.App, db.DB, *http.Cookie, *http.Cookie, string) {
	t.Helper()
	app, database := newTestAppForLogin(t, dbFile)
	user := createTestUser(t, database, slug+"_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_"+slug+"_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, slug, "territori.confessional.import_export.import")
	addGrantToPolicy(t, database, policyID, "territori.confessional.import_export.view")
	assignPolicyToUser(t, database, user.ID, policyID)
	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, app, session)
	return app, database, session, csrfCookie, csrfToken
}

func f354U17Payload(t *testing.T, items map[string]interface{}) []byte {
	t.Helper()
	return confessionalExportPayloadForTest(t, items)
}

func f354U17DryRunBody(t *testing.T, app *core.App, database db.DB, session, csrfCookie *http.Cookie, csrfToken string, payload []byte) string {
	t.Helper()
	return f354U1DryRun(t, app, database, session, csrfCookie, csrfToken, payload)
}

func f354U17ArchiveRelationPayload(entityCode, archiveName, relationType string, municipality map[string]interface{}) []map[string]interface{} {
	archive := map[string]interface{}{
		"name": archiveName,
		"type": "arxiu",
	}
	if municipality != nil {
		archive["municipality"] = municipality
	}
	return []map[string]interface{}{
		{
			"entity":        confEntityRef(entityCode, "catolicisme_ritu_llati", "parroquia"),
			"archive":       archive,
			"relation_type": relationType,
		},
	}
}

func f354U17TerritoryRelationPayload(entityCode string, municipality map[string]interface{}, nucleus map[string]interface{}) []map[string]interface{} {
	record := map[string]interface{}{
		"entity":        confEntityRef(entityCode, "catolicisme_ritu_llati", "parroquia"),
		"municipality":  municipality,
		"relation_type": "parroquia_local",
	}
	if nucleus != nil {
		record["nucleus"] = nucleus
	}
	return []map[string]interface{}{record}
}

func f354U17BaseEntityPayload(code, name string) []map[string]interface{} {
	return []map[string]interface{}{
		confEntityExportRecord(code, name, "catolicisme_ritu_llati", "parroquia"),
	}
}

func f354U17CreateCountryLevel(t *testing.T, database db.DB, iso2, iso3, num string) int {
	t.Helper()
	paisID, err := database.CreatePais(&db.Pais{CodiISO2: iso2, CodiISO3: iso3, CodiPaisNum: num})
	if err != nil {
		t.Fatalf("CreatePais(%s): %v", iso2, err)
	}
	levelID, err := database.CreateNivell(&db.NivellAdministratiu{
		PaisID:         paisID,
		Nivel:          1,
		NomNivell:      "Pais " + iso2,
		TipusNivell:    "pais",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateNivell(%s): %v", iso2, err)
	}
	return levelID
}

type f354U17AdminLevelSpec struct {
	PaisID   int
	Nivel    int
	Nom      string
	Tipus    string
	Codi     string
	ParentID int
}

type f354U17HomonymousMunicipalityContext struct {
	PaisLevelID      int
	ProvinciaLevelID int
	ComarcaLevelID   int
	LevelIDs         [7]int
	Name             string
	Comarca          string
	Provincia        string
	IdescatCodi      string
	MunicipiIdescat  string
}

func f354U17CreateAdminLevel(t *testing.T, database db.DB, spec f354U17AdminLevelSpec) int {
	t.Helper()
	level := &db.NivellAdministratiu{
		PaisID:         spec.PaisID,
		Nivel:          spec.Nivel,
		NomNivell:      spec.Nom,
		TipusNivell:    spec.Tipus,
		CodiOficial:    spec.Codi,
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	}
	if spec.ParentID > 0 {
		level.ParentID = sql.NullInt64{Int64: int64(spec.ParentID), Valid: true}
	}
	id, err := database.CreateNivell(level)
	if err != nil {
		t.Fatalf("CreateNivell(%s): %v", spec.Nom, err)
	}
	return id
}

func f354U17CreateMunicipalityWithAdminContext(t *testing.T, database db.DB, name, tipus string, levelIDs [7]int, altres string) int {
	t.Helper()
	var nivellRefs [7]sql.NullInt64
	for i, id := range levelIDs {
		if id > 0 {
			nivellRefs[i] = sql.NullInt64{Int64: int64(id), Valid: true}
		}
	}
	id, err := database.CreateMunicipi(&db.Municipi{
		Nom:                   name,
		Tipus:                 tipus,
		NivellAdministratiuID: nivellRefs,
		Altres:                altres,
		Estat:                 "actiu",
		ModeracioEstat:        "publicat",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi contextual municipality (%s): %v", name, err)
	}
	return id
}

func f354U17CreateHomonymousMunicipalityContext(t *testing.T, database db.DB, paisID, paisLevelID int, suffix, label, provinciaCode, comarcaCode, idescatCodi, municipiIdescat string) f354U17HomonymousMunicipalityContext {
	t.Helper()
	provinciaName := label + " provincia F35-4U17 " + suffix
	provinciaLevelID := f354U17CreateAdminLevel(t, database, f354U17AdminLevelSpec{
		PaisID: paisID, Nivel: 3, Nom: provinciaName, Tipus: "provincia", Codi: provinciaCode, ParentID: paisLevelID,
	})
	comarcaName := label + " comarca F35-4U17 " + suffix
	comarcaLevelID := f354U17CreateAdminLevel(t, database, f354U17AdminLevelSpec{
		PaisID: paisID, Nivel: 4, Nom: comarcaName, Tipus: "comarca", Codi: comarcaCode, ParentID: provinciaLevelID,
	})
	levelIDs := [7]int{}
	levelIDs[0] = paisLevelID
	levelIDs[2] = provinciaLevelID
	levelIDs[3] = comarcaLevelID
	return f354U17HomonymousMunicipalityContext{
		PaisLevelID:      paisLevelID,
		ProvinciaLevelID: provinciaLevelID,
		ComarcaLevelID:   comarcaLevelID,
		LevelIDs:         levelIDs,
		Name:             "Cambrils F35-4U17 " + suffix,
		Comarca:          comarcaName,
		Provincia:        provinciaName,
		IdescatCodi:      idescatCodi,
		MunicipiIdescat:  municipiIdescat,
	}
}

func f354U17CreateContextualCambrils(t *testing.T, database db.DB, ctx f354U17HomonymousMunicipalityContext) int {
	t.Helper()
	altres := "idescat_codi=" + ctx.IdescatCodi + "; municipi_idescat=" + ctx.MunicipiIdescat + "; comarca=" + ctx.Comarca + "; provincia=" + ctx.Provincia
	return f354U17CreateMunicipalityWithAdminContext(t, database, ctx.Name, "poble", ctx.LevelIDs, altres)
}

func f354U17MunicipalityRefWithIdescatContext(ctx f354U17HomonymousMunicipalityContext) map[string]interface{} {
	return map[string]interface{}{
		"name":                    ctx.Name,
		"type":                    "poble",
		"country_iso2":            "ES",
		"municipi_idescat":        ctx.MunicipiIdescat,
		"idescat_codi":            ctx.IdescatCodi,
		"comarca":                 ctx.Comarca,
		"provincia":               ctx.Provincia,
		"municipality_admin_name": ctx.Name,
		"municipality_admin_code": ctx.MunicipiIdescat,
		"nivells": []int{
			ctx.PaisLevelID, 0, ctx.ProvinciaLevelID, ctx.ComarcaLevelID, 0, 0, 0,
		},
	}
}

func f354U17AssertTokens(t *testing.T, body string, tokens []string, message string) {
	t.Helper()
	for _, token := range tokens {
		if !strings.Contains(body, token) {
			t.Fatalf("%s (%q); body=%s", message, token, body)
		}
	}
}

func f354U17CreateStructuralMunicipality(t *testing.T, database db.DB, name, tipus string) int {
	t.Helper()
	id, err := database.CreateMunicipi(&db.Municipi{
		Nom:            name,
		Tipus:          tipus,
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi structural municipality (%s, %s): %v", name, tipus, err)
	}
	return id
}

func f354U17CreateStructuralNucleus(t *testing.T, database db.DB, parentID int, name, tipus string) int {
	t.Helper()
	id, err := database.CreateMunicipi(&db.Municipi{
		Nom:            name,
		Tipus:          tipus,
		MunicipiID:     sql.NullInt64{Int64: int64(parentID), Valid: true},
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi structural nucleus (%s, %s, parent=%d): %v", name, tipus, parentID, err)
	}
	return id
}

func f354U17AssertNoReferenceErrors(t *testing.T, body, message string) {
	t.Helper()
	if strings.Contains(body, "Referència no resolta") || strings.Contains(body, "Referència ambigua") {
		t.Fatalf("%s; body=%s", message, body)
	}
}

func f354U17AssertDryRunOffersApply(t *testing.T, body, message string) {
	t.Helper()
	if !strings.Contains(body, `name="payload_b64"`) {
		t.Fatalf("%s; body=%s", message, body)
	}
}

func f354U17AssertDryRunNoReferenceErrorsAndApply(t *testing.T, body, errorMessage, applyMessage string) {
	t.Helper()
	f354U17AssertNoReferenceErrors(t, body, errorMessage)
	f354U17AssertDryRunOffersApply(t, body, applyMessage)
}

func TestF354U17DryRunShowsContextualUnresolvedMunicipality(t *testing.T) {
	app, _, session, csrfCookie, csrfToken := f354U17ImportSession(t, "test_f35_4u17_unresolved_municipality.sqlite3", "f354u17_unresolved")
	payload := f354U17Payload(t, map[string]interface{}{
		"entitats_religioses":    f354U17BaseEntityPayload("f354u17_missing_mun", "Parroquia F35-4U17 Missing"),
		"relacions_entitats":     []map[string]interface{}{},
		"relacions_territorials": f354U17TerritoryRelationPayload("f354u17_missing_mun", map[string]interface{}{"name": "Municipi inexistent F35-4U17", "type": "municipi", "country_iso2": "ES"}, nil),
		"relacions_arxius":       []map[string]interface{}{},
	})
	req := newMultipartRequest(t, "/admin/confessional/import/dry-run", "import_file", "f35-4u17-unresolved.json", payload, map[string]string{
		"csrf_token": csrfToken,
	})
	req.AddCookie(session)
	req.AddCookie(csrfCookie)
	rr := httptest.NewRecorder()
	app.AdminConfessionalImportDryRun(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("dry-run unresolved municipality status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, token := range []string{
		"seccio=relacions_territorials",
		"tipus=municipi",
		"camp=municipality",
		"valor=Municipi inexistent F35-4U17",
		"origen=Parroquia F35-4U17 Missing (f354u17_missing_mun)",
		"abast=municipis reals existents",
		"accio=",
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("el dry-run contextual ha de contenir %q; body=%s", token, body)
		}
	}
	if strings.Contains(body, `name="payload_b64"`) {
		t.Fatalf("un dry-run amb referencia territorial no resolta no ha d'oferir apply; body=%s", body)
	}
}

func TestF354U17DryRunKeepsSamePayloadHierarchyResolvable(t *testing.T) {
	app, database, session, csrfCookie, csrfToken := f354U17ImportSession(t, "test_f35_4u17_same_payload.sqlite3", "f354u17_same_payload")
	payload := f354U17Payload(t, map[string]interface{}{
		"entitats_religioses": []map[string]interface{}{
			confEntityExportRecord("f354u17_parent", "Bisbat F35-4U17", "catolicisme_ritu_llati", "bisbat_diocesi"),
			confEntityExportRecord("f354u17_child", "Parroquia F35-4U17", "catolicisme_ritu_llati", "parroquia"),
		},
		"relacions_entitats": []map[string]interface{}{
			{
				"parent":        confEntityRef("f354u17_parent", "catolicisme_ritu_llati", "bisbat_diocesi"),
				"child":         confEntityRef("f354u17_child", "catolicisme_ritu_llati", "parroquia"),
				"relation_type": "parroquia",
			},
		},
		"relacions_territorials": []map[string]interface{}{},
		"relacions_arxius":       []map[string]interface{}{},
	})

	body := f354U17DryRunBody(t, app, database, session, csrfCookie, csrfToken, payload)
	if strings.Contains(body, "seccio=relacions_entitats") || strings.Contains(body, "Referència no resolta") {
		t.Fatalf("les relacions entre entitats del mateix payload han de resoldre's sense error; body=%s", body)
	}
	if !strings.Contains(body, `name="payload_b64"`) {
		t.Fatalf("el dry-run valid ha d'oferir apply; body=%s", body)
	}
}

func TestF354U17DryRunResolvesMunicipalityAndNucleusWithParentContext(t *testing.T) {
	app, database, session, csrfCookie, csrfToken := f354U17ImportSession(t, "test_f35_4u17_municipality_nucleus.sqlite3", "f354u17_territory")
	suffix := time.Now().Format("150405000000000")
	municipiID := f353YCreateMunicipi(t, database, "Municipi F35-4U17 "+suffix)
	if _, err := database.CreateMunicipi(&db.Municipi{
		Nom:            "Nucli F35-4U17 " + suffix,
		Tipus:          "nucli_urba",
		MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	}); err != nil {
		t.Fatalf("CreateMunicipi nucli: %v", err)
	}

	payload := f354U17Payload(t, map[string]interface{}{
		"entitats_religioses":    f354U17BaseEntityPayload("f354u17_local", "Parroquia F35-4U17 Territorial"),
		"relacions_entitats":     []map[string]interface{}{},
		"relacions_territorials": f354U17TerritoryRelationPayload("f354u17_local", map[string]interface{}{"name": "Municipi F35-4U17 " + suffix, "type": "municipi"}, map[string]interface{}{"name": "Nucli F35-4U17 " + suffix, "type": "nucli_urba", "parent_names": []string{"Municipi F35-4U17 " + suffix}}),
		"relacions_arxius":       []map[string]interface{}{},
	})
	body := f354U17DryRunBody(t, app, database, session, csrfCookie, csrfToken, payload)
	f354U17AssertDryRunNoReferenceErrorsAndApply(t, body, "municipi i nucli existents amb context de pare s'han de resoldre", "el dry-run valid amb municipi i nucli ha d'oferir apply")
}

func TestF354U17DryRunResolvesStructuralMunicipalityDespiteDescriptiveType(t *testing.T) {
	cases := []struct {
		name         string
		dbFile       string
		slug         string
		municipality string
		tipus        string
		entityCode   string
		entityName   string
		errorMessage string
	}{
		{
			name:         "nucli_urba",
			dbFile:       "test_f35_4u17_structural_municipality_nucli_urba.sqlite3",
			slug:         "f354u17_structural_mun_nucli",
			municipality: "Alio F35-4U17 ",
			tipus:        "nucli_urba",
			entityCode:   "f354u17_structural_mun_nucli",
			entityName:   "Parroquia F35-4U17 Structural Nucli",
			errorMessage: "un municipi structural amb tipus nucli_urba s'ha de resoldre com a municipi",
		},
		{
			name:         "poble",
			dbFile:       "test_f35_4u17_structural_municipality_poble.sqlite3",
			slug:         "f354u17_structural_mun_poble",
			municipality: "Poble F35-4U17 ",
			tipus:        "poble",
			entityCode:   "f354u17_structural_mun_poble",
			entityName:   "Parroquia F35-4U17 Structural Poble",
			errorMessage: "un municipi structural amb tipus poble s'ha de resoldre com a municipi",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			app, database, session, csrfCookie, csrfToken := f354U17ImportSession(t, tc.dbFile, tc.slug)
			suffix := time.Now().Format("150405000000000")
			name := tc.municipality + suffix
			f354U17CreateStructuralMunicipality(t, database, name, tc.tipus)

			payload := f354U17Payload(t, map[string]interface{}{
				"entitats_religioses":    f354U17BaseEntityPayload(tc.entityCode, tc.entityName),
				"relacions_entitats":     []map[string]interface{}{},
				"relacions_territorials": f354U17TerritoryRelationPayload(tc.entityCode, map[string]interface{}{"name": name, "type": tc.tipus}, nil),
				"relacions_arxius":       []map[string]interface{}{},
			})
			body := f354U17DryRunBody(t, app, database, session, csrfCookie, csrfToken, payload)
			f354U17AssertDryRunNoReferenceErrorsAndApply(t, body, tc.errorMessage, "el dry-run valid ha d'oferir apply")
		})
	}
}

func TestF354U17DryRunFlagsAmbiguousNucleus(t *testing.T) {
	app, database, session, csrfCookie, csrfToken := f354U17ImportSession(t, "test_f35_4u17_ambiguous_nucleus.sqlite3", "f354u17_ambiguous")
	suffix := time.Now().Format("150405000000000")
	municipiID := f353YCreateMunicipi(t, database, "Municipi ambigu F35-4U17 "+suffix)
	for i := 0; i < 2; i++ {
		f354U17CreateStructuralNucleus(t, database, municipiID, "Nucli ambigu F35-4U17 "+suffix, "nucli_urba")
	}

	payload := f354U17Payload(t, map[string]interface{}{
		"entitats_religioses":    f354U17BaseEntityPayload("f354u17_ambiguous_local", "Parroquia F35-4U17 Ambigua"),
		"relacions_entitats":     []map[string]interface{}{},
		"relacions_territorials": f354U17TerritoryRelationPayload("f354u17_ambiguous_local", map[string]interface{}{"name": "Municipi ambigu F35-4U17 " + suffix, "type": "municipi"}, map[string]interface{}{"name": "Nucli ambigu F35-4U17 " + suffix, "type": "nucli_urba", "parent_names": []string{"Municipi ambigu F35-4U17 " + suffix}}),
		"relacions_arxius":       []map[string]interface{}{},
	})
	body := f354U17DryRunBody(t, app, database, session, csrfCookie, csrfToken, payload)
	for _, token := range []string{
		"Referència ambigua",
		"tipus=nucli",
		"camp=nucleus",
		"candidats=Nucli ambigu F35-4U17",
		"id=",
		"parent=Municipi ambigu F35-4U17",
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("el nucli ambigu ha d'explicar l'ambiguitat (%q); body=%s", token, body)
		}
	}
}

func TestF354U17DryRunResolvesHomonymousMunicipalityWithIdescatContext(t *testing.T) {
	app, database, session, csrfCookie, csrfToken := f354U17ImportSession(t, "test_f35_4u17_homonymous_idescat.sqlite3", "f354u17_homonymous_idescat")
	suffix := time.Now().Format("150405000000000")
	paisID := createBrowseTestCountry(t, database, "ES")
	paisLevelID := f354U17CreateAdminLevel(t, database, f354U17AdminLevelSpec{
		PaisID: paisID, Nivel: 1, Nom: "Espanya homonima F35-4U17 " + suffix, Tipus: "pais", Codi: "ES",
	})
	tarragonaCtx := f354U17CreateHomonymousMunicipalityContext(t, database, paisID, paisLevelID, suffix, "Tarragona homonima", "43", "03", "430385 03", "430385")
	lleidaCtx := f354U17CreateHomonymousMunicipalityContext(t, database, paisID, paisLevelID, suffix, "Lleida homonima", "25", "01", "251484 01", "251484")
	targetID := f354U17CreateContextualCambrils(t, database, tarragonaCtx)
	f354U17CreateContextualCambrils(t, database, lleidaCtx)

	payload := f354U17Payload(t, map[string]interface{}{
		"entitats_religioses":    f354U17BaseEntityPayload("f354u17_cambrils_idescat", "Parroquia F35-4U17 Cambrils"),
		"relacions_entitats":     []map[string]interface{}{},
		"relacions_territorials": f354U17TerritoryRelationPayload("f354u17_cambrils_idescat", f354U17MunicipalityRefWithIdescatContext(tarragonaCtx), nil),
		"relacions_arxius":       []map[string]interface{}{},
	})
	body := f354U17DryRunBody(t, app, database, session, csrfCookie, csrfToken, payload)
	f354U17AssertDryRunNoReferenceErrorsAndApply(t, body, "el context Idescat ha de resoldre el municipi homonim correcte", "el dry-run valid amb context Idescat ha d'oferir apply")

	applyRR := f354U1Apply(t, app, session, csrfCookie, extractCSRFTokenFromHTML(t, body), extractHiddenTextareaValue(t, body, "payload_b64"))
	if applyRR.Code != http.StatusSeeOther {
		t.Fatalf("apply homonymous municipality status=%d body=%s", applyRR.Code, applyRR.Body.String())
	}
	territoryRows := f354U1ListTerritory(t, database)
	if len(territoryRows) != 1 || territoryRows[0].MunicipiID != targetID {
		t.Fatalf("la relacio territorial ha de resoldre el Cambrils correcte %d, got %+v", targetID, territoryRows)
	}
}

func TestF354U17DryRunKeepsHomonymousMunicipalityAmbiguousWithoutContext(t *testing.T) {
	app, database, session, csrfCookie, csrfToken := f354U17ImportSession(t, "test_f35_4u17_homonymous_ambiguous.sqlite3", "f354u17_homonymous_ambiguous")
	suffix := time.Now().Format("150405000000000")
	paisID := createBrowseTestCountry(t, database, "ES")
	paisLevelID := f354U17CreateAdminLevel(t, database, f354U17AdminLevelSpec{
		PaisID: paisID, Nivel: 1, Nom: "Espanya ambigua F35-4U17 " + suffix, Tipus: "pais", Codi: "ES",
	})
	tarragonaCtx := f354U17CreateHomonymousMunicipalityContext(t, database, paisID, paisLevelID, suffix, "Tarragona ambigua", "43", "03", "430385 03", "430385")
	lleidaCtx := f354U17CreateHomonymousMunicipalityContext(t, database, paisID, paisLevelID, suffix, "Lleida ambigua", "25", "01", "251484 01", "251484")
	f354U17CreateContextualCambrils(t, database, tarragonaCtx)
	f354U17CreateContextualCambrils(t, database, lleidaCtx)

	payload := f354U17Payload(t, map[string]interface{}{
		"entitats_religioses":    f354U17BaseEntityPayload("f354u17_cambrils_ambiguous", "Parroquia F35-4U17 Cambrils Ambigua"),
		"relacions_entitats":     []map[string]interface{}{},
		"relacions_territorials": f354U17TerritoryRelationPayload("f354u17_cambrils_ambiguous", map[string]interface{}{"name": "Cambrils F35-4U17 " + suffix, "type": "poble", "country_iso2": "ES"}, nil),
		"relacions_arxius":       []map[string]interface{}{},
	})
	body := f354U17DryRunBody(t, app, database, session, csrfCookie, csrfToken, payload)
	f354U17AssertTokens(t, body, []string{
		"Referència ambigua",
		"valor=Cambrils F35-4U17 " + suffix,
		"municipi_idescat=430385",
		"municipi_idescat=251484",
		"comarca=" + tarragonaCtx.Comarca,
		"comarca=" + lleidaCtx.Comarca,
		"provincia=" + tarragonaCtx.Provincia,
		"provincia=" + lleidaCtx.Provincia,
	}, "el dry-run sense context ha de continuar mostrant ambiguitat contextual")
	if strings.Contains(body, `name="payload_b64"`) {
		t.Fatalf("un municipi homonim sense context no ha d'oferir apply; body=%s", body)
	}
}

func TestF354U17ExportIncludesTerritoryContext(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u17_export_context.sqlite3")
	user := createTestUser(t, database, "f354u17_export_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f354u17_export_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f354u17_export", "territori.confessional.import_export.export")
	assignPolicyToUser(t, database, user.ID, policyID)

	suffix := time.Now().Format("150405000000000")
	paisID := createBrowseTestCountry(t, database, "ES")
	paisLevelID := f354U17CreateAdminLevel(t, database, f354U17AdminLevelSpec{
		PaisID: paisID, Nivel: 1, Nom: "Espanya export F35-4U17 " + suffix, Tipus: "pais", Codi: "ES",
	})
	provinciaLevelID := f354U17CreateAdminLevel(t, database, f354U17AdminLevelSpec{
		PaisID: paisID, Nivel: 3, Nom: "Tarragona export F35-4U17 " + suffix, Tipus: "provincia", Codi: "43", ParentID: paisLevelID,
	})
	comarcaLevelID := f354U17CreateAdminLevel(t, database, f354U17AdminLevelSpec{
		PaisID: paisID, Nivel: 4, Nom: "Baix Camp export F35-4U17 " + suffix, Tipus: "comarca", Codi: "03", ParentID: provinciaLevelID,
	})
	levelIDs := [7]int{}
	levelIDs[0] = paisLevelID
	levelIDs[2] = provinciaLevelID
	levelIDs[3] = comarcaLevelID
	municipiID := f354U17CreateMunicipalityWithAdminContext(
		t,
		database,
		"Municipi export F35-4U17 "+suffix,
		"poble",
		levelIDs,
		"idescat_codi=430385 03; municipi_idescat=430385; comarca=Baix Camp export F35-4U17 "+suffix+"; provincia=Tarragona export F35-4U17 "+suffix,
	)
	nucliLevels := [7]sql.NullInt64{}
	nucliLevels[0] = sql.NullInt64{Int64: int64(paisLevelID), Valid: true}
	nucliLevels[2] = sql.NullInt64{Int64: int64(provinciaLevelID), Valid: true}
	nucliLevels[3] = sql.NullInt64{Int64: int64(comarcaLevelID), Valid: true}
	nucliID, err := database.CreateMunicipi(&db.Municipi{
		Nom:                   "Nucli export F35-4U17 " + suffix,
		Tipus:                 "nucli_urba",
		MunicipiID:            sql.NullInt64{Int64: int64(municipiID), Valid: true},
		NivellAdministratiuID: nucliLevels,
		Altres:                "idescat_codi=430385 03; municipi_idescat=430385; comarca=Baix Camp export F35-4U17 " + suffix + "; provincia=Tarragona export F35-4U17 " + suffix,
		Estat:                 "actiu",
		ModeracioEstat:        "publicat",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi export nucli: %v", err)
	}
	entityID := f354SSaveEntitat(t, database, "f354u17_export_"+suffix, "Parroquia export F35-4U17 "+suffix, "catolicisme_ritu_llati", "parroquia", "publicat")
	if _, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiID,
		NucliID:            sql.NullInt64{Int64: int64(nucliID), Valid: true},
		EntitatReligiosaID: entityID,
		TipusRelacio:       "parroquia_local",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa export: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/confessional/export", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminConfessionalExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("export context status=%d body=%s", rr.Code, rr.Body.String())
	}

	var payload struct {
		Items struct {
			RelacionsTerritorials []struct {
				Municipality struct {
					Name                  string   `json:"name"`
					Type                  string   `json:"type"`
					CountryISO2           string   `json:"country_iso2"`
					ParentNames           []string `json:"parent_names"`
					Nivells               []int    `json:"nivells"`
					AdminPath             []string `json:"admin_path"`
					IdescatCodi           string   `json:"idescat_codi"`
					MunicipiIdescat       string   `json:"municipi_idescat"`
					Comarca               string   `json:"comarca"`
					Provincia             string   `json:"provincia"`
					MunicipalityAdminName string   `json:"municipality_admin_name"`
					MunicipalityAdminCode string   `json:"municipality_admin_code"`
				} `json:"municipality"`
				Nucleus *struct {
					Name                  string   `json:"name"`
					Type                  string   `json:"type"`
					CountryISO2           string   `json:"country_iso2"`
					ParentNames           []string `json:"parent_names"`
					Nivells               []int    `json:"nivells"`
					AdminPath             []string `json:"admin_path"`
					IdescatCodi           string   `json:"idescat_codi"`
					MunicipiIdescat       string   `json:"municipi_idescat"`
					Comarca               string   `json:"comarca"`
					Provincia             string   `json:"provincia"`
					MunicipalityAdminName string   `json:"municipality_admin_name"`
					MunicipalityAdminCode string   `json:"municipality_admin_code"`
				} `json:"nucleus"`
			} `json:"relacions_territorials"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal export context: %v", err)
	}
	if len(payload.Items.RelacionsTerritorials) != 1 {
		t.Fatalf("s'esperava 1 relacio territorial exportada, got %d", len(payload.Items.RelacionsTerritorials))
	}
	territory := payload.Items.RelacionsTerritorials[0]
	if territory.Municipality.Name != "Municipi export F35-4U17 "+suffix || territory.Municipality.Type != "poble" {
		t.Fatalf("el municipi exportat ha de conservar name/type; got %+v", territory.Municipality)
	}
	for _, token := range []string{
		"430385",
		"430385 03",
		"Baix Camp export F35-4U17 " + suffix,
		"Tarragona export F35-4U17 " + suffix,
	} {
		if !strings.Contains(rr.Body.String(), token) {
			t.Fatalf("l'export territorial confessional ha d'incloure %q; body=%s", token, rr.Body.String())
		}
	}
	if len(territory.Municipality.Nivells) != 7 || territory.Municipality.Nivells[0] != paisLevelID || territory.Municipality.Nivells[2] != provinciaLevelID || territory.Municipality.Nivells[3] != comarcaLevelID {
		t.Fatalf("el municipi exportat ha d'incloure nivells administratius; got %+v", territory.Municipality.Nivells)
	}
	if territory.Municipality.IdescatCodi != "430385 03" || territory.Municipality.MunicipiIdescat != "430385" {
		t.Fatalf("el municipi exportat ha d'incloure codis Idescat; got %+v", territory.Municipality)
	}
	if territory.Municipality.Comarca != "Baix Camp export F35-4U17 "+suffix || territory.Municipality.Provincia != "Tarragona export F35-4U17 "+suffix {
		t.Fatalf("el municipi exportat ha d'incloure comarca/provincia; got %+v", territory.Municipality)
	}
	if territory.Municipality.MunicipalityAdminName != "Municipi export F35-4U17 "+suffix || territory.Municipality.MunicipalityAdminCode != "430385" {
		t.Fatalf("el municipi exportat ha d'incloure municipality_admin_*; got %+v", territory.Municipality)
	}
	if territory.Nucleus == nil || len(territory.Nucleus.ParentNames) != 1 || territory.Nucleus.ParentNames[0] != "Municipi export F35-4U17 "+suffix {
		t.Fatalf("el nucli exportat ha d'incloure parent_names; got %+v", territory.Nucleus)
	}
	if territory.Nucleus.IdescatCodi != "430385 03" || territory.Nucleus.MunicipalityAdminName != "Municipi export F35-4U17 "+suffix || territory.Nucleus.MunicipalityAdminCode != "430385" {
		t.Fatalf("el nucli exportat ha de conservar el context administratiu; got %+v", territory.Nucleus)
	}
}

func TestF354U17ExportNormalizesMunicipalityFieldWhenRelationPointsToNucleus(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u17_export_normalizes_nucleus.sqlite3")
	user := createTestUser(t, database, "f354u17_export_normalized_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f354u17_export_normalized_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f354u17_export_normalized", "territori.confessional.import_export.export")
	assignPolicyToUser(t, database, user.ID, policyID)

	suffix := time.Now().Format("150405000000000")
	parentID := f353YCreateMunicipi(t, database, "Municipi pare export F35-4U17 "+suffix)
	nucleusID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            "Nucli legacy export F35-4U17 " + suffix,
		Tipus:          "municipi",
		MunicipiID:     sql.NullInt64{Int64: int64(parentID), Valid: true},
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi export legacy nucleus: %v", err)
	}
	entityID := f354SSaveEntitat(t, database, "f354u17_export_legacy_"+suffix, "Parroquia export legacy F35-4U17 "+suffix, "catolicisme_ritu_llati", "parroquia", "publicat")
	if _, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         nucleusID,
		EntitatReligiosaID: entityID,
		TipusRelacio:       "parroquia_local",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa export legacy: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/confessional/export", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminConfessionalExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("export normalized status=%d body=%s", rr.Code, rr.Body.String())
	}

	var payload struct {
		Items struct {
			RelacionsTerritorials []struct {
				Municipality struct {
					Name string `json:"name"`
				} `json:"municipality"`
				Nucleus *struct {
					Name string `json:"name"`
				} `json:"nucleus"`
			} `json:"relacions_territorials"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal export normalized: %v", err)
	}
	if len(payload.Items.RelacionsTerritorials) != 1 {
		t.Fatalf("s'esperava 1 relacio territorial exportada, got %d", len(payload.Items.RelacionsTerritorials))
	}
	territory := payload.Items.RelacionsTerritorials[0]
	if territory.Municipality.Name != "Municipi pare export F35-4U17 "+suffix {
		t.Fatalf("el municipi exportat s'ha de normalitzar al pare, got %+v", territory.Municipality)
	}
	if territory.Nucleus == nil || territory.Nucleus.Name != "Nucli legacy export F35-4U17 "+suffix {
		t.Fatalf("el nucli exportat s'ha de preservar, got %+v", territory.Nucleus)
	}
}

func TestF354U17DryRunNormalizesLegacyMunicipalityThatActuallyPointsToNucleus(t *testing.T) {
	app, database, session, csrfCookie, csrfToken := f354U17ImportSession(t, "test_f35_4u17_legacy_municipality_points_to_nucleus.sqlite3", "f354u17_legacy_nucleus")
	suffix := time.Now().Format("150405000000000")
	parentID := f353YCreateMunicipi(t, database, "Municipi pare import F35-4U17 "+suffix)
	nucleusName := "Nucli legacy import F35-4U17 " + suffix
	if _, err := database.CreateMunicipi(&db.Municipi{
		Nom:            nucleusName,
		Tipus:          "municipi",
		MunicipiID:     sql.NullInt64{Int64: int64(parentID), Valid: true},
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	}); err != nil {
		t.Fatalf("CreateMunicipi legacy import nucleus: %v", err)
	}

	payload := f354U17Payload(t, map[string]interface{}{
		"entitats_religioses":    f354U17BaseEntityPayload("f354u17_legacy_nucleus", "Parroquia F35-4U17 Legacy"),
		"relacions_entitats":     []map[string]interface{}{},
		"relacions_territorials": f354U17TerritoryRelationPayload("f354u17_legacy_nucleus", map[string]interface{}{"name": nucleusName, "type": "municipi"}, nil),
		"relacions_arxius":       []map[string]interface{}{},
	})
	dryRunBody := f354U17DryRunBody(t, app, database, session, csrfCookie, csrfToken, payload)
	if strings.Contains(dryRunBody, "Referència no resolta") || strings.Contains(dryRunBody, "Referència ambigua") {
		t.Fatalf("el payload legacy amb municipality apuntant a un nucli resoluble s'ha de normalitzar; body=%s", dryRunBody)
	}

	applyRR := f354U1Apply(t, app, session, csrfCookie, extractCSRFTokenFromHTML(t, dryRunBody), extractHiddenTextareaValue(t, dryRunBody, "payload_b64"))
	if applyRR.Code != http.StatusSeeOther {
		t.Fatalf("apply legacy normalization status=%d body=%s", applyRR.Code, applyRR.Body.String())
	}
	territoryRows := f354U1ListTerritory(t, database)
	if len(territoryRows) != 1 {
		t.Fatalf("s'esperava 1 relacio territorial importada, got %d", len(territoryRows))
	}
	if territoryRows[0].MunicipiID != parentID {
		t.Fatalf("el municipi importat s'ha de normalitzar al pare %d, got %+v", parentID, territoryRows[0])
	}
	if !territoryRows[0].NucliID.Valid {
		t.Fatalf("la relacio importada ha de conservar el nucli: %+v", territoryRows[0])
	}
}

func TestF354U17DryRunResolvesMisleadingMunicipiTypeAsNucleusWhenParentMatches(t *testing.T) {
	app, database, session, csrfCookie, csrfToken := f354U17ImportSession(t, "test_f35_4u17_misleading_nucleus_type.sqlite3", "f354u17_misleading_nucleus")
	suffix := time.Now().Format("150405000000000")
	parentName := "Municipi pare nucleus F35-4U17 " + suffix
	parentID := f353YCreateMunicipi(t, database, parentName)
	nucleusName := "Nucli tipus municipi F35-4U17 " + suffix
	if _, err := database.CreateMunicipi(&db.Municipi{
		Nom:            nucleusName,
		Tipus:          "municipi",
		MunicipiID:     sql.NullInt64{Int64: int64(parentID), Valid: true},
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	}); err != nil {
		t.Fatalf("CreateMunicipi misleading nucleus type: %v", err)
	}

	payload := f354U17Payload(t, map[string]interface{}{
		"entitats_religioses":    f354U17BaseEntityPayload("f354u17_misleading_nucleus", "Parroquia F35-4U17 Misleading Nucleus"),
		"relacions_entitats":     []map[string]interface{}{},
		"relacions_territorials": f354U17TerritoryRelationPayload("f354u17_misleading_nucleus", map[string]interface{}{"name": parentName, "type": "municipi"}, map[string]interface{}{"name": nucleusName, "type": "municipi", "parent_names": []string{parentName}}),
		"relacions_arxius":       []map[string]interface{}{},
	})
	body := f354U17DryRunBody(t, app, database, session, csrfCookie, csrfToken, payload)
	if strings.Contains(body, "Referència no resolta") || strings.Contains(body, "Referència ambigua") {
		t.Fatalf("un nucli structural amb tipus enganyos s'ha de resoldre com a nucli; body=%s", body)
	}
	if !strings.Contains(body, `name="payload_b64"`) {
		t.Fatalf("el dry-run valid ha d'oferir apply; body=%s", body)
	}
}

func TestF354U17DryRunArchiveFallbackRespectsMunicipalityContext(t *testing.T) {
	app, database, session, csrfCookie, csrfToken := f354U17ImportSession(t, "test_f35_4u17_archive_context.sqlite3", "f354u17_archive")

	suffix := time.Now().Format("150405000000000")
	levelID := f354U17CreateCountryLevel(t, database, "ES", "ESP", "724")
	municipiA, err := database.CreateMunicipi(&db.Municipi{
		Nom:            "Municipi arxiu A F35-4U17 " + suffix,
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
		NivellAdministratiuID: [7]sql.NullInt64{
			{Int64: int64(levelID), Valid: true},
		},
	})
	if err != nil {
		t.Fatalf("CreateMunicipi A: %v", err)
	}
	if _, err := database.CreateMunicipi(&db.Municipi{
		Nom:            "Municipi arxiu B F35-4U17 " + suffix,
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
		NivellAdministratiuID: [7]sql.NullInt64{
			{Int64: int64(levelID), Valid: true},
		},
	}); err != nil {
		t.Fatalf("CreateMunicipi B: %v", err)
	}
	f354SCreateArxiu(t, database, "Arxiu context F35-4U17 "+suffix, municipiA)

	baseEntity := f354U17BaseEntityPayload("f354u17_archive_entity", "Parroquia arxiu F35-4U17")
	withMunicipality := f354U17Payload(t, map[string]interface{}{
		"entitats_religioses":    baseEntity,
		"relacions_entitats":     []map[string]interface{}{},
		"relacions_territorials": []map[string]interface{}{},
		"relacions_arxius":       f354U17ArchiveRelationPayload("f354u17_archive_entity", "Arxiu context F35-4U17 "+suffix, "custodia", map[string]interface{}{"name": "Municipi arxiu A F35-4U17 " + suffix, "type": "municipi"}),
	})
	withCountryOnly := f354U17Payload(t, map[string]interface{}{
		"entitats_religioses":    baseEntity,
		"relacions_entitats":     []map[string]interface{}{},
		"relacions_territorials": []map[string]interface{}{},
		"relacions_arxius":       f354U17ArchiveRelationPayload("f354u17_archive_entity", "Arxiu context F35-4U17 "+suffix, "custodia", map[string]interface{}{"name": "Municipi arxiu A F35-4U17 " + suffix, "country_iso2": "ES"}),
	})
	withParentNamesOnly := f354U17Payload(t, map[string]interface{}{
		"entitats_religioses":    baseEntity,
		"relacions_entitats":     []map[string]interface{}{},
		"relacions_territorials": []map[string]interface{}{},
		"relacions_arxius":       f354U17ArchiveRelationPayload("f354u17_archive_entity", "Arxiu context F35-4U17 "+suffix, "custodia", map[string]interface{}{"name": "Municipi arxiu A F35-4U17 " + suffix, "parent_names": []string{}}),
	})
	wrongMunicipality := f354U17Payload(t, map[string]interface{}{
		"entitats_religioses":    baseEntity,
		"relacions_entitats":     []map[string]interface{}{},
		"relacions_territorials": []map[string]interface{}{},
		"relacions_arxius":       f354U17ArchiveRelationPayload("f354u17_archive_entity", "Arxiu context F35-4U17 "+suffix, "custodia", map[string]interface{}{"name": "Municipi arxiu B F35-4U17 " + suffix, "type": "municipi"}),
	})

	withMunicipalityBody := f354U17DryRunBody(t, app, database, session, csrfCookie, csrfToken, withMunicipality)
	if strings.Contains(withMunicipalityBody, "Referència ambigua") || strings.Contains(withMunicipalityBody, "Referència no resolta") {
		t.Fatalf("el context de municipi ha de resoldre l'arxiu correcte; body=%s", withMunicipalityBody)
	}
	if !strings.Contains(withMunicipalityBody, `name="payload_b64"`) {
		t.Fatalf("el dry-run amb municipi concret ha d'oferir apply; body=%s", withMunicipalityBody)
	}

	withCountryOnlyBody := f354U17DryRunBody(t, app, database, session, csrfCookie, csrfToken, withCountryOnly)
	if strings.Contains(withCountryOnlyBody, "Referència ambigua") || strings.Contains(withCountryOnlyBody, "Referència no resolta") {
		t.Fatalf("country_iso2 sense type ha de continuar resolent l'arxiu; body=%s", withCountryOnlyBody)
	}
	if !strings.Contains(withCountryOnlyBody, `name="payload_b64"`) {
		t.Fatalf("el dry-run amb country_iso2 sense type ha d'oferir apply; body=%s", withCountryOnlyBody)
	}

	withParentNamesOnlyBody := f354U17DryRunBody(t, app, database, session, csrfCookie, csrfToken, withParentNamesOnly)
	if strings.Contains(withParentNamesOnlyBody, "Referència ambigua") || strings.Contains(withParentNamesOnlyBody, "Referència no resolta") {
		t.Fatalf("parent_names buit sense type no ha de trencar la resolucio d'arxiu; body=%s", withParentNamesOnlyBody)
	}

	wrongMunicipalityBody := f354U17DryRunBody(t, app, database, session, csrfCookie, csrfToken, wrongMunicipality)
	for _, token := range []string{
		"Referència no resolta",
		"tipus=arxiu",
		"camp=archive",
		"valor=Arxiu context F35-4U17",
	} {
		if !strings.Contains(wrongMunicipalityBody, token) {
			t.Fatalf("un municipi incompatible no ha de resoldre l'arxiu (%q); body=%s", token, wrongMunicipalityBody)
		}
	}
}

func TestF354U17RoundTripWithTerritoryContext(t *testing.T) {
	targetApp, targetDB, targetSession, exportedJSON, _, _ := f354U1PrepareTargetAndExport(t, "test_f35_4u17_roundtrip.sqlite3", true)
	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, targetApp, targetSession)

	dryRunBody := f354U1DryRun(t, targetApp, targetDB, targetSession, csrfCookie, csrfToken, exportedJSON)
	if strings.Contains(dryRunBody, "Referència no resolta") || strings.Contains(dryRunBody, "Referència ambigua") {
		t.Fatalf("el round-trip territorial valid no ha de perdre context ni fallar; body=%s", dryRunBody)
	}

	applyRR := f354U1Apply(t, targetApp, targetSession, csrfCookie, extractCSRFTokenFromHTML(t, dryRunBody), extractHiddenTextareaValue(t, dryRunBody, "payload_b64"))
	if applyRR.Code != http.StatusSeeOther {
		t.Fatalf("apply round-trip F35-4U17 status=%d body=%s", applyRR.Code, applyRR.Body.String())
	}
	if got := len(f354U1ListTerritory(t, targetDB)); got != 1 {
		t.Fatalf("la relacio territorial round-trip s'ha de crear exactament un cop, got %d", got)
	}
}
