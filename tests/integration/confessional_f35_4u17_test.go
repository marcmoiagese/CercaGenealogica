package integration

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF354U17DryRunShowsContextualUnresolvedMunicipality(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u17_unresolved_municipality.sqlite3")
	user := createTestUser(t, database, "f354u17_unresolved_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f354u17_unresolved_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f354u17_unresolved", "territori.confessional.import_export.import")
	addGrantToPolicy(t, database, policyID, "territori.confessional.import_export.view")
	assignPolicyToUser(t, database, user.ID, policyID)

	payload := confessionalExportPayloadForTest(t, map[string]interface{}{
		"entitats_religioses": []map[string]interface{}{
			confEntityExportRecord("f354u17_missing_mun", "Parroquia F35-4U17 Missing", "catolicisme_ritu_llati", "parroquia"),
		},
		"relacions_entitats": []map[string]interface{}{},
		"relacions_territorials": []map[string]interface{}{
			{
				"entity":        confEntityRef("f354u17_missing_mun", "catolicisme_ritu_llati", "parroquia"),
				"municipality":  map[string]interface{}{"name": "Municipi inexistent F35-4U17", "type": "municipi", "country_iso2": "ES"},
				"relation_type": "parroquia_local",
			},
		},
		"relacions_arxius": []map[string]interface{}{},
	})

	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, app, session)
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
	app, database := newTestAppForLogin(t, "test_f35_4u17_same_payload.sqlite3")
	user := createTestUser(t, database, "f354u17_same_payload_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f354u17_same_payload_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f354u17_same_payload", "territori.confessional.import_export.import")
	addGrantToPolicy(t, database, policyID, "territori.confessional.import_export.view")
	assignPolicyToUser(t, database, user.ID, policyID)

	payload := confessionalExportPayloadForTest(t, map[string]interface{}{
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

	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, app, session)
	body := f354U1DryRun(t, app, database, session, csrfCookie, csrfToken, payload)
	if strings.Contains(body, "seccio=relacions_entitats") || strings.Contains(body, "Referencia no resolta") {
		t.Fatalf("les relacions entre entitats del mateix payload han de resoldre's sense error; body=%s", body)
	}
	if !strings.Contains(body, `name="payload_b64"`) {
		t.Fatalf("el dry-run valid ha d'oferir apply; body=%s", body)
	}
}

func TestF354U17DryRunResolvesMunicipalityAndNucleusWithParentContext(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u17_municipality_nucleus.sqlite3")
	user := createTestUser(t, database, "f354u17_territory_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f354u17_territory_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f354u17_territory", "territori.confessional.import_export.import")
	addGrantToPolicy(t, database, policyID, "territori.confessional.import_export.view")
	assignPolicyToUser(t, database, user.ID, policyID)

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

	payload := confessionalExportPayloadForTest(t, map[string]interface{}{
		"entitats_religioses": []map[string]interface{}{
			confEntityExportRecord("f354u17_local", "Parroquia F35-4U17 Territorial", "catolicisme_ritu_llati", "parroquia"),
		},
		"relacions_entitats": []map[string]interface{}{},
		"relacions_territorials": []map[string]interface{}{
			{
				"entity":        confEntityRef("f354u17_local", "catolicisme_ritu_llati", "parroquia"),
				"municipality":  map[string]interface{}{"name": "Municipi F35-4U17 " + suffix, "type": "municipi"},
				"nucleus":       map[string]interface{}{"name": "Nucli F35-4U17 " + suffix, "type": "nucli_urba", "parent_names": []string{"Municipi F35-4U17 " + suffix}},
				"relation_type": "parroquia_local",
			},
		},
		"relacions_arxius": []map[string]interface{}{},
	})

	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, app, session)
	body := f354U1DryRun(t, app, database, session, csrfCookie, csrfToken, payload)
	if strings.Contains(body, "Referencia no resolta") || strings.Contains(body, "Referencia ambigua") {
		t.Fatalf("municipi i nucli existents amb context de pare s'han de resoldre; body=%s", body)
	}
	if !strings.Contains(body, `name="payload_b64"`) {
		t.Fatalf("el dry-run valid amb municipi i nucli ha d'oferir apply; body=%s", body)
	}
}

func TestF354U17DryRunFlagsAmbiguousNucleus(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u17_ambiguous_nucleus.sqlite3")
	user := createTestUser(t, database, "f354u17_ambiguous_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f354u17_ambiguous_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f354u17_ambiguous", "territori.confessional.import_export.import")
	addGrantToPolicy(t, database, policyID, "territori.confessional.import_export.view")
	assignPolicyToUser(t, database, user.ID, policyID)

	suffix := time.Now().Format("150405000000000")
	municipiID := f353YCreateMunicipi(t, database, "Municipi ambigu F35-4U17 "+suffix)
	for i := 0; i < 2; i++ {
		if _, err := database.CreateMunicipi(&db.Municipi{
			Nom:            "Nucli ambigu F35-4U17 " + suffix,
			Tipus:          "nucli_urba",
			MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
			Estat:          "actiu",
			ModeracioEstat: "publicat",
		}); err != nil {
			t.Fatalf("CreateMunicipi nucli ambigu: %v", err)
		}
	}

	payload := confessionalExportPayloadForTest(t, map[string]interface{}{
		"entitats_religioses": []map[string]interface{}{
			confEntityExportRecord("f354u17_ambiguous_local", "Parroquia F35-4U17 Ambigua", "catolicisme_ritu_llati", "parroquia"),
		},
		"relacions_entitats": []map[string]interface{}{},
		"relacions_territorials": []map[string]interface{}{
			{
				"entity":        confEntityRef("f354u17_ambiguous_local", "catolicisme_ritu_llati", "parroquia"),
				"municipality":  map[string]interface{}{"name": "Municipi ambigu F35-4U17 " + suffix, "type": "municipi"},
				"nucleus":       map[string]interface{}{"name": "Nucli ambigu F35-4U17 " + suffix, "type": "nucli_urba", "parent_names": []string{"Municipi ambigu F35-4U17 " + suffix}},
				"relation_type": "parroquia_local",
			},
		},
		"relacions_arxius": []map[string]interface{}{},
	})

	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, app, session)
	body := f354U1DryRun(t, app, database, session, csrfCookie, csrfToken, payload)
	for _, token := range []string{
		"Referencia ambigua",
		"tipus=nucli",
		"camp=nucleus",
		"candidats=Nucli ambigu F35-4U17",
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("el nucli ambigu ha d'explicar l'ambiguitat (%q); body=%s", token, body)
		}
	}
}

func TestF354U17ExportIncludesTerritoryContext(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u17_export_context.sqlite3")
	user := createTestUser(t, database, "f354u17_export_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f354u17_export_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f354u17_export", "territori.confessional.import_export.export")
	assignPolicyToUser(t, database, user.ID, policyID)

	suffix := time.Now().Format("150405000000000")
	municipiID := f353YCreateMunicipi(t, database, "Municipi export F35-4U17 "+suffix)
	nucliID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            "Nucli export F35-4U17 " + suffix,
		Tipus:          "nucli_urba",
		MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
		Estat:          "actiu",
		ModeracioEstat: "publicat",
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
					Name        string   `json:"name"`
					Type        string   `json:"type"`
					CountryISO2 string   `json:"country_iso2"`
					ParentNames []string `json:"parent_names"`
				} `json:"municipality"`
				Nucleus *struct {
					Name        string   `json:"name"`
					Type        string   `json:"type"`
					CountryISO2 string   `json:"country_iso2"`
					ParentNames []string `json:"parent_names"`
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
	if territory.Municipality.Name != "Municipi export F35-4U17 "+suffix || territory.Municipality.Type != "municipi" {
		t.Fatalf("el municipi exportat ha de conservar name/type; got %+v", territory.Municipality)
	}
	if territory.Nucleus == nil || len(territory.Nucleus.ParentNames) != 1 || territory.Nucleus.ParentNames[0] != "Municipi export F35-4U17 "+suffix {
		t.Fatalf("el nucli exportat ha d'incloure parent_names; got %+v", territory.Nucleus)
	}
}

func TestF354U17RoundTripWithTerritoryContext(t *testing.T) {
	targetApp, targetDB, targetSession, exportedJSON, _, _ := f354U1PrepareTargetAndExport(t, "test_f35_4u17_roundtrip.sqlite3", true)
	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, targetApp, targetSession)

	dryRunBody := f354U1DryRun(t, targetApp, targetDB, targetSession, csrfCookie, csrfToken, exportedJSON)
	if strings.Contains(dryRunBody, "Referencia no resolta") || strings.Contains(dryRunBody, "Referencia ambigua") {
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
