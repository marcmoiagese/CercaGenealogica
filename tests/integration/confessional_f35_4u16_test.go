package integration

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF354U16RejectsTorredembarraCapellaUnderUnitatPastoralOnCreate(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u16_create_reject.sqlite3")
	session := f353YAdminSession(t, database, "f354u16_create")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	parentID := f353Z8SaveEntity(t, database, "f354u16_parent_"+suffix, "Unitat pastoral de Torredembarra "+suffix, "unitat_pastoral", "publicat")
	form := f353ZEntityForm("Capella de la Mare de Deu Miraculosa "+suffix, "f354u16_capella_"+suffix, 0)
	form.Set("nivell_confessional_codi", "capella_ermita_santuari")
	form.Set("parent_id", strconv.Itoa(parentID))

	body := f353YPostConfessional(t, app.AdminSaveConfessional, session, form)
	for _, token := range []string{"Relacio jerarquica no valida", "Capella / Ermita / Santuari", "Unitat pastoral", "Parroquia"} {
		if !strings.Contains(body, token) {
			t.Fatalf("l'alta ha de rebutjar el cas Torredembarra amb missatge accionable (%q); body=%s", token, body)
		}
	}
	if f354U13EntityExistsByName(t, database, "Capella de la Mare de Deu Miraculosa "+suffix) {
		t.Fatalf("la capella incompatible no s'ha de crear")
	}
}

func TestF354U16RejectsIncompatibleParentChangeOnEdit(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u16_edit_reject.sqlite3")
	session := f353YAdminSession(t, database, "f354u16_edit")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	unitatID := f353Z8SaveEntity(t, database, "f354u16_unitat_"+suffix, "Unitat pastoral edit F35-4U16 "+suffix, "unitat_pastoral", "publicat")
	parishID := f353Z8SaveEntity(t, database, "f354u16_parish_"+suffix, "Parroquia edit F35-4U16 "+suffix, "parroquia", "publicat")
	capellaID := f353Z8SaveEntity(t, database, "f354u16_capella_edit_"+suffix, "Capella edit F35-4U16 "+suffix, "capella_ermita_santuari", "publicat")
	f354SSaveEntitatRelacio(t, database, parishID, capellaID, "capella_ermita_santuari", "publicat")

	child, err := database.GetEntitatReligiosa(capellaID)
	if err != nil || child == nil {
		t.Fatalf("GetEntitatReligiosa capella: %v", err)
	}
	form := f353Z12EntityEditForm(capellaID, child.Codi, child.Nom, child.NivellConfessionalCodi, unitatID)
	body := f353YPostConfessional(t, app.AdminSaveConfessional, session, form)
	if !strings.Contains(body, "Relacio jerarquica no valida") {
		t.Fatalf("l'edicio ha de rebutjar canviar el pare a una unitat pastoral incompatible; body=%s", body)
	}

	after, err := database.GetEntitatReligiosa(capellaID)
	if err != nil || after == nil {
		t.Fatalf("GetEntitatReligiosa after: %v", err)
	}
	if after.ParentID.Valid && int(after.ParentID.Int64) == unitatID {
		t.Fatalf("el parent incompatible no s'ha de persistir a l'entitat")
	}
}

func TestF354U16RejectsAdvancedHierarchyAndKeepsValidChain(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u16_rel_ent.sqlite3")
	session := f353YAdminSession(t, database, "f354u16_rel_ent")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	unitatID := f353Z8SaveEntity(t, database, "f354u16_rel_unitat_"+suffix, "Unitat pastoral relacio F35-4U16 "+suffix, "unitat_pastoral", "publicat")
	parishID := f353Z8SaveEntity(t, database, "f354u16_rel_parish_"+suffix, "Parroquia relacio F35-4U16 "+suffix, "parroquia", "publicat")
	capellaID := f353Z8SaveEntity(t, database, "f354u16_rel_capella_"+suffix, "Capella relacio F35-4U16 "+suffix, "capella_ermita_santuari", "publicat")

	form := f353ZEntityForm("rel", "rel", 0)
	form.Set("kind", "rel_ent")
	form.Set("entitat_origen_id", strconv.Itoa(unitatID))
	form.Set("entitat_desti_id", strconv.Itoa(capellaID))
	body := f353YPostConfessional(t, app.AdminSaveConfessional, session, form)
	if !strings.Contains(body, "Relacio jerarquica no valida") {
		t.Fatalf("la relacio avancada ha de rebutjar Unitat pastoral -> Capella; body=%s", body)
	}
	if got := f353Z12CountHierarchyRelations(t, database, unitatID, capellaID); got != 0 {
		t.Fatalf("no s'ha de crear cap relacio incompatible, got=%d", got)
	}

	if _, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
		EntitatOrigenID: unitatID,
		EntitatDestiID:  capellaID,
		TipusRelacio:    "capella_ermita_santuari",
		ModeracioEstat:  "publicat",
	}); err == nil {
		t.Fatalf("el backend db.SaveEntitatReligiosaRelacio ha de blindar la mateixa incompatibilitat")
	}

	f354SSaveEntitatRelacio(t, database, unitatID, parishID, "parroquia", "publicat")
	f354SSaveEntitatRelacio(t, database, parishID, capellaID, "capella_ermita_santuari", "publicat")
}

func TestF354U16ImportDryRunAndApplyRejectInvalidHierarchy(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u16_import_reject.sqlite3")
	user := createTestUser(t, database, "f354u16_import_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f354u16_import_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f354u16_import", "territori.confessional.import_export.import")
	addGrantToPolicy(t, database, policyID, "territori.confessional.import_export.view")
	assignPolicyToUser(t, database, user.ID, policyID)

	payload := confessionalExportPayloadForTest(t, map[string]interface{}{
		"entitats_religioses": []map[string]interface{}{
			confEntityExportRecord("f354u16_unitat", "Unitat pastoral de Torredembarra", "catolicisme_ritu_llati", "unitat_pastoral"),
			confEntityExportRecord("f354u16_capella", "Capella de la Mare de Deu Miraculosa", "catolicisme_ritu_llati", "capella_ermita_santuari"),
		},
		"relacions_entitats": []map[string]interface{}{
			{
				"parent":        confEntityRef("f354u16_unitat", "catolicisme_ritu_llati", "unitat_pastoral"),
				"child":         confEntityRef("f354u16_capella", "catolicisme_ritu_llati", "capella_ermita_santuari"),
				"relation_type": "capella_ermita_santuari",
			},
		},
	})

	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, app, session)
	req := newMultipartRequest(t, "/admin/confessional/import/dry-run", "import_file", "invalid-confessional.json", payload, map[string]string{
		"csrf_token": csrfToken,
	})
	req.AddCookie(session)
	req.AddCookie(csrfCookie)
	rr := httptest.NewRecorder()
	app.AdminConfessionalImportDryRun(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("dry-run status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, token := range []string{"Relacio jerarquica no valida", "Capella / Ermita / Santuari", "Unitat pastoral"} {
		if !strings.Contains(body, token) {
			t.Fatalf("el dry-run ha de descriure la incompatibilitat (%q); body=%s", token, body)
		}
	}
	if strings.Contains(body, `/admin/confessional/import/apply`) || strings.Contains(body, `name="payload_b64"`) {
		t.Fatalf("un dry-run invalid no ha d'oferir apply; body=%s", body)
	}

	applyReq := httptest.NewRequest(http.MethodPost, "/admin/confessional/import/apply", strings.NewReader("csrf_token="+csrfToken+"&payload_b64="+base64.StdEncoding.EncodeToString(payload)))
	applyReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	applyReq.AddCookie(session)
	applyReq.AddCookie(csrfCookie)
	applyRR := httptest.NewRecorder()
	app.AdminConfessionalImportApply(applyRR, applyReq)
	if applyRR.Code != http.StatusOK {
		t.Fatalf("apply invalid ha de rerenderitzar el resultat, got=%d body=%s", applyRR.Code, applyRR.Body.String())
	}
	if rows, _ := database.ListEntitatsReligioses(); len(filterPublishedConfEntitats(rows)) != 0 {
		t.Fatalf("l'import real no ha de crear res quan el dry-run detecta la incompatibilitat")
	}
}

func TestF354U16ValidRoundTripKeepsHierarchyCompatible(t *testing.T) {
	sourceApp, sourceDB := newTestAppForLogin(t, "test_f35_4u16_roundtrip_source.sqlite3")
	sourceUser := createTestUser(t, sourceDB, "f354u16_source_"+time.Now().Format("150405000000000"))
	sourceSession := createSessionCookie(t, sourceDB, sourceUser.ID, "sess_f354u16_source_"+time.Now().Format("150405000000000"))
	sourcePolicy := createPolicyWithGrant(t, sourceDB, "f354u16_source", "territori.confessional.import_export.export")
	assignPolicyToUser(t, sourceDB, sourceUser.ID, sourcePolicy)

	suffix := time.Now().Format("150405000000000")
	unitatID := f354SSaveEntitat(t, sourceDB, "f354u16_round_unitat_"+suffix, "Unitat pastoral roundtrip "+suffix, "catolicisme_ritu_llati", "unitat_pastoral", "publicat")
	parishID := f354SSaveEntitat(t, sourceDB, "f354u16_round_parish_"+suffix, "Parroquia roundtrip "+suffix, "catolicisme_ritu_llati", "parroquia", "publicat")
	capellaID := f354SSaveEntitat(t, sourceDB, "f354u16_round_capella_"+suffix, "Capella roundtrip "+suffix, "catolicisme_ritu_llati", "capella_ermita_santuari", "publicat")
	f354SSaveEntitatRelacio(t, sourceDB, unitatID, parishID, "parroquia", "publicat")
	f354SSaveEntitatRelacio(t, sourceDB, parishID, capellaID, "capella_ermita_santuari", "publicat")

	req := httptest.NewRequest(http.MethodGet, "/admin/confessional/export", nil)
	req.AddCookie(sourceSession)
	rr := httptest.NewRecorder()
	sourceApp.AdminConfessionalExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("export status=%d body=%s", rr.Code, rr.Body.String())
	}

	targetApp, targetDB := newTestAppForLogin(t, "test_f35_4u16_roundtrip_target.sqlite3")
	targetUser := createTestUser(t, targetDB, "f354u16_target_"+time.Now().Format("150405000000000"))
	targetSession := createSessionCookie(t, targetDB, targetUser.ID, "sess_f354u16_target_"+time.Now().Format("150405000000000"))
	targetPolicy := createPolicyWithGrant(t, targetDB, "f354u16_target", "territori.confessional.import_export.import")
	addGrantToPolicy(t, targetDB, targetPolicy, "territori.confessional.import_export.view")
	assignPolicyToUser(t, targetDB, targetUser.ID, targetPolicy)

	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, targetApp, targetSession)
	dryRunReq := newMultipartRequest(t, "/admin/confessional/import/dry-run", "import_file", "roundtrip-confessional.json", rr.Body.Bytes(), map[string]string{
		"csrf_token": csrfToken,
	})
	dryRunReq.AddCookie(targetSession)
	dryRunReq.AddCookie(csrfCookie)
	dryRunRR := httptest.NewRecorder()
	targetApp.AdminConfessionalImportDryRun(dryRunRR, dryRunReq)
	if dryRunRR.Code != http.StatusOK {
		t.Fatalf("roundtrip dry-run status=%d body=%s", dryRunRR.Code, dryRunRR.Body.String())
	}
	body := dryRunRR.Body.String()
	if strings.Contains(body, "Relacio jerarquica no valida") || strings.Contains(body, "confessional-import-alert") && strings.Contains(body, "error") {
		t.Fatalf("un round-trip jerarquicament valid no ha de fallar el dry-run; body=%s", body)
	}
	if !strings.Contains(body, `name="payload_b64"`) {
		t.Fatalf("el round-trip valid ha de poder-se aplicar; body=%s", body)
	}
}

func confessionalExportPayloadForTest(t *testing.T, items map[string]interface{}) []byte {
	t.Helper()
	payload := map[string]interface{}{
		"schema":      "cercagenealogica.confessional.v1",
		"exported_at": time.Now().Format(time.RFC3339),
		"source": map[string]string{
			"app":    "CercaGenealogica",
			"module": "confessional",
		},
		"items": items,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal payload: %v", err)
	}
	return raw
}

func confEntityExportRecord(code, name, religionCode, levelCode string) map[string]interface{} {
	return map[string]interface{}{
		"ref":               confEntityRef(code, religionCode, levelCode),
		"name":              name,
		"religion_code":     religionCode,
		"level_code":        levelCode,
		"state":             "actiu",
		"moderation_status": "publicat",
	}
}

func confEntityRef(code, religionCode, levelCode string) map[string]string {
	return map[string]string{
		"code":          code,
		"religion_code": religionCode,
		"level_code":    levelCode,
	}
}
