package integration

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF354U15EditEntityShowsEditablePrimaryMunicipality(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u15_edit_form.sqlite3")
	session := f353YAdminSession(t, database, "f354u15_edit_form")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	municipiName := "Municipi edit F35-4U15 " + suffix
	municipiID := f353YCreateMunicipi(t, database, municipiName)
	entityID := f353Z8SaveEntity(t, database, "f354u15_edit_"+suffix, "Parroquia edit F35-4U15 "+suffix, "parroquia", "publicat")
	f354SSaveMunicipiEntitatRelacio(t, database, municipiID, entityID, "parroquia", "publicat")

	body := f353YGet(t, app.AdminEditConfessional, "/confessional/entitats/"+strconv.Itoa(entityID)+"/edit", session)
	for _, token := range []string{
		`id="municipi_principal_label"`,
		`value="` + municipiName + `"`,
		`id="municipi_principal_id" name="municipi_principal_id" type="hidden" value="` + strconv.Itoa(municipiID) + `"`,
		`Les relacions territorials avancades permeten afegir nuclis, dates, observacions o relacions secundaries.`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("edicio d'entitat sense %q; body=%s", token, body)
		}
	}
	if strings.Contains(body, "Per canviar el municipi principal existent") {
		t.Fatalf("l'edicio no ha de forcar l'usuari a sortir del formulari; body=%s", body)
	}
}

func TestF354U15EditEntityChangesPrimaryMunicipalityWithoutDuplicating(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u15_change_primary.sqlite3")
	session := f353YAdminSession(t, database, "f354u15_change_primary")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	municipiAName := "Municipi A F35-4U15 " + suffix
	municipiBName := "Municipi B F35-4U15 " + suffix
	municipiAID := f353YCreateMunicipi(t, database, municipiAName)
	municipiBID := f353YCreateMunicipi(t, database, municipiBName)
	nucliID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            "Nucli F35-4U15 " + suffix,
		Tipus:          "nucli_urba",
		MunicipiID:     sql.NullInt64{Int64: int64(municipiAID), Valid: true},
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi nucli: %v", err)
	}
	entityID := f353Z8SaveEntity(t, database, "f354u15_change_"+suffix, "Parroquia change F35-4U15 "+suffix, "parroquia", "publicat")
	f354SSaveMunicipiEntitatRelacio(t, database, municipiAID, entityID, "parroquia", "publicat")
	if _, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiAID,
		NucliID:            sql.NullInt64{Int64: int64(nucliID), Valid: true},
		EntitatReligiosaID: entityID,
		TipusRelacio:       "altres",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa nucli: %v", err)
	}

	entity, err := database.GetEntitatReligiosa(entityID)
	if err != nil || entity == nil {
		t.Fatalf("GetEntitatReligiosa: %v", err)
	}
	form := f353Z12EntityEditForm(entityID, entity.Codi, entity.Nom, entity.NivellConfessionalCodi, 0)
	form.Set("parent_id", "")
	form.Set("municipi_principal_id", strconv.Itoa(municipiBID))
	form.Set("municipi_principal_label", municipiBName)
	rr := f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)
	f354U13AssertSafeRedirectLocation(t, rr.Header().Get("Location"))

	rels, err := database.ListMunicipiEntitatsReligiosesByEntitat(entityID)
	if err != nil {
		t.Fatalf("ListMunicipiEntitatsReligiosesByEntitat: %v", err)
	}
	primaryCount := 0
	nucliCount := 0
	for _, rel := range rels {
		if rel.ModeracioEstat == "rebutjat" {
			continue
		}
		if rel.NucliID.Valid {
			nucliCount++
			continue
		}
		primaryCount++
		if rel.MunicipiID != municipiBID {
			t.Fatalf("el municipi principal actualitzat ha de ser B; rel=%+v", rel)
		}
	}
	if primaryCount != 1 {
		t.Fatalf("nomes hi pot haver una relacio principal sense nucli; got=%d rels=%+v", primaryCount, rels)
	}
	if nucliCount != 1 {
		t.Fatalf("la relacio amb nucli s'ha de preservar; got=%d rels=%+v", nucliCount, rels)
	}
}

func TestF354U15PrimaryMunicipalityRejectsNucli(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u15_reject_nucli.sqlite3")
	session := f353YAdminSession(t, database, "f354u15_reject_nucli")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	municipiID := f353YCreateMunicipi(t, database, "Municipi reject F35-4U15 "+suffix)
	nucliName := "Nucli reject F35-4U15 " + suffix
	nucliID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            nucliName,
		Tipus:          "nucli_urba",
		MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi nucli: %v", err)
	}
	entityID := f353Z8SaveEntity(t, database, "f354u15_reject_"+suffix, "Parroquia reject F35-4U15 "+suffix, "parroquia", "publicat")
	f354SSaveMunicipiEntitatRelacio(t, database, municipiID, entityID, "parroquia", "publicat")

	entity, err := database.GetEntitatReligiosa(entityID)
	if err != nil || entity == nil {
		t.Fatalf("GetEntitatReligiosa: %v", err)
	}
	form := f353Z12EntityEditForm(entityID, entity.Codi, entity.Nom, entity.NivellConfessionalCodi, 0)
	form.Set("parent_id", "")
	form.Set("municipi_principal_id", strconv.Itoa(nucliID))
	form.Set("municipi_principal_label", nucliName)
	body := f353YPostConfessional(t, app.AdminSaveConfessional, session, form)
	if !strings.Contains(body, "municipi real") {
		t.Fatalf("cal error de nucli com a municipi principal; body=%s", body)
	}
	if rel := f354U13FindPrimaryMunicipiRelation(t, database, entityID, municipiID); rel == nil {
		t.Fatalf("la relacio principal existent no s'ha de perdre")
	}
}

func TestF354U15CreateEntityRejectsUnpublishedPrimaryMunicipality(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u15_create_unpublished_primary.sqlite3")
	session := f353YAdminSession(t, database, "f354u15_create_unpublished_primary")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	municipiName := "Municipi pendent alta F35-4U15 " + suffix
	municipiID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            municipiName,
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi pendent: %v", err)
	}
	entityName := "Parroquia pendent alta F35-4U15 " + suffix
	entityCode := "f354u15_create_pending_" + suffix
	form := f353ZEntityForm(entityName, entityCode, 0)
	form.Set("municipi_principal_id", strconv.Itoa(municipiID))
	form.Set("municipi_principal_label", municipiName)

	body := f353YPostConfessional(t, app.AdminSaveConfessional, session, form)
	if !strings.Contains(body, "municipi indicat ha d") {
		t.Fatalf("cal error de municipi principal no publicat en alta; body=%s", body)
	}
	if f354U13EntityExistsByName(t, database, entityName) {
		t.Fatalf("no s'ha de crear l'entitat si el municipi principal no esta publicat")
	}
}

func TestF354U15TerritorialRelationFormUsesAutocompleteAndPrefillsLabels(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u15_relacio_form.sqlite3")
	session := f353YAdminSession(t, database, "f354u15_relacio_form")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	municipiName := "Municipi relacio F35-4U15 " + suffix
	municipiID := f353YCreateMunicipi(t, database, municipiName)
	nucliName := "Nucli relacio F35-4U15 " + suffix
	nucliID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            nucliName,
		Tipus:          "nucli_urba",
		MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi nucli: %v", err)
	}
	entityName := "Parroquia relacio F35-4U15 " + suffix
	entityID := f353Z8SaveEntity(t, database, "f354u15_relacio_"+suffix, entityName, "parroquia", "publicat")
	relID, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiID,
		NucliID:            sql.NullInt64{Int64: int64(nucliID), Valid: true},
		EntitatReligiosaID: entityID,
		TipusRelacio:       "parroquia",
		ModeracioEstat:     "publicat",
	})
	if err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa: %v", err)
	}

	newBody := f353YGet(t, app.AdminNewConfessional, "/confessional/municipis-entitats/new?municipi_id="+strconv.Itoa(municipiID)+"&entitat_religiosa_id="+strconv.Itoa(entityID), session)
	for _, token := range []string{
		`id="municipi_id_label"`,
		`id="municipi_id" name="municipi_id" type="hidden" value="` + strconv.Itoa(municipiID) + `"`,
		`id="entitat_religiosa_id_label"`,
		`id="entitat_religiosa_id" name="entitat_religiosa_id" type="hidden" value="` + strconv.Itoa(entityID) + `"`,
		`id="nucli_id_label"`,
		`role="combobox"`,
		`role="listbox"`,
		`/api/territori/municipis/suggest?scope=municipi`,
		`/api/territori/municipis/suggest?scope=nucli`,
		`/api/confessional/entitats/suggest`,
		`/static/js/arxiu-form-suggest.js`,
		`/static/js/confessional-form.js`,
	} {
		if !strings.Contains(newBody, token) {
			t.Fatalf("formulari relacio sense %q; body=%s", token, newBody)
		}
	}
	for _, forbidden := range []string{
		`<select id="municipi_id"`,
		`<select id="entitat_religiosa_id"`,
		`<script>`,
		`style=`,
		`onclick=`,
		`onchange=`,
	} {
		if strings.Contains(newBody, forbidden) {
			t.Fatalf("formulari relacio no ha de contenir %q; body=%s", forbidden, newBody)
		}
	}

	editBody := f353YGet(t, app.AdminEditConfessional, "/confessional/municipis-entitats/"+strconv.Itoa(relID)+"/edit", session)
	for _, token := range []string{
		`id="municipi_id_label"`,
		`value="` + municipiName + `"`,
		`id="nucli_id_label"`,
		`value="` + nucliName + `"`,
		`id="entitat_religiosa_id_label"`,
		`value="` + entityName + `"`,
		`id="municipi_id" name="municipi_id" type="hidden" value="` + strconv.Itoa(municipiID) + `"`,
		`id="nucli_id" name="nucli_id" type="hidden" value="` + strconv.Itoa(nucliID) + `"`,
		`id="entitat_religiosa_id" name="entitat_religiosa_id" type="hidden" value="` + strconv.Itoa(entityID) + `"`,
	} {
		if !strings.Contains(editBody, token) {
			t.Fatalf("prefill relacio sense %q; body=%s", token, editBody)
		}
	}
}

func TestF354U15EditEntityRejectsUnpublishedPrimaryMunicipality(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u15_edit_unpublished_primary.sqlite3")
	session := f353YAdminSession(t, database, "f354u15_edit_unpublished_primary")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	municipiAName := "Municipi A pendent edit F35-4U15 " + suffix
	municipiBName := "Municipi B pendent edit F35-4U15 " + suffix
	municipiAID := f353YCreateMunicipi(t, database, municipiAName)
	municipiBID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            municipiBName,
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi pendent B: %v", err)
	}
	entityID := f353Z8SaveEntity(t, database, "f354u15_edit_pending_"+suffix, "Parroquia edit pendent F35-4U15 "+suffix, "parroquia", "publicat")
	f354SSaveMunicipiEntitatRelacio(t, database, municipiAID, entityID, "parroquia", "publicat")

	entity, err := database.GetEntitatReligiosa(entityID)
	if err != nil || entity == nil {
		t.Fatalf("GetEntitatReligiosa: %v", err)
	}
	form := f353Z12EntityEditForm(entityID, entity.Codi, entity.Nom, entity.NivellConfessionalCodi, 0)
	form.Set("parent_id", "")
	form.Set("municipi_principal_id", strconv.Itoa(municipiBID))
	form.Set("municipi_principal_label", municipiBName)

	body := f353YPostConfessional(t, app.AdminSaveConfessional, session, form)
	if !strings.Contains(body, "municipi indicat ha d") {
		t.Fatalf("cal error de municipi principal no publicat en edicio; body=%s", body)
	}
	if rel := f354U13FindPrimaryMunicipiRelation(t, database, entityID, municipiAID); rel == nil {
		t.Fatalf("la relacio principal existent ha de continuar apuntant a A")
	}
	if rel := f354U13FindPrimaryMunicipiRelation(t, database, entityID, municipiBID); rel != nil {
		t.Fatalf("no s'ha de crear cap relacio principal nova cap a B: %+v", rel)
	}
	if got := f354U13CountPrimaryMunicipiRelations(t, database, entityID, municipiAID); got != 1 {
		t.Fatalf("nomes hi ha d'haver una relacio principal cap a A, got=%d", got)
	}
}

func TestF354U15MunicipiSuggestScopesFilterMunicipisAndNuclis(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u15_suggest.sqlite3")
	session := f353YAdminSession(t, database, "f354u15_suggest")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	municipiName := "Suggest municipi objectiu F35-4U15 " + suffix
	municipiID := f353YCreateMunicipi(t, database, municipiName)
	for i := 0; i < 12; i++ {
		_, err := database.CreateMunicipi(&db.Municipi{
			Nom:            "Suggest AAA nucli truncament F35-4U15 " + suffix + " " + strconv.Itoa(i),
			Tipus:          "nucli_urba",
			MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
			Estat:          "actiu",
			ModeracioEstat: "publicat",
		})
		if err != nil {
			t.Fatalf("CreateMunicipi nucli truncament municipi: %v", err)
		}
	}

	municipiItems := f354U15MunicipiSuggest(t, app, session, "/api/territori/municipis/suggest?scope=municipi&q=Suggest")
	if !f354U15SuggestContainsNom(municipiItems, municipiName) {
		t.Fatalf("scope municipi ha d'incloure el municipi objectiu malgrat els nuclis anteriors al limit: %+v", municipiItems)
	}
	for _, item := range municipiItems {
		if strings.Contains(item.Nom, "nucli truncament") {
			t.Fatalf("scope municipi no ha d'incloure nuclis: %+v", municipiItems)
		}
	}

	parentMunicipiName := "Suggest pare nucli F35-4U15 " + suffix
	parentMunicipiID := f353YCreateMunicipi(t, database, parentMunicipiName)
	nucliName := "Suggest nucli objectiu F35-4U15 " + suffix
	_, err := database.CreateMunicipi(&db.Municipi{
		Nom:            nucliName,
		Tipus:          "nucli_urba",
		MunicipiID:     sql.NullInt64{Int64: int64(parentMunicipiID), Valid: true},
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi nucli objectiu: %v", err)
	}
	for i := 0; i < 12; i++ {
		f353YCreateMunicipi(t, database, "Suggest AAA municipi truncament F35-4U15 "+suffix+" "+strconv.Itoa(i))
	}
	nucliItems := f354U15MunicipiSuggest(t, app, session, "/api/territori/municipis/suggest?scope=nucli&parent_municipi_id="+strconv.Itoa(parentMunicipiID)+"&q=Suggest")
	if !f354U15SuggestContainsNom(nucliItems, nucliName) {
		t.Fatalf("scope nucli ha d'incloure el nucli objectiu malgrat els municipis anteriors al limit: %+v", nucliItems)
	}
	for _, item := range nucliItems {
		if strings.Contains(item.Nom, "municipi truncament") || item.Nom == parentMunicipiName {
			t.Fatalf("scope nucli no ha d'incloure municipis: %+v", nucliItems)
		}
	}

	municipiItemsWithParent := f354U15MunicipiSuggest(t, app, session, "/api/territori/municipis/suggest?scope=municipi&parent_municipi_id="+strconv.Itoa(parentMunicipiID)+"&q=objectiu")
	if !f354U15SuggestContainsNom(municipiItemsWithParent, municipiName) {
		t.Fatalf("scope municipi ha d'ignorar parent_municipi_id i mantenir municipis: %+v", municipiItemsWithParent)
	}
	for _, item := range municipiItemsWithParent {
		if strings.Contains(item.Nom, "nucli") || item.Nom == parentMunicipiName {
			t.Fatalf("scope municipi amb parent no ha d'incloure nuclis: %+v", municipiItemsWithParent)
		}
	}
}

func TestF354U15TerritorialRelationRejectsUnpublishedMunicipality(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u15_relacio_unpublished_municipi.sqlite3")
	session := f353YAdminSession(t, database, "f354u15_relacio_unpublished_municipi")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	municipiID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            "Municipi draft F35-4U15 " + suffix,
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi esborrany: %v", err)
	}
	entityID := f353Z8SaveEntity(t, database, "f354u15_rel_draft_municipi_"+suffix, "Parroquia draft municipi F35-4U15 "+suffix, "parroquia", "publicat")
	body := f353YPostConfessional(t, app.AdminSaveConfessional, session, f354U15RelationForm(municipiID, 0, entityID))
	if !strings.Contains(body, "municipi indicat ha d") {
		t.Fatalf("cal error de municipi no publicat; body=%s", body)
	}
	rels, err := database.ListMunicipiEntitatsReligiosesByEntitat(entityID)
	if err != nil {
		t.Fatalf("ListMunicipiEntitatsReligiosesByEntitat: %v", err)
	}
	if len(rels) != 0 {
		t.Fatalf("no s'ha de guardar cap relacio amb municipi no publicat: %+v", rels)
	}
}

func TestF354U15TerritorialRelationRejectsUnpublishedNucli(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u15_relacio_unpublished_nucli.sqlite3")
	session := f353YAdminSession(t, database, "f354u15_relacio_unpublished_nucli")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	municipiID := f353YCreateMunicipi(t, database, "Municipi publicat F35-4U15 "+suffix)
	draftNucliID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            "Nucli draft F35-4U15 " + suffix,
		Tipus:          "nucli_urba",
		MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi nucli esborrany: %v", err)
	}
	entityID := f353Z8SaveEntity(t, database, "f354u15_rel_draft_nucli_"+suffix, "Parroquia draft nucli F35-4U15 "+suffix, "parroquia", "publicat")
	f354SSaveMunicipiEntitatRelacio(t, database, municipiID, entityID, "parroquia", "publicat")

	body := f353YPostConfessional(t, app.AdminSaveConfessional, session, f354U15RelationForm(municipiID, draftNucliID, entityID))
	if !strings.Contains(body, "nucli indicat ha d") {
		t.Fatalf("cal error de nucli no publicat; body=%s", body)
	}
	if rel := f354U13FindPrimaryMunicipiRelation(t, database, entityID, municipiID); rel == nil {
		t.Fatalf("la relacio existent s'ha de preservar quan el nucli no es publicat")
	}
	rels, err := database.ListMunicipiEntitatsReligiosesByEntitat(entityID)
	if err != nil {
		t.Fatalf("ListMunicipiEntitatsReligiosesByEntitat: %v", err)
	}
	for _, rel := range rels {
		if rel.NucliID.Valid && int(rel.NucliID.Int64) == draftNucliID {
			t.Fatalf("no s'ha de guardar cap relacio amb nucli no publicat: %+v", rels)
		}
	}
}

type f354U15SuggestItem struct {
	Nom string `json:"nom"`
}

func f354U15MunicipiSuggest(t *testing.T, app interface {
	AdminMunicipisSuggest(http.ResponseWriter, *http.Request)
}, session *http.Cookie, path string) []f354U15SuggestItem {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminMunicipisSuggest(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminMunicipisSuggest(%s) status=%d body=%s", path, rr.Code, rr.Body.String())
	}
	var payload struct {
		Items []f354U15SuggestItem `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal suggest municipis: %v body=%s", err, rr.Body.String())
	}
	return payload.Items
}

func f354U15SuggestContainsNom(items []f354U15SuggestItem, want string) bool {
	for _, item := range items {
		if strings.TrimSpace(item.Nom) == strings.TrimSpace(want) {
			return true
		}
	}
	return false
}

func f354U15RelationForm(municipiID, nucliID, entityID int) url.Values {
	form := url.Values{}
	form.Set("kind", "relacio")
	form.Set("municipi_id", strconv.Itoa(municipiID))
	form.Set("entitat_religiosa_id", strconv.Itoa(entityID))
	form.Set("tipus_relacio", "parroquia")
	form.Set("status", "publicat")
	if nucliID > 0 {
		form.Set("nucli_id", strconv.Itoa(nucliID))
	}
	return form
}
