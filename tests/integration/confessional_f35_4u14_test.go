package integration

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF354U14ConfessionalLevelProfileShowsCatalogAndEntities(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u14_level_profile.sqlite3")
	session := f353YAdminSession(t, database, "f354u14_level_profile")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	entityID := f353Z8SaveEntity(t, database, "f354u14_level_"+suffix, "Parroquia perfil F35-4U14 "+suffix, "parroquia", "publicat")

	body := f353YGet(t, app.AdminConfessionalLevelShow, "/confessional/nivells/parroquia", session)
	for _, want := range []string{
		"Parroquia",
		"parroquia",
		"/confessional/entitats/" + strconv.Itoa(entityID),
		"Entitats d&#39;aquest nivell",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("perfil de nivell sense %q; body=%s", want, body)
		}
	}
}

func TestF354U14ConfessionalEntityProfileShowsHierarchyAndPrimaryMunicipality(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u14_entity_profile.sqlite3")
	session := f353YAdminSession(t, database, "f354u14_entity_profile")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	parentID := f353Z8SaveEntity(t, database, "f354u14_parent_"+suffix, "Arxiprestat perfil F35-4U14 "+suffix, "arxiprestat_vicariat_forani", "publicat")
	childID := f353Z8SaveEntity(t, database, "f354u14_child_"+suffix, "Parroquia perfil F35-4U14 "+suffix, "parroquia", "publicat")
	grandChildID := f353Z8SaveEntity(t, database, "f354u14_grand_"+suffix, "Lloc de culte perfil F35-4U14 "+suffix, "lloc_de_culte", "publicat")
	if _, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{EntitatOrigenID: parentID, EntitatDestiID: childID, TipusRelacio: "parroquia", ModeracioEstat: "publicat"}); err != nil {
		t.Fatalf("SaveEntitatReligiosaRelacio parent: %v", err)
	}
	if _, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{EntitatOrigenID: childID, EntitatDestiID: grandChildID, TipusRelacio: "lloc_de_culte", ModeracioEstat: "publicat"}); err != nil {
		t.Fatalf("SaveEntitatReligiosaRelacio child: %v", err)
	}
	municipiName := "Municipi perfil F35-4U14 " + suffix
	municipiID := f353YCreateMunicipi(t, database, municipiName)
	if _, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiID,
		EntitatReligiosaID: childID,
		TipusRelacio:       "parroquia",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa: %v", err)
	}

	body := f353YGet(t, app.AdminConfessionalEntityShow, "/confessional/entitats/"+strconv.Itoa(childID), session)
	for _, want := range []string{
		"/confessional/nivells/parroquia",
		"/confessional/entitats/" + strconv.Itoa(parentID),
		"/confessional/entitats/" + strconv.Itoa(grandChildID),
		"/territori/municipis/" + strconv.Itoa(municipiID),
		municipiName,
		"Entitats superiors",
		"Entitats inferiors",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("perfil d'entitat sense %q; body=%s", want, body)
		}
	}
}

func TestF354U14NucliRelationDoesNotOverridePrimaryMunicipality(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u14_nucli.sqlite3")
	session := f353YAdminSession(t, database, "f354u14_nucli")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	municipiName := "Municipi principal F35-4U14 " + suffix
	municipiID := f353YCreateMunicipi(t, database, municipiName)
	nucliName := "Nucli F35-4U14 " + suffix
	nucliID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            nucliName,
		Tipus:          "nucli",
		Estat:          "actiu",
		MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi nucli: %v", err)
	}
	entityID := f353Z8SaveEntity(t, database, "f354u14_nucli_"+suffix, "Parroquia nucli F35-4U14 "+suffix, "parroquia", "publicat")
	if _, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiID,
		EntitatReligiosaID: entityID,
		TipusRelacio:       "parroquia",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("Save primary municipi: %v", err)
	}
	if _, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiID,
		NucliID:            sql.NullInt64{Int64: int64(nucliID), Valid: true},
		EntitatReligiosaID: entityID,
		TipusRelacio:       "altres",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("Save nucli relation: %v", err)
	}

	body := f353YGet(t, app.AdminConfessionalEntityShow, "/confessional/entitats/"+strconv.Itoa(entityID), session)
	if !strings.Contains(body, `Municipi principal</strong><a href="/territori/municipis/`+strconv.Itoa(municipiID)) {
		t.Fatalf("la capcalera ha de mostrar el municipi principal real; body=%s", body)
	}
	if !strings.Contains(body, nucliName) {
		t.Fatalf("la relacio amb nucli ha de continuar visible; body=%s", body)
	}
}

func TestF354U14ConfessionalProfilesReturn404ForMissingRecords(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u14_missing.sqlite3")
	session := f353YAdminSession(t, database, "f354u14_missing")

	req := httptest.NewRequest(http.MethodGet, "/confessional/nivells/no-existeix", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminConfessionalLevelShow(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("nivell inexistent status=%d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/confessional/entitats/999999", nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminConfessionalEntityShow(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("entitat inexistent status=%d body=%s", rr.Code, rr.Body.String())
	}
	_ = database
}
