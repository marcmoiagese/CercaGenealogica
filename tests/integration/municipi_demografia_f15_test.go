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

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createDemografiaRegistre(t *testing.T, database db.DB, llibreID, paginaID, userID int, tipus string, any int, status string) int {
	t.Helper()

	registre := &db.TranscripcioRaw{
		LlibreID:       llibreID,
		PaginaID:       sql.NullInt64{Int64: int64(paginaID), Valid: true},
		TipusActe:      tipus,
		DataActeEstat:  "clar",
		ModeracioEstat: status,
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	if any > 0 {
		registre.AnyDoc = sql.NullInt64{Int64: int64(any), Valid: true}
	}
	registreID, err := database.CreateTranscripcioRaw(registre)
	if err != nil {
		t.Fatalf("CreateTranscripcioRaw ha fallat: %v", err)
	}
	return registreID
}

func moderateObject(t *testing.T, app interface {
	AdminModeracioAprovar(http.ResponseWriter, *http.Request)
	AdminModeracioRebutjar(http.ResponseWriter, *http.Request)
}, sessionID, objectType string, objectID int, action string) {
	t.Helper()

	csrfToken := "csrf_f15_" + strconv.Itoa(objectID) + "_" + action
	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("object_type", objectType)
	if action == "rebutjar" {
		form.Set("motiu", "test")
	}
	path := "/moderacio/" + strconv.Itoa(objectID) + "/" + action
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrfToken})
	rr := httptest.NewRecorder()

	if action == "rebutjar" {
		app.AdminModeracioRebutjar(rr, req)
	} else {
		app.AdminModeracioAprovar(rr, req)
	}
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("moderacio %s status inesperat: %d", action, rr.Code)
	}
}

func TestDemografiaModerationDelta(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f15_demografia_delta.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)

	llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)
	llibre, err := database.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		t.Fatalf("GetLlibre ha fallat: %v", err)
	}
	munID := llibre.MunicipiID

	registreID := createDemografiaRegistre(t, database, llibreID, paginaID, user.ID, "baptisme", 1900, "pendent")
	moderateObject(t, app, sessionID, "registre", registreID, "aprovar")

	meta, err := database.GetMunicipiDemografiaMeta(munID)
	if err != nil || meta == nil {
		t.Fatalf("GetMunicipiDemografiaMeta ha fallat: %v", err)
	}
	if meta.TotalNatalitat != 1 {
		t.Fatalf("esperava natalitat 1, got %d", meta.TotalNatalitat)
	}

	moderateObject(t, app, sessionID, "registre", registreID, "rebutjar")
	meta, err = database.GetMunicipiDemografiaMeta(munID)
	if err != nil || meta == nil {
		t.Fatalf("GetMunicipiDemografiaMeta rebutjar ha fallat: %v", err)
	}
	if meta.TotalNatalitat != 0 {
		t.Fatalf("esperava natalitat 0 després rebutjar, got %d", meta.TotalNatalitat)
	}

	moderateObject(t, app, sessionID, "registre", registreID, "aprovar")
	registre, err := database.GetTranscripcioRaw(registreID)
	if err != nil || registre == nil {
		t.Fatalf("GetTranscripcioRaw ha fallat: %v", err)
	}
	before := *registre
	after := *registre
	after.TipusActe = "matrimoni"
	after.AnyDoc = sql.NullInt64{Int64: 1910, Valid: true}
	after.ModeracioEstat = "pendent"
	metaPayload := map[string]interface{}{
		"before": map[string]interface{}{
			"raw":      before,
			"persones": []db.TranscripcioPersonaRaw{},
			"atributs": []db.TranscripcioAtributRaw{},
		},
		"after": map[string]interface{}{
			"raw":      after,
			"persones": []db.TranscripcioPersonaRaw{},
			"atributs": []db.TranscripcioAtributRaw{},
		},
	}
	metaJSON, _ := json.Marshal(metaPayload)
	changeID, err := database.CreateTranscripcioRawChange(&db.TranscripcioRawChange{
		TranscripcioID: registreID,
		ChangeType:     "form",
		FieldKey:       "bulk",
		Metadata:       string(metaJSON),
		ModeracioEstat: "pendent",
		ChangedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
	})
	if err != nil || changeID == 0 {
		t.Fatalf("CreateTranscripcioRawChange ha fallat: %v", err)
	}
	moderateObject(t, app, sessionID, "registre_canvi", changeID, "aprovar")

	rows, err := database.ListMunicipiDemografiaAny(munID, 0, 0)
	if err != nil {
		t.Fatalf("ListMunicipiDemografiaAny ha fallat: %v", err)
	}
	found1900 := false
	found1910 := false
	for _, row := range rows {
		switch row.Any {
		case 1900:
			found1900 = true
			if row.Natalitat != 0 {
				t.Fatalf("1900 natalitat esperada 0, got %d", row.Natalitat)
			}
		case 1910:
			found1910 = true
			if row.Matrimonis != 1 {
				t.Fatalf("1910 matrimonis esperat 1, got %d", row.Matrimonis)
			}
		}
	}
	if found1900 {
		t.Fatalf("no s'hauria d'haver mantingut 1900 després del canvi")
	}
	if !found1910 {
		t.Fatalf("no s'ha trobat la fila 1910 després del canvi")
	}
}

func TestDemografiaRebuildFiltersInvalid(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f15_demografia_rebuild.sqlite3")
	_ = app

	user, _ := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)

	llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)
	llibre, err := database.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		t.Fatalf("GetLlibre ha fallat: %v", err)
	}
	munID := llibre.MunicipiID

	createDemografiaRegistre(t, database, llibreID, paginaID, user.ID, "baptisme", 1900, "publicat")
	createDemografiaRegistre(t, database, llibreID, paginaID, user.ID, "obit", 1890, "publicat")
	createDemografiaRegistre(t, database, llibreID, paginaID, user.ID, "altres", 1900, "publicat")
	createDemografiaRegistre(t, database, llibreID, paginaID, user.ID, "baptisme", 1100, "publicat")

	if err := database.RebuildMunicipiDemografia(munID); err != nil {
		t.Fatalf("RebuildMunicipiDemografia ha fallat: %v", err)
	}
	meta, err := database.GetMunicipiDemografiaMeta(munID)
	if err != nil || meta == nil {
		t.Fatalf("GetMunicipiDemografiaMeta ha fallat: %v", err)
	}
	if meta.TotalNatalitat != 1 || meta.TotalDefuncions != 1 || meta.TotalMatrimonis != 0 {
		t.Fatalf("totals inesperats: nat %d mat %d def %d", meta.TotalNatalitat, meta.TotalMatrimonis, meta.TotalDefuncions)
	}
	if !meta.AnyMin.Valid || !meta.AnyMax.Valid || int(meta.AnyMin.Int64) != 1890 || int(meta.AnyMax.Int64) != 1900 {
		t.Fatalf("rang inesperat: %+v", meta)
	}
	rows, err := database.ListMunicipiDemografiaAny(munID, 0, 0)
	if err != nil {
		t.Fatalf("ListMunicipiDemografiaAny ha fallat: %v", err)
	}
	for _, row := range rows {
		if row.Any == 1100 {
			t.Fatalf("no hauria d'incloure any 1100")
		}
		if row.Any == 1900 && row.Natalitat != 1 {
			t.Fatalf("natalitat 1900 esperada 1, got %d", row.Natalitat)
		}
		if row.Any == 1890 && row.Defuncions != 1 {
			t.Fatalf("defuncions 1890 esperada 1, got %d", row.Defuncions)
		}
	}
}
