package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func submitAsyncRegistreBulkJob(t *testing.T, app interface {
	AdminModeracioBulk(http.ResponseWriter, *http.Request)
}, session *http.Cookie, csrf string) int {
	t.Helper()

	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "registre",
		"csrf_token":  csrf,
		"return_to":   "/moderacio",
		"async":       "1",
	})
	req := httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("bulk async registre esperava 200, got %d", rr.Code)
	}
	var payload struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("response JSON invàlid: %v", err)
	}
	jobID, err := strconv.Atoi(payload.JobID)
	if err != nil || jobID <= 0 {
		t.Fatalf("job_id invàlid: %q err=%v", payload.JobID, err)
	}
	return jobID
}

func TestModeracioBulkAsyncRegistrePreservesDerivedData(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_13_registre_derived.sqlite3")

	admin, _ := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, admin.ID)
	session := createSessionCookie(t, database, admin.ID, "sess_f30_13_registre_derived")

	llibreID, paginaID := createF7LlibreWithPagina(t, database, admin.ID)
	llibre, err := database.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		t.Fatalf("GetLlibre ha fallat: %v", err)
	}
	munID := llibre.MunicipiID

	reg1 := createDemografiaRegistre(t, database, llibreID, paginaID, admin.ID, "baptisme", 1900, "pendent")
	reg2 := createDemografiaRegistre(t, database, llibreID, paginaID, admin.ID, "baptisme", 1900, "pendent")
	for _, regID := range []int{reg1, reg2} {
		if _, err := database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
			TranscripcioID: regID,
			Rol:            "batejat",
			Nom:            "Joan",
			Cognom1:        "Pujol",
		}); err != nil {
			t.Fatalf("CreateTranscripcioPersona ha fallat: %v", err)
		}
	}

	jobID := submitAsyncRegistreBulkJob(t, app, session, "csrf_f30_13_registre_derived")
	job := waitForAdminJobTerminal(t, database, jobID)
	if job.Status != "done" {
		t.Fatalf("job esperat done, got status=%s phase=%s error=%s", job.Status, job.Phase, job.ErrorText)
	}

	meta, err := database.GetMunicipiDemografiaMeta(munID)
	if err != nil || meta == nil {
		t.Fatalf("GetMunicipiDemografiaMeta ha fallat: %v", err)
	}
	if meta.TotalNatalitat != 2 {
		t.Fatalf("natalitat esperada 2, got %d", meta.TotalNatalitat)
	}

	noms, err := database.ListTopNomsByMunicipi(munID, 10)
	if err != nil {
		t.Fatalf("ListTopNomsByMunicipi ha fallat: %v", err)
	}
	if len(noms) == 0 || noms[0].Forma != "Joan" || noms[0].TotalFreq != 2 {
		t.Fatalf("stats noms inesperades: %+v", noms)
	}

	doc, err := database.GetSearchDoc("registre_raw", reg1)
	if err != nil || doc == nil {
		t.Fatalf("GetSearchDoc registre ha fallat: %v", err)
	}
	if !doc.LlibreID.Valid || int(doc.LlibreID.Int64) != llibreID {
		t.Fatalf("search_doc llibre_id inesperat: %+v", doc.LlibreID)
	}
	if !doc.MunicipiID.Valid || int(doc.MunicipiID.Int64) != munID {
		t.Fatalf("search_doc municipi_id inesperat: %+v", doc.MunicipiID)
	}
}

func TestModeracioBulkAsyncRegistreLargeChunkedCompletes(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_13_registre_chunked.sqlite3")

	admin, _ := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, admin.ID)
	session := createSessionCookie(t, database, admin.ID, "sess_f30_13_registre_chunked")

	llibreID, paginaID := createF7LlibreWithPagina(t, database, admin.ID)
	llibre, err := database.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		t.Fatalf("GetLlibre ha fallat: %v", err)
	}

	const totalRegistres = 520
	ids := make([]int, 0, totalRegistres)
	for i := 0; i < totalRegistres; i++ {
		ids = append(ids, createDemografiaRegistre(t, database, llibreID, paginaID, admin.ID, "baptisme", 1901, "pendent"))
	}

	jobID := submitAsyncRegistreBulkJob(t, app, session, "csrf_f30_13_registre_chunked")
	job := waitForAdminJobTerminal(t, database, jobID)
	if job.Status != "done" {
		t.Fatalf("job chunked esperat done, got status=%s phase=%s error=%s", job.Status, job.Phase, job.ErrorText)
	}

	var result struct {
		Targets int `json:"targets"`
		Updated int `json:"updated"`
		Errors  int `json:"errors"`
	}
	if err := json.Unmarshal([]byte(job.ResultJSON), &result); err != nil {
		t.Fatalf("result_json invàlid: %v", err)
	}
	if result.Targets != totalRegistres || result.Updated != totalRegistres || result.Errors != 0 {
		t.Fatalf("result chunked inesperat: %+v", result)
	}

	meta, err := database.GetMunicipiDemografiaMeta(llibre.MunicipiID)
	if err != nil || meta == nil {
		t.Fatalf("GetMunicipiDemografiaMeta chunked ha fallat: %v", err)
	}
	if meta.TotalNatalitat != totalRegistres {
		t.Fatalf("natalitat chunked esperada %d, got %d", totalRegistres, meta.TotalNatalitat)
	}

	first, err := database.GetTranscripcioRaw(ids[0])
	if err != nil || first == nil {
		t.Fatalf("GetTranscripcioRaw first ha fallat: %v", err)
	}
	last, err := database.GetTranscripcioRaw(ids[len(ids)-1])
	if err != nil || last == nil {
		t.Fatalf("GetTranscripcioRaw last ha fallat: %v", err)
	}
	if first.ModeracioEstat != "publicat" || last.ModeracioEstat != "publicat" {
		t.Fatalf("registres chunked no publicats: first=%s last=%s", first.ModeracioEstat, last.ModeracioEstat)
	}
}
