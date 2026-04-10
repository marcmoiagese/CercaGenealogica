package integration

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

type searchDocFunctionalProjectionF314 struct {
	Published             bool
	MunicipiID            sql.NullInt64
	ArxiuID               sql.NullInt64
	LlibreID              sql.NullInt64
	EntitatEclesiasticaID sql.NullInt64
	DataActe              sql.NullString
	AnyActe               sql.NullInt64
	PersonNomNorm         string
	PersonCognomsNorm     string
	PersonFullNorm        string
	PersonTokensNorm      string
	CognomsTokensNorm     string
	PersonPhonetic        string
	CognomsPhonetic       string
	CognomsCanon          string
}

func projectSearchDocFunctionalF314(doc *db.SearchDoc) searchDocFunctionalProjectionF314 {
	if doc == nil {
		return searchDocFunctionalProjectionF314{}
	}
	return searchDocFunctionalProjectionF314{
		Published:             doc.Published,
		MunicipiID:            doc.MunicipiID,
		ArxiuID:               doc.ArxiuID,
		LlibreID:              doc.LlibreID,
		EntitatEclesiasticaID: doc.EntitatEclesiasticaID,
		DataActe:              doc.DataActe,
		AnyActe:               doc.AnyActe,
		PersonNomNorm:         doc.PersonNomNorm,
		PersonCognomsNorm:     doc.PersonCognomsNorm,
		PersonFullNorm:        doc.PersonFullNorm,
		PersonTokensNorm:      doc.PersonTokensNorm,
		CognomsTokensNorm:     doc.CognomsTokensNorm,
		PersonPhonetic:        doc.PersonPhonetic,
		CognomsPhonetic:       doc.CognomsPhonetic,
		CognomsCanon:          doc.CognomsCanon,
	}
}

func createF314RegistreWithPeople(t *testing.T, database db.DB, llibreID, paginaID, userID int, status string) int {
	t.Helper()
	registreID := createDemografiaRegistre(t, database, llibreID, paginaID, userID, "baptisme", 1902, status)
	people := []db.TranscripcioPersonaRaw{
		{TranscripcioID: registreID, Rol: "batejat", Nom: "Àngela", Cognom1: "Pujol-Soler", Cognom2: "d'Oliva"},
		{TranscripcioID: registreID, Rol: "pare", Nom: "Josep", Cognom1: "Pujol-Soler"},
	}
	for _, person := range people {
		if _, err := database.CreateTranscripcioPersona(&person); err != nil {
			t.Fatalf("CreateTranscripcioPersona ha fallat: %v", err)
		}
	}
	return registreID
}

func submitAsyncBulkJobByTypeActionF311(t *testing.T, app interface {
	AdminModeracioBulk(http.ResponseWriter, *http.Request)
}, session *http.Cookie, csrf, bulkType, action string) int {
	t.Helper()

	form := newFormValues(map[string]string{
		"bulk_action": action,
		"bulk_scope":  "all",
		"bulk_type":   bulkType,
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
		t.Fatalf("bulk async %s/%s esperava 200, got %d", bulkType, action, rr.Code)
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

func collectNomTotals(rows []db.NomTotalRow) map[string]int {
	out := make(map[string]int, len(rows))
	for _, row := range rows {
		out[row.Forma] = row.TotalFreq
	}
	return out
}

func collectCognomTotals(rows []db.CognomTotalRow) map[string]int {
	out := make(map[string]int, len(rows))
	for _, row := range rows {
		out[row.Forma] = row.TotalFreq
	}
	return out
}

func TestModeracioBulkAsyncRegistreAggregatesDerivedSideEffectsF311(t *testing.T) {
	forEachModeracioBulkHistoryDB(t, func(t *testing.T, label string, app *core.App, database db.DB, engine string) {
		admin, _ := createF7UserWithSession(t, database)
		ensureAdminPolicyForUser(t, database, admin.ID)
		session := createSessionCookie(t, database, admin.ID, "sess_f31_1_aggregate_"+label+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))

		llibreID, paginaID := createF7LlibreWithPagina(t, database, admin.ID)
		llibre, err := database.GetLlibre(llibreID)
		if err != nil || llibre == nil {
			t.Fatalf("GetLlibre ha fallat: %v", err)
		}

		type personSeed struct {
			Year    int
			Nom     string
			Cognom1 string
		}
		seeds := []personSeed{
			{Year: 1900, Nom: "Joan", Cognom1: "Pujol"},
			{Year: 1900, Nom: "Joan", Cognom1: "Pujol"},
			{Year: 1900, Nom: "Joan", Cognom1: "Pujol"},
			{Year: 1901, Nom: "Maria", Cognom1: "Pujol"},
		}
		registreIDs := make([]int, 0, len(seeds))
		for _, seed := range seeds {
			registreID := createDemografiaRegistre(t, database, llibreID, paginaID, admin.ID, "baptisme", seed.Year, "pendent")
			registreIDs = append(registreIDs, registreID)
			if _, err := database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
				TranscripcioID: registreID,
				Rol:            "batejat",
				Nom:            seed.Nom,
				Cognom1:        seed.Cognom1,
			}); err != nil {
				t.Fatalf("CreateTranscripcioPersona ha fallat: %v", err)
			}
		}

		jobID := submitAsyncBulkJobByTypeActionF311(t, app, session, "csrf_f31_1_aggregate_"+label, "registre", "approve")
		job := waitForAdminJobTerminal(t, database, jobID)
		if job.Status != "done" {
			t.Fatalf("job esperat done, got status=%s phase=%s error=%s", job.Status, job.Phase, job.ErrorText)
		}

		meta, err := database.GetMunicipiDemografiaMeta(llibre.MunicipiID)
		if err != nil || meta == nil {
			t.Fatalf("GetMunicipiDemografiaMeta ha fallat: %v", err)
		}
		if meta.TotalNatalitat != len(seeds) {
			t.Fatalf("natalitat esperada %d, got %d", len(seeds), meta.TotalNatalitat)
		}

		noms, err := database.ListTopNomsByMunicipi(llibre.MunicipiID, 10)
		if err != nil {
			t.Fatalf("ListTopNomsByMunicipi ha fallat: %v", err)
		}
		nomTotals := collectNomTotals(noms)
		if nomTotals["Joan"] != 3 || nomTotals["Maria"] != 1 {
			t.Fatalf("stats noms inesperades: %+v", noms)
		}

		cognoms, err := database.ListTopCognomsByMunicipi(llibre.MunicipiID, 10)
		if err != nil {
			t.Fatalf("ListTopCognomsByMunicipi ha fallat: %v", err)
		}
		cognomTotals := collectCognomTotals(cognoms)
		if cognomTotals["Pujol"] != len(seeds) {
			t.Fatalf("stats cognoms inesperades: %+v", cognoms)
		}

		for _, registreID := range registreIDs {
			doc, err := database.GetSearchDoc("registre_raw", registreID)
			if err != nil || doc == nil {
				t.Fatalf("GetSearchDoc registre %d ha fallat: %v", registreID, err)
			}
		}
	})
}

func TestModeracioBulkRegistreSearchDocsEquivalentToIndividualF314(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f31_4_search_docs_equiv.sqlite3")

	admin, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, admin.ID)
	bulkSession := &http.Cookie{Name: "cg_session", Value: sessionID, Path: "/"}

	llibreID, paginaID := createF7LlibreWithPagina(t, database, admin.ID)
	individualID := createF314RegistreWithPeople(t, database, llibreID, paginaID, admin.ID, "pendent")
	bulkID := createF314RegistreWithPeople(t, database, llibreID, paginaID, admin.ID, "pendent")

	moderateObject(t, app, sessionID, "registre", individualID, "aprovar")
	individualDoc, err := database.GetSearchDoc("registre_raw", individualID)
	if err != nil || individualDoc == nil {
		t.Fatalf("search_doc individual no creat: %v", err)
	}

	jobID := submitAsyncBulkJobByTypeActionF311(t, app, bulkSession, "csrf_f31_4_equiv", "registre", "approve")
	job := waitForAdminJobTerminal(t, database, jobID)
	if job.Status != "done" {
		t.Fatalf("job bulk esperat done, got status=%s phase=%s error=%s", job.Status, job.Phase, job.ErrorText)
	}
	bulkDoc, err := database.GetSearchDoc("registre_raw", bulkID)
	if err != nil || bulkDoc == nil {
		t.Fatalf("search_doc bulk no creat: %v", err)
	}

	individualProjection := projectSearchDocFunctionalF314(individualDoc)
	bulkProjection := projectSearchDocFunctionalF314(bulkDoc)
	if individualProjection != bulkProjection {
		t.Fatalf("search_doc bulk difereix de l'individual:\nindividual=%+v\nbulk=%+v", individualProjection, bulkProjection)
	}

	moderateObject(t, app, sessionID, "registre", bulkID, "rebutjar")
	if doc, err := database.GetSearchDoc("registre_raw", bulkID); err == nil && doc != nil {
		t.Fatalf("search_doc bulk %d no s'ha eliminat en rebutjar-lo: %+v", bulkID, doc)
	}

	rejectedPendingID := createF314RegistreWithPeople(t, database, llibreID, paginaID, admin.ID, "pendent")
	jobID = submitAsyncBulkJobByTypeActionF311(t, app, bulkSession, "csrf_f31_4_reject", "registre", "reject")
	job = waitForAdminJobTerminal(t, database, jobID)
	if job.Status != "done" {
		t.Fatalf("job bulk reject esperat done, got status=%s phase=%s error=%s", job.Status, job.Phase, job.ErrorText)
	}
	if doc, err := database.GetSearchDoc("registre_raw", rejectedPendingID); err == nil && doc != nil {
		t.Fatalf("bulk reject d'un pendent no hauria de crear search_doc: %+v", doc)
	}
}
