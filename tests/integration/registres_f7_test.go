package integration

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createF7UserWithSession(t *testing.T, database db.DB) (*db.User, string) {
	t.Helper()

	user := &db.User{
		Usuari:        fmt.Sprintf("f7_user_%d", time.Now().UnixNano()),
		Name:          "F7",
		Surname:       "Tester",
		Email:         fmt.Sprintf("f7_%d@example.com", time.Now().UnixNano()),
		Password:      []byte("hash"),
		DataNaixament: "1990-01-01",
		Active:        true,
		CreatedAt:     time.Now().Format(time.RFC3339),
	}
	if err := database.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}
	sessionID := fmt.Sprintf("sess_f7_%d", time.Now().UnixNano())
	expiry := time.Now().Add(2 * time.Hour).Format(time.RFC3339)
	if err := database.SaveSession(sessionID, user.ID, expiry); err != nil {
		t.Fatalf("SaveSession ha fallat: %v", err)
	}
	return user, sessionID
}

func createF7LlibreWithPagina(t *testing.T, database db.DB, userID int) (int, int) {
	t.Helper()

	mun := &db.Municipi{
		Nom:            "Municipi Test",
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	munID, err := database.CreateMunicipi(mun)
	if err != nil {
		t.Fatalf("CreateMunicipi ha fallat: %v", err)
	}
	arch := &db.Arquebisbat{
		Nom:            fmt.Sprintf("Bisbat Test %d", time.Now().UnixNano()),
		TipusEntitat:   "bisbat",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	archID, err := database.CreateArquebisbat(arch)
	if err != nil {
		t.Fatalf("CreateArquebisbat ha fallat: %v", err)
	}
	llibre := &db.Llibre{
		ArquebisbatID:  archID,
		MunicipiID:     munID,
		Titol:          "Llibre Test",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	llibreID, err := database.CreateLlibre(llibre)
	if err != nil {
		t.Fatalf("CreateLlibre ha fallat: %v", err)
	}
	pagina := &db.LlibrePagina{
		LlibreID:  llibreID,
		NumPagina: 1,
		Estat:     "indexada",
	}
	paginaID, err := database.SaveLlibrePagina(pagina)
	if err != nil {
		t.Fatalf("SaveLlibrePagina ha fallat: %v", err)
	}
	return llibreID, paginaID
}

func TestAdminExportRegistresCSVIncludesDerivedColumns(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f7_export.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)
	llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)

	registre := &db.TranscripcioRaw{
		LlibreID:       llibreID,
		PaginaID:       sql.NullInt64{Int64: int64(paginaID), Valid: true},
		TipusActe:      "baptisme",
		DataActeEstat:  "clar",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
	}
	registreID, err := database.CreateTranscripcioRaw(registre)
	if err != nil {
		t.Fatalf("CreateTranscripcioRaw ha fallat: %v", err)
	}
	_, _ = database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: registreID,
		Rol:            "pare",
		Nom:            "Joan",
		Cognom1:        "Garcia",
	})
	_, _ = database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: registreID,
		Rol:            "mare",
		Nom:            "Maria",
		Cognom1:        "Soler",
	})

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/documentals/llibres/%d/export", llibreID), nil)
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	rr := httptest.NewRecorder()

	app.AdminExportRegistresLlibre(rr, req)

	if rr.Result().StatusCode != http.StatusOK {
		t.Fatalf("Status inesperat: %d", rr.Result().StatusCode)
	}
	reader := csv.NewReader(strings.NewReader(rr.Body.String()))
	header, err := reader.Read()
	if err != nil {
		t.Fatalf("no puc llegir capçalera CSV: %v", err)
	}
	row, err := reader.Read()
	if err != nil {
		t.Fatalf("no puc llegir fila CSV: %v", err)
	}
	index := map[string]int{}
	for i, h := range header {
		index[h] = i
	}
	for _, key := range []string{"pare", "mare", "testimonis"} {
		if _, ok := index[key]; !ok {
			t.Fatalf("capçalera CSV no conté %q", key)
		}
	}
	if got := row[index["pare"]]; got != "Joan Garcia" {
		t.Fatalf("valor pare inesperat: %q", got)
	}
	if got := row[index["mare"]]; got != "Maria Soler" {
		t.Fatalf("valor mare inesperat: %q", got)
	}
}

func TestAdminImportRegistresCSVCreatesRows(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f7_import.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)
	llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)

	csvContent := strings.Join([]string{
		"pagina_id,tipus_acte,any_doc,data_acte_estat,person:batejat:nom,person:batejat:cognom1,attr:ofici:text",
		fmt.Sprintf("%d,baptisme,1880,clar,Pere,Casa,forner", paginaID),
	}, "\n")

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	filePart, err := writer.CreateFormFile("csv_file", "import.csv")
	if err != nil {
		t.Fatalf("CreateFormFile ha fallat: %v", err)
	}
	if _, err := filePart.Write([]byte(csvContent)); err != nil {
		t.Fatalf("escriptura CSV ha fallat: %v", err)
	}
	if err := writer.WriteField("separator", ","); err != nil {
		t.Fatalf("WriteField separator ha fallat: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close multipart ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/documentals/llibres/%d/import", llibreID), &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	rr := httptest.NewRecorder()

	app.AdminImportRegistresLlibre(rr, req)

	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("Status inesperat: %d", rr.Result().StatusCode)
	}

	registres, err := database.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{Limit: -1})
	if err != nil {
		t.Fatalf("ListTranscripcionsRaw ha fallat: %v", err)
	}
	if len(registres) != 1 {
		t.Fatalf("esperava 1 registre, però n'he trobat %d", len(registres))
	}
	persones, err := database.ListTranscripcioPersones(registres[0].ID)
	if err != nil {
		t.Fatalf("ListTranscripcioPersones ha fallat: %v", err)
	}
	if len(persones) == 0 || persones[0].Nom != "Pere" {
		t.Fatalf("persona importada inesperada: %+v", persones)
	}
	atributs, err := database.ListTranscripcioAtributs(registres[0].ID)
	if err != nil {
		t.Fatalf("ListTranscripcioAtributs ha fallat: %v", err)
	}
	found := false
	for _, a := range atributs {
		if a.Clau == "ofici" && a.ValorText == "forner" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("atribut ofici no importat correctament: %+v", atributs)
	}
}

func TestListTranscripcionsRawSearchFiltersByPersonName(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_f7_search.sqlite3")

	user, _ := createF7UserWithSession(t, database)
	llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)

	registre1 := &db.TranscripcioRaw{
		LlibreID:       llibreID,
		PaginaID:       sql.NullInt64{Int64: int64(paginaID), Valid: true},
		TipusActe:      "baptisme",
		DataActeEstat:  "clar",
		ModeracioEstat: "publicat",
	}
	id1, err := database.CreateTranscripcioRaw(registre1)
	if err != nil {
		t.Fatalf("CreateTranscripcioRaw #1 ha fallat: %v", err)
	}
	_, _ = database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: id1,
		Rol:            "testimoni",
		Nom:            "Joan",
		Cognom1:        "Garcia",
	})

	registre2 := &db.TranscripcioRaw{
		LlibreID:       llibreID,
		PaginaID:       sql.NullInt64{Int64: int64(paginaID), Valid: true},
		TipusActe:      "obit",
		DataActeEstat:  "clar",
		ModeracioEstat: "publicat",
	}
	id2, err := database.CreateTranscripcioRaw(registre2)
	if err != nil {
		t.Fatalf("CreateTranscripcioRaw #2 ha fallat: %v", err)
	}
	_, _ = database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: id2,
		Rol:            "testimoni",
		Nom:            "Pere",
		Cognom1:        "Soler",
	})

	filter := db.TranscripcioFilter{
		Search: "Garcia",
		Limit:  -1,
	}
	results, err := database.ListTranscripcionsRaw(llibreID, filter)
	if err != nil {
		t.Fatalf("ListTranscripcionsRaw ha fallat: %v", err)
	}
	if len(results) != 1 || results[0].ID != id1 {
		t.Fatalf("resultat inesperat de cerca: %+v", results)
	}
}
