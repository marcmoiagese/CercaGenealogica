package integration

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createBrowseTestCountry(t *testing.T, database db.DB, iso string) int {
	t.Helper()
	p := &db.Pais{CodiISO2: iso, CodiISO3: iso + "3", CodiPaisNum: strconv.Itoa(len(iso) + int(time.Now().UnixNano()%1000))}
	id, err := database.CreatePais(p)
	if err != nil {
		t.Fatalf("CreatePais %s ha fallat: %v", iso, err)
	}
	return id
}

func createBrowseTestLevel(t *testing.T, database db.DB, paisID, nivel int, nom, tipus string, parentID int) int {
	t.Helper()
	n := &db.NivellAdministratiu{
		PaisID:         paisID,
		Nivel:          nivel,
		NomNivell:      nom,
		TipusNivell:    tipus,
		CodiOficial:    fmt.Sprintf("%s-%d", strings.ToUpper(tipus), nivel),
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	}
	if parentID > 0 {
		n.ParentID = sql.NullInt64{Int64: int64(parentID), Valid: true}
	}
	id, err := database.CreateNivell(n)
	if err != nil {
		t.Fatalf("CreateNivell %s ha fallat: %v", nom, err)
	}
	return id
}

func createBrowseTestMunicipi(t *testing.T, database db.DB, createdBy int, nom string, levelIDs [7]int) int {
	t.Helper()
	return createBrowseTestMunicipiWithOptions(t, database, createdBy, nom, "municipi", "publicat", levelIDs)
}

func createBrowseTestMunicipiWithOptions(t *testing.T, database db.DB, createdBy int, nom, tipus, status string, levelIDs [7]int) int {
	t.Helper()
	m := &db.Municipi{
		Nom:            nom,
		Tipus:          tipus,
		Estat:          "actiu",
		ModeracioEstat: status,
		CreatedBy:      sql.NullInt64{Int64: int64(createdBy), Valid: createdBy > 0},
	}
	for i, id := range levelIDs {
		if id > 0 {
			m.NivellAdministratiuID[i] = sql.NullInt64{Int64: int64(id), Valid: true}
		}
	}
	id, err := database.CreateMunicipi(m)
	if err != nil {
		t.Fatalf("CreateMunicipi %s ha fallat: %v", nom, err)
	}
	return id
}

func createScopedPolicyWithGrant(t *testing.T, database db.DB, policyName, permKey string, scopeType core.ScopeType, scopeID int, includeChildren bool) int {
	t.Helper()
	pol := &db.Politica{Nom: policyName, Permisos: "{}"}
	policyID, err := database.SavePolitica(pol)
	if err != nil {
		t.Fatalf("SavePolitica %s ha fallat: %v", policyName, err)
	}
	grant := &db.PoliticaGrant{
		PoliticaID:      policyID,
		PermKey:         permKey,
		ScopeType:       string(scopeType),
		IncludeChildren: includeChildren,
		ScopeID:         sql.NullInt64{Int64: int64(scopeID), Valid: scopeID > 0},
	}
	if _, err := database.SavePoliticaGrant(grant); err != nil {
		t.Fatalf("SavePoliticaGrant %s ha fallat: %v", permKey, err)
	}
	return policyID
}

func TestMunicipiBrowseFiltersByNivellIDAcrossAnyColumnF3014(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_f30_14_browse_any_column.sqlite3")

	user := createTestUser(t, database, "f30_14_browse_user")
	_ = createBrowseTestCountry(t, database, "DX")
	targetPaisID := createBrowseTestCountry(t, database, "TX")

	level1 := createBrowseTestLevel(t, database, targetPaisID, 1, "Nivell 1", "regio", 0)
	level2 := createBrowseTestLevel(t, database, targetPaisID, 2, "Nivell 2", "provincia", level1)
	level3 := createBrowseTestLevel(t, database, targetPaisID, 3, "Nivell 3", "vegueria", level2)
	level4 := createBrowseTestLevel(t, database, targetPaisID, 4, "Nivell 4", "subregio", level3)
	level5 := createBrowseTestLevel(t, database, targetPaisID, 5, "Nivell 5", "partit", level4)
	targetLevelID := createBrowseTestLevel(t, database, targetPaisID, 6, "Comarca Alfa", "comarca", level5)

	levelIDsA := [7]int{level1, level2, targetLevelID}
	levelIDsB := [7]int{level1, level2, level3, level4, targetLevelID}
	levelIDsC := [7]int{level1, level2, level3}
	createBrowseTestMunicipi(t, database, user.ID, "Municipi Alfa", levelIDsA)
	createBrowseTestMunicipi(t, database, user.ID, "Municipi Beta", levelIDsB)
	createBrowseTestMunicipi(t, database, user.ID, "Municipi Fora", levelIDsC)

	rows, err := database.ListMunicipisBrowse(db.MunicipiBrowseFilter{
		NivellID: targetLevelID,
		Status:   "publicat",
		Sort:     "nom",
		SortDir:  "asc",
	})
	if err != nil {
		t.Fatalf("ListMunicipisBrowse ha fallat: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("municipis esperats 2 per NivellID any-column, got %d", len(rows))
	}
	if rows[0].Nom != "Municipi Alfa" || rows[1].Nom != "Municipi Beta" {
		t.Fatalf("municipis inesperats per NivellID any-column: %+v", rows)
	}

	total, err := database.CountMunicipisBrowse(db.MunicipiBrowseFilter{
		NivellID: targetLevelID,
		Status:   "publicat",
	})
	if err != nil {
		t.Fatalf("CountMunicipisBrowse ha fallat: %v", err)
	}
	if total != 2 {
		t.Fatalf("count municipis esperat 2 per NivellID any-column, got %d", total)
	}

	suggestions, err := database.SuggestMunicipis(db.MunicipiBrowseFilter{
		Text:     "Municipi",
		NivellID: targetLevelID,
		Status:   "publicat",
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("SuggestMunicipis ha fallat: %v", err)
	}
	if len(suggestions) != 2 {
		t.Fatalf("suggestions esperades 2 per NivellID any-column, got %d", len(suggestions))
	}

	rowsDepth, err := database.ListMunicipisBrowse(db.MunicipiBrowseFilter{
		LevelIDs: [7]int{0, 0, targetLevelID},
		Status:   "publicat",
		Sort:     "nom",
		SortDir:  "asc",
	})
	if err != nil {
		t.Fatalf("ListMunicipisBrowse amb filtre explícit de profunditat ha fallat: %v", err)
	}
	if len(rowsDepth) != 1 || rowsDepth[0].Nom != "Municipi Alfa" {
		t.Fatalf("filtre explícit LevelIDs[2]=6 hauria de retornar només Municipi Alfa, got %+v", rowsDepth)
	}
}

func TestAdminMunicipisCountryScopedEditUsesRealPaisIDF3014(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_14_admin_permissions.sqlite3")

	editor := createTestUser(t, database, "f30_14_editor")
	session := createSessionCookie(t, database, editor.ID, "sess_f30_14_editor")

	_ = createBrowseTestCountry(t, database, "DX")
	targetPaisID := createBrowseTestCountry(t, database, "TX")
	level1 := createBrowseTestLevel(t, database, targetPaisID, 1, "Regio Alfa", "regio", 0)
	level2 := createBrowseTestLevel(t, database, targetPaisID, 2, "Provincia Alfa", "provincia", level1)
	targetLevelID := createBrowseTestLevel(t, database, targetPaisID, 3, "Comarca Alfa", "comarca", level2)

	munID := createBrowseTestMunicipi(t, database, editor.ID, "Municipi Alfa", [7]int{level1, level2, targetLevelID})
	if targetPaisID == level1 {
		t.Fatalf("el test requereix pais_id i level1ID diferents; got %d i %d", targetPaisID, level1)
	}

	viewPolicyID := createScopedPolicyWithGrant(t, database, "f30_14_view_policy", "territori.municipis.view", core.ScopePais, targetPaisID, true)
	editPolicyID := createScopedPolicyWithGrant(t, database, "f30_14_edit_policy", "territori.municipis.edit", core.ScopePais, targetPaisID, true)
	if err := database.AddUserPolitica(editor.ID, viewPolicyID); err != nil {
		t.Fatalf("AddUserPolitica view ha fallat: %v", err)
	}
	if err := database.AddUserPolitica(editor.ID, editPolicyID); err != nil {
		t.Fatalf("AddUserPolitica edit ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/municipis?pais_id=%d&nivell_id_3=%d", targetPaisID, targetLevelID), nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminListMunicipis(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminListMunicipis esperava 200, got %d", rr.Code)
	}

	body := rr.Body.String()
	editHref := fmt.Sprintf("/territori/municipis/%d/edit?", munID)
	if !strings.Contains(body, editHref) {
		t.Fatalf("esperava acció d'edició per municipi amb permís scoped de país; body no conté %q", editHref)
	}
}

func TestMunicipiBrowseAllowedComarcaUsesAnyLevelColumnF3014Emergency(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_f30_14_allowed_comarca_any_column.sqlite3")

	user := createTestUser(t, database, "f30_14_comarca_scope_user")
	targetPaisID := createBrowseTestCountry(t, database, "TC")
	level1 := createBrowseTestLevel(t, database, targetPaisID, 1, "Regio Alfa", "regio", 0)
	level2 := createBrowseTestLevel(t, database, targetPaisID, 2, "Provincia Alfa", "provincia", level1)
	targetComarcaID := createBrowseTestLevel(t, database, targetPaisID, 3, "Comarca Alfa", "comarca", level2)
	otherComarcaID := createBrowseTestLevel(t, database, targetPaisID, 3, "Comarca Beta", "comarca", level2)

	createBrowseTestMunicipi(t, database, user.ID, "Municipi Alfa", [7]int{level1, level2, targetComarcaID})
	createBrowseTestMunicipi(t, database, user.ID, "Municipi Beta", [7]int{level1, level2, targetComarcaID})
	createBrowseTestMunicipi(t, database, user.ID, "Municipi Gamma", [7]int{level1, level2, otherComarcaID})

	filter := db.MunicipiBrowseFilter{
		Status:            "publicat",
		Sort:              "nom",
		SortDir:           "asc",
		AllowedComarcaIDs: []int{targetComarcaID},
	}

	rows, err := database.ListMunicipisBrowse(filter)
	if err != nil {
		t.Fatalf("ListMunicipisBrowse amb AllowedComarcaIDs ha fallat: %v", err)
	}
	if len(rows) != 2 || rows[0].Nom != "Municipi Alfa" || rows[1].Nom != "Municipi Beta" {
		t.Fatalf("esperava Municipi Alfa/Beta amb AllowedComarcaIDs semàntic, got %+v", rows)
	}

	total, err := database.CountMunicipisBrowse(db.MunicipiBrowseFilter{
		Status:            "publicat",
		AllowedComarcaIDs: []int{targetComarcaID},
	})
	if err != nil {
		t.Fatalf("CountMunicipisBrowse amb AllowedComarcaIDs ha fallat: %v", err)
	}
	if total != 2 {
		t.Fatalf("count esperat 2 amb AllowedComarcaIDs semàntic, got %d", total)
	}

	suggestions, err := database.SuggestMunicipis(db.MunicipiBrowseFilter{
		Text:              "Municipi",
		Status:            "publicat",
		Limit:             10,
		AllowedComarcaIDs: []int{targetComarcaID},
	})
	if err != nil {
		t.Fatalf("SuggestMunicipis amb AllowedComarcaIDs ha fallat: %v", err)
	}
	if len(suggestions) != 2 {
		t.Fatalf("suggestions esperades 2 amb AllowedComarcaIDs semàntic, got %d", len(suggestions))
	}
}

func TestAdminMunicipisComarcaScopedViewAndSuggestWhenComarcaAtLevel3F3014Emergency(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_14_comarca_scope_handler.sqlite3")

	editor := createTestUser(t, database, "f30_14_comarca_editor")
	session := createSessionCookie(t, database, editor.ID, "sess_f30_14_comarca_editor")

	targetPaisID := createBrowseTestCountry(t, database, "GX")
	level1 := createBrowseTestLevel(t, database, targetPaisID, 1, "Regio Alfa", "regio", 0)
	level2 := createBrowseTestLevel(t, database, targetPaisID, 2, "Provincia Alfa", "provincia", level1)
	targetComarcaID := createBrowseTestLevel(t, database, targetPaisID, 3, "Comarca Alfa", "comarca", level2)

	createBrowseTestMunicipi(t, database, editor.ID, "Municipi Alfa", [7]int{level1, level2, targetComarcaID})

	viewPolicyID := createScopedPolicyWithGrant(t, database, "f30_14_comarca_view_policy", "territori.municipis.view", core.ScopeComarca, targetComarcaID, true)
	if err := database.AddUserPolitica(editor.ID, viewPolicyID); err != nil {
		t.Fatalf("AddUserPolitica view ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/municipis?pais_id=%d&q=Alfa", targetPaisID), nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminListMunicipis(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminListMunicipis esperava 200, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "Municipi Alfa") {
		t.Fatalf("esperava municipi visible amb scope comarca quan la comarca és a nivell_id_3")
	}

	reqSuggest := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/territori/municipis/suggest?q=Alf&pais_id=%d", targetPaisID), nil)
	reqSuggest.AddCookie(session)
	rrSuggest := httptest.NewRecorder()
	app.AdminMunicipisSuggest(rrSuggest, reqSuggest)
	if rrSuggest.Code != http.StatusOK {
		t.Fatalf("AdminMunicipisSuggest esperava 200, got %d", rrSuggest.Code)
	}
	if !strings.Contains(rrSuggest.Body.String(), "Municipi Alfa") {
		t.Fatalf("esperava suggest visible amb scope comarca quan la comarca és a nivell_id_3: %s", rrSuggest.Body.String())
	}
}

func TestTerritorialScopedBooksAndArchivesUseAnyLevelColumnF3015(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_f30_15_llibres_arxius_any_column.sqlite3")

	user := createTestUser(t, database, "f30_15_scope_docs_user")
	targetPaisID := createBrowseTestCountry(t, database, "LC")
	level1 := createBrowseTestLevel(t, database, targetPaisID, 1, "Regio Alfa", "regio", 0)
	level2 := createBrowseTestLevel(t, database, targetPaisID, 2, "Provincia Alfa", "provincia", level1)
	targetComarcaID := createBrowseTestLevel(t, database, targetPaisID, 3, "Comarca Alfa", "comarca", level2)

	munID := createBrowseTestMunicipi(t, database, user.ID, "Municipi Alfa", [7]int{level1, level2, targetComarcaID})
	eclesID, err := database.CreateArquebisbat(&db.Arquebisbat{
		Nom:            "Bisbat Test",
		TipusEntitat:   "bisbat",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("CreateArquebisbat ha fallat: %v", err)
	}
	arxiuID, err := database.CreateArxiu(&db.Arxiu{
		Nom:            "Arxiu Test",
		Tipus:          "parroquia",
		MunicipiID:     sql.NullInt64{Int64: int64(munID), Valid: true},
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("CreateArxiu ha fallat: %v", err)
	}
	llibreID, err := database.CreateLlibre(&db.Llibre{
		ArquebisbatID:  eclesID,
		MunicipiID:     munID,
		Titol:          "Llibre Test",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("CreateLlibre ha fallat: %v", err)
	}
	if err := database.AddArxiuLlibre(arxiuID, llibreID, "", ""); err != nil {
		t.Fatalf("AddArxiuLlibre ha fallat: %v", err)
	}

	arxius, err := database.ListArxius(db.ArxiuFilter{
		Status:            "publicat",
		AllowedComarcaIDs: []int{targetComarcaID},
	})
	if err != nil {
		t.Fatalf("ListArxius amb AllowedComarcaIDs ha fallat: %v", err)
	}
	if len(arxius) != 1 || arxius[0].Nom != "Arxiu Test" {
		t.Fatalf("esperava Arxiu Test amb filtre de comarca semàntic, got %+v", arxius)
	}
	totalArxius, err := database.CountArxius(db.ArxiuFilter{
		Status:            "publicat",
		AllowedComarcaIDs: []int{targetComarcaID},
	})
	if err != nil {
		t.Fatalf("CountArxius amb AllowedComarcaIDs ha fallat: %v", err)
	}
	if totalArxius != 1 {
		t.Fatalf("count arxius esperat 1 amb filtre de comarca semàntic, got %d", totalArxius)
	}

	llibres, err := database.ListLlibres(db.LlibreFilter{
		Status:            "publicat",
		AllowedComarcaIDs: []int{targetComarcaID},
	})
	if err != nil {
		t.Fatalf("ListLlibres amb AllowedComarcaIDs ha fallat: %v", err)
	}
	if len(llibres) != 1 || llibres[0].Titol != "Llibre Test" {
		t.Fatalf("esperava Llibre Test amb filtre de comarca semàntic, got %+v", llibres)
	}
	totalLlibres, err := database.CountLlibres(db.LlibreFilter{
		Status:            "publicat",
		AllowedComarcaIDs: []int{targetComarcaID},
	})
	if err != nil {
		t.Fatalf("CountLlibres amb AllowedComarcaIDs ha fallat: %v", err)
	}
	if totalLlibres != 1 {
		t.Fatalf("count llibres esperat 1 amb filtre de comarca semàntic, got %d", totalLlibres)
	}
}
