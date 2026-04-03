package integration

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createBrowseTestSessionAdmin(t *testing.T, database db.DB, prefix string) *http.Cookie {
	t.Helper()
	user := createTestUser(t, database, prefix+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, user.ID, "admin")
	return createSessionCookie(t, database, user.ID, "sess_"+prefix+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))
}

func updateBrowseMunicipiStatus(t *testing.T, database db.DB, municipiID int, status string) {
	t.Helper()
	mun, err := database.GetMunicipi(municipiID)
	if err != nil || mun == nil {
		t.Fatalf("GetMunicipi %d ha fallat: %v", municipiID, err)
	}
	mun.ModeracioEstat = status
	if err := database.UpdateMunicipi(mun); err != nil {
		t.Fatalf("UpdateMunicipi %d ha fallat: %v", municipiID, err)
	}
}

func updateBrowseNivellStatus(t *testing.T, database db.DB, nivellID int, status string) {
	t.Helper()
	nivell, err := database.GetNivell(nivellID)
	if err != nil || nivell == nil {
		t.Fatalf("GetNivell %d ha fallat: %v", nivellID, err)
	}
	nivell.ModeracioEstat = status
	if err := database.UpdateNivell(nivell); err != nil {
		t.Fatalf("UpdateNivell %d ha fallat: %v", nivellID, err)
	}
}

func browseEffectivePaisID(t *testing.T, database db.DB, levelIDs [7]int) int {
	t.Helper()
	for _, levelID := range levelIDs {
		if levelID <= 0 {
			continue
		}
		nivell, err := database.GetNivell(levelID)
		if err == nil && nivell != nil && nivell.PaisID > 0 {
			return nivell.PaisID
		}
	}
	return 0
}

func clearBrowseTestLevelPaisID(t *testing.T, database db.DB, nivellID int) {
	t.Helper()
	if _, err := database.Exec("UPDATE nivells_administratius SET pais_id = NULL WHERE id = ?", nivellID); err != nil {
		t.Fatalf("no s'ha pogut deixar pais_id a NULL per nivell %d: %v", nivellID, err)
	}
}

func TestMunicipiBrowseHandlerFiltersPublishedWithEffectivePaisAndFocusF3016(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_16_browse_handler.sqlite3")
	session := createBrowseTestSessionAdmin(t, database, "f30_16_handler_admin")

	targetPaisID := createBrowseTestCountry(t, database, "HZ")
	level1 := createBrowseTestLevel(t, database, targetPaisID, 1, "Regio Sense Pais", "regio", 0)
	clearBrowseTestLevelPaisID(t, database, level1)
	level2 := createBrowseTestLevel(t, database, targetPaisID, 2, "Provincia Alfa", "provincia", level1)
	level3 := createBrowseTestLevel(t, database, targetPaisID, 3, "Comarca Alfa", "comarca", level2)
	levelIDs := [7]int{level1, level2, level3}

	alphaID := createBrowseTestMunicipiWithOptions(t, database, 0, "Municipi Alfa", "nucli_urba", "publicat", levelIDs)
	betaID := createBrowseTestMunicipiWithOptions(t, database, 0, "Municipi Beta", "nucli_urba", "publicat", levelIDs)
	createBrowseTestMunicipiWithOptions(t, database, 0, "Municipi Gamma", "poble", "publicat", levelIDs)
	createBrowseTestMunicipiWithOptions(t, database, 0, "Municipi Delta", "nucli_urba", "pendent", levelIDs)

	req := httptest.NewRequest(http.MethodGet,
		fmt.Sprintf("/territori/municipis?page=1&per_page=25&focus_id=%d&pais_id=%d&tipus=nucli_urba&status=publicat&nivell_id_1=%d&nivell_id_2=%d&nivell_id_3=%d", betaID, targetPaisID, level1, level2, level3),
		nil,
	)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminListMunicipis(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminListMunicipis esperava 200, got %d", rr.Code)
	}

	body := rr.Body.String()
	if !strings.Contains(body, "Municipi Alfa") || !strings.Contains(body, "Municipi Beta") {
		t.Fatalf("el handler no mostra els municipis publicats esperats; body=%s", body)
	}
	if strings.Contains(body, "Municipi Gamma") || strings.Contains(body, "Municipi Delta") {
		t.Fatalf("el handler ha mostrat municipis fora de filtre; body=%s", body)
	}
	if idxBeta, idxAlpha := strings.Index(body, "Municipi Beta"), strings.Index(body, "Municipi Alfa"); idxBeta < 0 || idxAlpha < 0 || idxBeta > idxAlpha {
		t.Fatalf("focus_id hauria de prioritzar Municipi Beta davant Alfa; idxBeta=%d idxAlpha=%d", idxBeta, idxAlpha)
	}

	total, err := database.CountMunicipisBrowse(db.MunicipiBrowseFilter{
		PaisID:   targetPaisID,
		Tipus:    "nucli_urba",
		Status:   "publicat",
		LevelIDs: [7]int{level1, level2, level3},
	})
	if err != nil {
		t.Fatalf("CountMunicipisBrowse ha fallat: %v", err)
	}
	if total != 2 {
		t.Fatalf("count municipis esperat 2, got %d", total)
	}
	if alphaID == betaID {
		t.Fatalf("els IDs de focus i municipis de control no poden coincidir")
	}
}

func TestMunicipiBrowseFocusDoesNotInjectOutOfFilterF3016(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_16_focus_guard.sqlite3")
	session := createBrowseTestSessionAdmin(t, database, "f30_16_focus_admin")

	targetPaisID := createBrowseTestCountry(t, database, "QZ")
	level1 := createBrowseTestLevel(t, database, targetPaisID, 1, "Regio Sense Pais", "regio", 0)
	clearBrowseTestLevelPaisID(t, database, level1)
	level2 := createBrowseTestLevel(t, database, targetPaisID, 2, "Provincia Alfa", "provincia", level1)
	level3 := createBrowseTestLevel(t, database, targetPaisID, 3, "Comarca Alfa", "comarca", level2)
	levelIDs := [7]int{level1, level2, level3}

	createBrowseTestMunicipiWithOptions(t, database, 0, "Municipi Alfa", "nucli_urba", "publicat", levelIDs)
	createBrowseTestMunicipiWithOptions(t, database, 0, "Municipi Beta", "nucli_urba", "publicat", levelIDs)
	outOfFilterID := createBrowseTestMunicipiWithOptions(t, database, 0, "Municipi Fora", "poble", "publicat", levelIDs)

	req := httptest.NewRequest(http.MethodGet,
		fmt.Sprintf("/territori/municipis?page=1&per_page=25&focus_id=%d&pais_id=%d&tipus=nucli_urba&status=publicat&nivell_id_1=%d&nivell_id_2=%d&nivell_id_3=%d", outOfFilterID, targetPaisID, level1, level2, level3),
		nil,
	)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminListMunicipis(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminListMunicipis esperava 200, got %d", rr.Code)
	}

	body := rr.Body.String()
	if strings.Contains(body, "Municipi Fora") {
		t.Fatalf("focus_id no pot injectar un municipi fora de filtre; body=%s", body)
	}
	if !strings.Contains(body, "Municipi Alfa") || !strings.Contains(body, "Municipi Beta") {
		t.Fatalf("el handler ha de mantenir els municipis vàlids; body=%s", body)
	}
}

func TestMunicipiBrowseCountListAndSuggestStayCoherentF3016(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_f30_16_semantics.sqlite3")

	targetPaisID := createBrowseTestCountry(t, database, "RZ")
	level1 := createBrowseTestLevel(t, database, targetPaisID, 1, "Regio Sense Pais", "regio", 0)
	clearBrowseTestLevelPaisID(t, database, level1)
	level2 := createBrowseTestLevel(t, database, targetPaisID, 2, "Provincia Alfa", "provincia", level1)
	level3 := createBrowseTestLevel(t, database, targetPaisID, 3, "Comarca Alfa", "comarca", level2)
	levelIDs := [7]int{level1, level2, level3}

	createBrowseTestMunicipiWithOptions(t, database, 0, "Municipi Alfa", "nucli_urba", "publicat", levelIDs)
	createBrowseTestMunicipiWithOptions(t, database, 0, "Municipi Beta", "nucli_urba", "publicat", levelIDs)
	createBrowseTestMunicipiWithOptions(t, database, 0, "Municipi Fora Tipus", "poble", "publicat", levelIDs)
	createBrowseTestMunicipiWithOptions(t, database, 0, "Municipi Fora Status", "nucli_urba", "pendent", levelIDs)

	filter := db.MunicipiBrowseFilter{
		Text:     "Municipi",
		PaisID:   targetPaisID,
		Tipus:    "nucli_urba",
		Status:   "publicat",
		LevelIDs: [7]int{level1, level2, level3},
		Sort:     "nom",
		SortDir:  "asc",
	}

	total, err := database.CountMunicipisBrowse(filter)
	if err != nil {
		t.Fatalf("CountMunicipisBrowse ha fallat: %v", err)
	}
	rows, err := database.ListMunicipisBrowse(filter)
	if err != nil {
		t.Fatalf("ListMunicipisBrowse ha fallat: %v", err)
	}
	suggestions, err := database.SuggestMunicipis(db.MunicipiBrowseFilter{
		Text:     "Municipi",
		PaisID:   targetPaisID,
		Tipus:    "nucli_urba",
		Status:   "publicat",
		LevelIDs: [7]int{level1, level2, level3},
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("SuggestMunicipis ha fallat: %v", err)
	}

	if total != 2 || len(rows) != 2 || len(suggestions) != 2 {
		t.Fatalf("semàntica incoherent count/list/suggest: total=%d rows=%d suggestions=%d", total, len(rows), len(suggestions))
	}
	if rows[0].Nom != "Municipi Alfa" || rows[1].Nom != "Municipi Beta" {
		t.Fatalf("rows inesperades: %+v", rows)
	}
	gotSuggest := []string{suggestions[0].Nom, suggestions[1].Nom}
	if !(gotSuggest[0] == "Municipi Alfa" && gotSuggest[1] == "Municipi Beta") {
		t.Fatalf("suggestions inesperades: %+v", gotSuggest)
	}
}

func TestTerritoriRealBrowseSmokeF3016(t *testing.T) {
	if os.Getenv("CG_RUN_REAL_TERRITORI_BROWSE_SMOKE") != "1" {
		t.Skip("smoke real F30-16 desactivada; posa CG_RUN_REAL_TERRITORI_BROWSE_SMOKE=1 per executar-la")
	}

	projectRoot := findProjectRoot(t)
	territoriPath := filepath.Join(projectRoot, "plantilla-temporal", "importacions", "territori-export-idescat-nuclis-mapat.json")
	if _, err := os.Stat(territoriPath); err != nil {
		t.Skipf("fitxer de smoke real no disponible: %v", err)
	}

	app, database := newTestAppForLogin(t, "test_f30_16_real_smoke.sqlite3")
	session := createBrowseTestSessionAdmin(t, database, "f30_16_real_smoke")
	runTerritoriImportFixture(t, app, session, territoriPath)

	arbeca := findMunicipiByNom(t, database, "Arbeca")
	updateBrowseMunicipiStatus(t, database, arbeca.ID, "publicat")

	levelIDs := [7]int{}
	for i, levelID := range arbeca.NivellAdministratiuID {
		if levelID.Valid && levelID.Int64 > 0 {
			levelIDs[i] = int(levelID.Int64)
			updateBrowseNivellStatus(t, database, int(levelID.Int64), "publicat")
		}
	}
	paisID := browseEffectivePaisID(t, database, levelIDs)
	if paisID <= 0 {
		t.Fatalf("no s'ha pogut deduir el país efectiu d'Arbeca")
	}

	req := httptest.NewRequest(http.MethodGet,
		fmt.Sprintf("/territori/municipis?page=1&per_page=25&focus_id=%d&pais_id=%d&tipus=%s&status=publicat&nivell_id_1=%d&nivell_id_2=%d&nivell_id_3=%d", arbeca.ID, paisID, arbeca.Tipus, levelIDs[0], levelIDs[1], levelIDs[2]),
		nil,
	)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminListMunicipis(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminListMunicipis smoke real esperava 200, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "Arbeca") {
		t.Fatalf("la smoke real no ha mostrat Arbeca; body=%s", rr.Body.String())
	}
}
