package integration

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
	testcommon "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

func newBrowseFix2Apps(t *testing.T, logLevel string) []appDB {
	t.Helper()

	var apps []appDB
	for _, dbCfg := range testcommon.LoadTestDBConfigs(t) {
		if strings.EqualFold(dbCfg.Engine, "mysql") {
			continue
		}
		cfg := map[string]string{}
		for k, v := range dbCfg.Config {
			cfg[k] = v
		}
		cfg["RECREADB"] = "true"
		cfg["LOG_LEVEL"] = logLevel
		if strings.EqualFold(dbCfg.Engine, "sqlite") {
			cfg["DB_PATH"] = filepath.Join(t.TempDir(), "test_f30_16_fix_2.sqlite3")
		}
		app, database := newTestAppForConfig(t, cfg)
		apps = append(apps, appDB{Label: dbCfg.Label, App: app, DB: database})
	}
	return apps
}

func browseFix2ISO(label, prefix string) string {
	letters := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	idx := int(time.Now().UnixNano() % int64(len(letters)))
	return prefix + string(letters[idx])
}

func createBrowseFix2SessionAdmin(t *testing.T, database db.DB, prefix string) *http.Cookie {
	t.Helper()
	suffix := fmt.Sprint(time.Now().UnixNano())
	user := &db.User{
		Usuari:        prefix + "_admin_" + suffix,
		Email:         prefix + "_" + suffix + "@example.com",
		Password:      []byte("hash"),
		Active:        true,
		DataNaixament: "1990-01-01",
		CreatedAt:     time.Now().Format(time.RFC3339),
	}
	if err := database.InsertUser(user); err != nil {
		t.Fatalf("InsertUser admin browse fix2 ha fallat: %v", err)
	}
	if err := database.EnsureDefaultPolicies(); err != nil {
		t.Fatalf("EnsureDefaultPolicies ha fallat: %v", err)
	}
	rows, err := database.Query("SELECT id FROM politiques WHERE nom = 'admin'")
	if err != nil || len(rows) == 0 {
		t.Fatalf("no puc obtenir politica admin: %v", err)
	}
	policyID := parseCountValue(t, rows[0]["id"])
	if err := database.AddUserPolitica(user.ID, policyID); err != nil {
		t.Fatalf("AddUserPolitica admin ha fallat: %v", err)
	}
	return createSessionCookie(t, database, user.ID, "sess_"+prefix+suffix)
}

func TestMunicipiBrowseCountAndListStayAlignedWithFocusF3016Fix2(t *testing.T) {
	for _, backend := range newBrowseFix2Apps(t, "silent") {
		backend := backend
		t.Run(backend.Label, func(t *testing.T) {
			targetPaisID := createBrowseTestCountry(t, backend.DB, browseFix2ISO(backend.Label, "F"))
			level1 := createBrowseTestLevel(t, backend.DB, targetPaisID, 1, "Regio Alfa", "regio", 0)
			level2 := createBrowseTestLevel(t, backend.DB, targetPaisID, 2, "Provincia Alfa", "provincia", level1)
			level3 := createBrowseTestLevel(t, backend.DB, targetPaisID, 3, "Comarca Alfa", "comarca", level2)
			levelIDs := [7]int{level1, level2, level3}

			alphaID := createBrowseTestMunicipiWithOptions(t, backend.DB, 0, "Municipi Alfa", "nucli_urba", "publicat", levelIDs)
			betaID := createBrowseTestMunicipiWithOptions(t, backend.DB, 0, "Municipi Beta", "nucli_urba", "publicat", levelIDs)
			createBrowseTestMunicipiWithOptions(t, backend.DB, 0, "Municipi Fora", "poble", "publicat", levelIDs)

			filter := db.MunicipiBrowseFilter{
				PaisID:   targetPaisID,
				FocusID:  betaID,
				Tipus:    "nucli_urba",
				Status:   "publicat",
				LevelIDs: [7]int{level1, level2, level3},
				Sort:     "nom",
				SortDir:  "asc",
				Limit:    25,
				Offset:   0,
			}

			total, err := backend.DB.CountMunicipisBrowse(filter)
			if err != nil {
				t.Fatalf("[%s] CountMunicipisBrowse ha fallat: %v", backend.Label, err)
			}
			probe := filter
			probe.FocusID = 0
			probe.Limit = 0
			probe.Offset = 0
			probe.MunicipiID = betaID
			focusMatches, err := backend.DB.CountMunicipisBrowse(probe)
			if err != nil {
				t.Fatalf("[%s] probe focus CountMunicipisBrowse ha fallat: %v", backend.Label, err)
			}
			rows, err := backend.DB.ListMunicipisBrowse(filter)
			if err != nil {
				t.Fatalf("[%s] ListMunicipisBrowse ha fallat: %v", backend.Label, err)
			}

			if total < 2 {
				t.Fatalf("[%s] count esperat almenys 2, got %d", backend.Label, total)
			}
			if focusMatches == 0 {
				t.Fatalf("[%s] el focus hauria de complir el filtre", backend.Label)
			}
			if len(rows) == 0 {
				t.Fatalf("[%s] si count=%d i focusMatches=true, list no pot ser buit", backend.Label, total)
			}
			if rows[0].ID != betaID {
				t.Fatalf("[%s] focus_id hauria de prioritzar Municipi Beta; primer ID=%d", backend.Label, rows[0].ID)
			}
			if alphaID == betaID {
				t.Fatalf("[%s] fixture invàlida: alpha i beta no poden compartir ID", backend.Label)
			}

			debugInfo := backend.DB.DebugMunicipiBrowse(filter)
			if !debugInfo.FocusInOrder || debugInfo.FocusArgIndex <= 0 {
				t.Fatalf("[%s] el LIST ha d'injectar focus a l'ORDER BY; debug=%+v", backend.Label, debugInfo)
			}
			if strings.EqualFold(backend.Label, "postgres") {
				if strings.Contains(debugInfo.ListSQL, "indexacio_completa = 1") {
					t.Fatalf("[%s] el LIST no pot comparar boolean amb enter: %s", backend.Label, debugInfo.ListSQL)
				}
				if !strings.Contains(debugInfo.ListSQL, "indexacio_completa = TRUE") {
					t.Fatalf("[%s] el LIST ha d'usar comparació booleana nativa: %s", backend.Label, debugInfo.ListSQL)
				}
			}
		})
	}
}

func TestMunicipiBrowseDebugInfoPreservesFocusAndLimitArgOrderF3016Fix2(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_f30_16_fix_2_debug.sqlite3")

	filter := db.MunicipiBrowseFilter{
		Text:     "Municipi",
		PaisID:   1,
		FocusID:  308,
		Tipus:    "nucli_urba",
		Status:   "publicat",
		LevelIDs: [7]int{1, 2, 6},
		Sort:     "nom",
		SortDir:  "asc",
		Limit:    25,
		Offset:   0,
	}
	debugInfo := database.DebugMunicipiBrowse(filter)

	if !reflect.DeepEqual(debugInfo.CountArgs, debugInfo.ListArgs[:len(debugInfo.CountArgs)]) {
		t.Fatalf("countArgs i prefix de listArgs haurien de coincidir; count=%v list=%v", debugInfo.CountArgs, debugInfo.ListArgs)
	}
	if !debugInfo.FocusInOrder || debugInfo.FocusArgIndex != len(debugInfo.CountArgs)+1 {
		t.Fatalf("focus_id ha d'entrar just després dels args del WHERE; debug=%+v", debugInfo)
	}
	if !debugInfo.LimitApplied || debugInfo.LimitArgIndex != len(debugInfo.ListArgs) {
		t.Fatalf("LIMIT ha d'ocupar l'últim arg del LIST quan offset=0; debug=%+v", debugInfo)
	}
	if debugInfo.OffsetApplied {
		t.Fatalf("OFFSET no hauria d'estar aplicat quan és 0; debug=%+v", debugInfo)
	}
	if gotFocus, ok := debugInfo.ListArgs[debugInfo.FocusArgIndex-1].(int); !ok || gotFocus != 308 {
		t.Fatalf("focus_id desalineat dins listArgs: debug=%+v", debugInfo)
	}
}

func TestMunicipiBrowseHandlerFullURLWithFocusF3016Fix2(t *testing.T) {
	core.SetLogLevel("debug")
	defer core.SetLogLevel("error")

	for _, backend := range newBrowseFix2Apps(t, "debug") {
		backend := backend
		t.Run(backend.Label, func(t *testing.T) {
			session := createBrowseFix2SessionAdmin(t, backend.DB, "f30_16_fix_2_"+backend.Label)

			targetPaisID := createBrowseTestCountry(t, backend.DB, browseFix2ISO(backend.Label, "H"))
			level1 := createBrowseTestLevel(t, backend.DB, targetPaisID, 1, "Regio Alfa", "regio", 0)
			level2 := createBrowseTestLevel(t, backend.DB, targetPaisID, 2, "Provincia Alfa", "provincia", level1)
			level3 := createBrowseTestLevel(t, backend.DB, targetPaisID, 3, "Comarca Alfa", "comarca", level2)
			levelIDs := [7]int{level1, level2, level3}

			createBrowseTestMunicipiWithOptions(t, backend.DB, 0, "Municipi Alfa", "nucli_urba", "publicat", levelIDs)
			betaID := createBrowseTestMunicipiWithOptions(t, backend.DB, 0, "Municipi Beta", "nucli_urba", "publicat", levelIDs)

			req := httptest.NewRequest(http.MethodGet,
				fmt.Sprintf("/territori/municipis?page=1&per_page=25&focus_id=%d&pais_id=%d&tipus=nucli_urba&status=publicat&nivell_id_1=%d&nivell_id_2=%d&nivell_id_3=%d", betaID, targetPaisID, level1, level2, level3),
				nil,
			)
			req.AddCookie(session)
			rr := httptest.NewRecorder()
			backend.App.AdminListMunicipis(rr, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("[%s] AdminListMunicipis esperava 200, got %d", backend.Label, rr.Code)
			}
			body := rr.Body.String()
			if !strings.Contains(body, "Municipi Alfa") || !strings.Contains(body, "Municipi Beta") {
				t.Fatalf("[%s] el handler no ha mostrat els municipis esperats; body=%s", backend.Label, body)
			}
			if idxBeta, idxAlpha := strings.Index(body, "Municipi Beta"), strings.Index(body, "Municipi Alfa"); idxBeta < 0 || idxAlpha < 0 || idxBeta > idxAlpha {
				t.Fatalf("[%s] focus_id hauria de prioritzar Municipi Beta; idxBeta=%d idxAlpha=%d", backend.Label, idxBeta, idxAlpha)
			}
		})
	}
}

func TestMunicipiBrowseExactCaseShapeF3016Fix2(t *testing.T) {
	core.SetLogLevel("debug")
	defer core.SetLogLevel("error")

	app, database := newTestAppForLogin(t, "test_f30_16_fix_2_exact_case.sqlite3")
	session := createBrowseFix2SessionAdmin(t, database, "f30_16_fix_2_exact_case")

	targetPaisID := createBrowseTestCountry(t, database, "EZ")
	if targetPaisID != 1 {
		t.Fatalf("el test requereix pais_id=1, got %d", targetPaisID)
	}
	level1 := createBrowseTestLevel(t, database, targetPaisID, 1, "Regio Alfa", "regio", 0)
	level2 := createBrowseTestLevel(t, database, targetPaisID, 2, "Provincia Alfa", "provincia", level1)
	level3Filler := createBrowseTestLevel(t, database, targetPaisID, 3, "Vegueria Filler", "vegueria", level2)
	level4Filler := createBrowseTestLevel(t, database, targetPaisID, 4, "Subregio Filler", "subregio", level3Filler)
	level5Filler := createBrowseTestLevel(t, database, targetPaisID, 5, "Partit Filler", "partit", level4Filler)
	level3 := createBrowseTestLevel(t, database, targetPaisID, 6, "Comarca Alfa", "comarca", level5Filler)
	if level1 != 1 || level2 != 2 || level3 != 6 {
		t.Fatalf("el test requereix nivell_id_1=1 nivell_id_2=2 nivell_id_3=6, got %d %d %d", level1, level2, level3)
	}

	for i := 0; i < 284; i++ {
		createBrowseTestMunicipiWithOptions(t, database, 0, fmt.Sprintf("Dummy %03d", i+1), "poble", "publicat", [7]int{level1, level2, level3})
	}
	for i := 0; i < 23; i++ {
		createBrowseTestMunicipiWithOptions(t, database, 0, fmt.Sprintf("Municipi Match %02d", i+1), "nucli_urba", "publicat", [7]int{level1, level2, level3})
	}
	focusID := createBrowseTestMunicipiWithOptions(t, database, 0, "Municipi Focus 308", "nucli_urba", "publicat", [7]int{level1, level2, level3})
	if focusID != 308 {
		t.Fatalf("el test requereix focus_id=308, got %d", focusID)
	}

	req := httptest.NewRequest(http.MethodGet,
		"/territori/municipis?page=1&per_page=25&focus_id=308&pais_id=1&tipus=nucli_urba&nivell_id_1=1&nivell_id_2=2&nivell_id_3=6",
		nil,
	)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminListMunicipis(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminListMunicipis exact case esperava 200, got %d", rr.Code)
	}

	filter := db.MunicipiBrowseFilter{
		PaisID:   1,
		FocusID:  308,
		Tipus:    "nucli_urba",
		Status:   "publicat",
		LevelIDs: [7]int{1, 2, 6},
		Sort:     "nom",
		SortDir:  "asc",
		Limit:    25,
		Offset:   0,
	}
	total, err := database.CountMunicipisBrowse(filter)
	if err != nil {
		t.Fatalf("CountMunicipisBrowse exact case ha fallat: %v", err)
	}
	rows, err := database.ListMunicipisBrowse(filter)
	if err != nil {
		t.Fatalf("ListMunicipisBrowse exact case ha fallat: %v", err)
	}
	if total != 24 || len(rows) != 24 {
		t.Fatalf("exact case esperava total=24 rows=24, got total=%d rows=%d", total, len(rows))
	}
	if rows[0].ID != 308 {
		t.Fatalf("exact case hauria de prioritzar focus_id=308; primer ID=%d", rows[0].ID)
	}
	if !strings.Contains(rr.Body.String(), "Municipi Focus 308") {
		t.Fatalf("el handler exact case no ha mostrat el focus; body=%s", rr.Body.String())
	}
}

func TestTerritoriRealCountListFocusSmokeF3016Fix2(t *testing.T) {
	if os.Getenv("CG_RUN_REAL_TERRITORI_BROWSE_SMOKE") != "1" {
		t.Skip("smoke real F30-16-fix-2 desactivada; posa CG_RUN_REAL_TERRITORI_BROWSE_SMOKE=1 per executar-la")
	}

	projectRoot := findProjectRoot(t)
	territoriPath := filepath.Join(projectRoot, "plantilla-temporal", "importacions", "territori-export-idescat-nuclis-mapat.json")
	if _, err := os.Stat(territoriPath); err != nil {
		t.Skipf("fitxer de smoke real no disponible: %v", err)
	}

	app, database := newTestAppForLogin(t, "test_f30_16_fix_2_real_smoke.sqlite3")
	session := createBrowseTestSessionAdmin(t, database, "f30_16_fix_2_real_smoke")
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
	filter := db.MunicipiBrowseFilter{
		PaisID:   browseEffectivePaisID(t, database, levelIDs),
		FocusID:  arbeca.ID,
		Tipus:    arbeca.Tipus,
		Status:   "publicat",
		LevelIDs: [7]int{levelIDs[0], levelIDs[1], levelIDs[2]},
		Sort:     "nom",
		SortDir:  "asc",
		Limit:    25,
	}
	total, err := database.CountMunicipisBrowse(filter)
	if err != nil {
		t.Fatalf("CountMunicipisBrowse smoke real ha fallat: %v", err)
	}
	rows, err := database.ListMunicipisBrowse(filter)
	if err != nil {
		t.Fatalf("ListMunicipisBrowse smoke real ha fallat: %v", err)
	}
	if total <= 0 || len(rows) == 0 {
		t.Fatalf("smoke real inconsistente count/list: total=%d rows=%d", total, len(rows))
	}
}
