package integration

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestF331MunicipiBrowseLinksAdministrativeLevels(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_1_municipi_level_links.sqlite3")
	session := createBrowseTestSessionAdmin(t, database, "f33_1_links_admin")

	targetPaisID := createBrowseTestCountry(t, database, "LQ")
	level1 := createBrowseTestLevel(t, database, targetPaisID, 1, "Regio Link", "regio", 0)
	level2 := createBrowseTestLevel(t, database, targetPaisID, 2, "Provincia Link", "provincia", level1)
	level3 := createBrowseTestLevel(t, database, targetPaisID, 3, "Comarca Link", "comarca", level2)
	munID := createBrowseTestMunicipi(t, database, 0, "Municipi Link", [7]int{level1, level2, level3})

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/municipis?pais_id=%d&nivell_id_1=%d&nivell_id_2=%d&nivell_id_3=%d&status=publicat", targetPaisID, level1, level2, level3), nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminListMunicipis(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminListMunicipis esperava 200, got %d", rr.Code)
	}

	body := rr.Body.String()
	for _, levelID := range []int{level1, level2, level3} {
		href := fmt.Sprintf(`href="/territori/nivells/%d"`, levelID)
		if !strings.Contains(body, href) {
			t.Fatalf("esperava link de nivell %d a la taula de municipis; body no conté %q", levelID, href)
		}
	}
	munHref := fmt.Sprintf(`href="/territori/municipis/%d"`, munID)
	if !strings.Contains(body, munHref) {
		t.Fatalf("el link existent del municipi s'ha de mantenir; body no conté %q", munHref)
	}
}

func TestF331NivellProfileShowsEditOnlyWithPermission(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_1_nivell_profile_edit.sqlite3")

	paisID := createBrowseTestCountry(t, database, "PE")
	levelID := createBrowseTestLevel(t, database, paisID, 1, "Regio Editable", "regio", 0)

	editor := createTestUser(t, database, "f33_1_nivell_editor")
	editorSession := createSessionCookie(t, database, editor.ID, "sess_f33_1_nivell_editor")
	viewPolicyID := createScopedPolicyWithGrant(t, database, "f33_1_nivell_view", "territori.nivells.view", core.ScopePais, paisID, true)
	editPolicyID := createScopedPolicyWithGrant(t, database, "f33_1_nivell_edit", "territori.nivells.edit", core.ScopePais, paisID, true)
	if err := database.AddUserPolitica(editor.ID, viewPolicyID); err != nil {
		t.Fatalf("AddUserPolitica view ha fallat: %v", err)
	}
	if err := database.AddUserPolitica(editor.ID, editPolicyID); err != nil {
		t.Fatalf("AddUserPolitica edit ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/nivells/%d", levelID), nil)
	req.AddCookie(editorSession)
	rr := httptest.NewRecorder()
	app.NivellPublic(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("NivellPublic editor esperava 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	editPrefix := fmt.Sprintf(`/territori/nivells/%d/edit?return_to=`, levelID)
	if !strings.Contains(body, editPrefix) || !strings.Contains(body, "Editar") {
		t.Fatalf("perfil de nivell hauria de mostrar botó Editar amb permís scoped; falta %q", editPrefix)
	}

	viewer := createTestUser(t, database, "f33_1_nivell_viewer")
	viewerSession := createSessionCookie(t, database, viewer.ID, "sess_f33_1_nivell_viewer")
	viewOnlyID := createScopedPolicyWithGrant(t, database, "f33_1_nivell_view_only", "territori.nivells.view", core.ScopePais, paisID, true)
	if err := database.AddUserPolitica(viewer.ID, viewOnlyID); err != nil {
		t.Fatalf("AddUserPolitica view-only ha fallat: %v", err)
	}
	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/nivells/%d", levelID), nil)
	req.AddCookie(viewerSession)
	rr = httptest.NewRecorder()
	app.NivellPublic(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("NivellPublic viewer esperava 200, got %d", rr.Code)
	}
	if strings.Contains(rr.Body.String(), editPrefix) {
		t.Fatalf("perfil de nivell no hauria de mostrar Editar sense permís d'edició")
	}
}

func TestF331ScopedNivellEditAndSaveStayInsidePais(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_1_nivell_scoped_edit.sqlite3")

	allowedPaisID := createBrowseTestCountry(t, database, "EA")
	blockedPaisID := createBrowseTestCountry(t, database, "EB")
	allowedLevelID := createBrowseTestLevel(t, database, allowedPaisID, 1, "Regio Permesa", "regio", 0)
	blockedLevelID := createBrowseTestLevel(t, database, blockedPaisID, 1, "Regio Bloquejada", "regio", 0)

	editor := createTestUser(t, database, "f33_1_scoped_editor")
	session := createSessionCookie(t, database, editor.ID, "sess_f33_1_scoped_editor")
	viewPolicyID := createScopedPolicyWithGrant(t, database, "f33_1_scoped_view", "territori.nivells.view", core.ScopePais, allowedPaisID, true)
	editPolicyID := createScopedPolicyWithGrant(t, database, "f33_1_scoped_edit", "territori.nivells.edit", core.ScopePais, allowedPaisID, true)
	if err := database.AddUserPolitica(editor.ID, viewPolicyID); err != nil {
		t.Fatalf("AddUserPolitica view ha fallat: %v", err)
	}
	if err := database.AddUserPolitica(editor.ID, editPolicyID); err != nil {
		t.Fatalf("AddUserPolitica edit ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/nivells/%d/edit", allowedLevelID), nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminEditNivell(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminEditNivell dins pais permès esperava 200, got %d", rr.Code)
	}

	form := url.Values{}
	form.Set("id", strconv.Itoa(allowedLevelID))
	form.Set("pais_id", strconv.Itoa(allowedPaisID))
	form.Set("nivel", "1")
	form.Set("nom_nivell", "Regio Permesa Editada")
	form.Set("tipus_nivell", "regio")
	form.Set("estat", "actiu")
	req = httptest.NewRequest(http.MethodPost, "/territori/nivells/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminSaveNivell(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("AdminSaveNivell dins pais permès esperava redirect, got %d body=%s", rr.Code, rr.Body.String())
	}
	updated, err := database.GetNivell(allowedLevelID)
	if err != nil || updated == nil {
		t.Fatalf("GetNivell actualitzat ha fallat: %v", err)
	}
	if updated.NomNivell != "Regio Permesa Editada" {
		t.Fatalf("AdminSaveNivell no ha guardat el canvi dins pais permès: %+v", updated)
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/nivells/%d/edit", blockedLevelID), nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminEditNivell(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("AdminEditNivell fora pais hauria de bloquejar amb 403, got %d", rr.Code)
	}
}

func TestF332MunicipiProfileShowsEditForGlobalAdmin(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_2_municipi_profile_admin.sqlite3")
	session := createBrowseTestSessionAdmin(t, database, "f33_2_mun_admin")

	paisID := createBrowseTestCountry(t, database, "MA")
	level1 := createBrowseTestLevel(t, database, paisID, 1, "Regio Admin", "regio", 0)
	munID := createBrowseTestMunicipi(t, database, 0, "Municipi Admin", [7]int{level1})

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/municipis/%d", munID), nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.MunicipiPublic(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("MunicipiPublic admin esperava 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	editPrefix := fmt.Sprintf(`/territori/municipis/%d/edit?return_to=`, munID)
	expectedReturnTo := url.QueryEscape(fmt.Sprintf("/territori/municipis/%d", munID))
	if !strings.Contains(body, editPrefix) || !strings.Contains(body, expectedReturnTo) || !strings.Contains(body, "Editar") {
		t.Fatalf("perfil de municipi hauria de mostrar Editar amb return_to; falten %q o %q", editPrefix, expectedReturnTo)
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/municipis/%d/edit", munID), nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminEditMunicipi(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminEditMunicipi admin esperava 200, got %d", rr.Code)
	}
}

func TestF332MunicipiProfileHidesEditWithoutPermissionAndEditURLForbids(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_2_municipi_profile_no_permission.sqlite3")

	paisID := createBrowseTestCountry(t, database, "MB")
	level1 := createBrowseTestLevel(t, database, paisID, 1, "Regio Publica", "regio", 0)
	munID := createBrowseTestMunicipi(t, database, 0, "Municipi Public", [7]int{level1})

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/municipis/%d", munID), nil)
	rr := httptest.NewRecorder()
	app.MunicipiPublic(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("MunicipiPublic anonim esperava 200, got %d", rr.Code)
	}
	editPrefix := fmt.Sprintf(`/territori/municipis/%d/edit?return_to=`, munID)
	if strings.Contains(rr.Body.String(), editPrefix) {
		t.Fatalf("perfil de municipi anonim no hauria de mostrar Editar")
	}

	user := createTestUser(t, database, "f33_2_mun_no_perm")
	session := createSessionCookie(t, database, user.ID, "sess_f33_2_mun_no_perm")
	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/municipis/%d/edit", munID), nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminEditMunicipi(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("AdminEditMunicipi sense permis hauria de bloquejar amb 403, got %d", rr.Code)
	}
}

func TestF332MunicipiProfileUsesScopedEditPermission(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_2_municipi_profile_scoped.sqlite3")

	allowedPaisID := createBrowseTestCountry(t, database, "MC")
	blockedPaisID := createBrowseTestCountry(t, database, "MD")
	allowedLevel := createBrowseTestLevel(t, database, allowedPaisID, 1, "Regio Permesa", "regio", 0)
	blockedLevel := createBrowseTestLevel(t, database, blockedPaisID, 1, "Regio Bloquejada", "regio", 0)
	allowedMunID := createBrowseTestMunicipi(t, database, 0, "Municipi Permes", [7]int{allowedLevel})
	blockedMunID := createBrowseTestMunicipi(t, database, 0, "Municipi Bloquejat", [7]int{blockedLevel})

	editor := createTestUser(t, database, "f33_2_mun_scoped_editor")
	session := createSessionCookie(t, database, editor.ID, "sess_f33_2_mun_scoped_editor")
	allowedViewID := createScopedPolicyWithGrant(t, database, "f33_2_mun_allowed_view", "territori.municipis.view", core.ScopePais, allowedPaisID, true)
	allowedEditID := createScopedPolicyWithGrant(t, database, "f33_2_mun_allowed_edit", "territori.municipis.edit", core.ScopePais, allowedPaisID, true)
	blockedViewID := createScopedPolicyWithGrant(t, database, "f33_2_mun_blocked_view", "territori.municipis.view", core.ScopePais, blockedPaisID, true)
	for _, policyID := range []int{allowedViewID, allowedEditID, blockedViewID} {
		if err := database.AddUserPolitica(editor.ID, policyID); err != nil {
			t.Fatalf("AddUserPolitica ha fallat: %v", err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/municipis/%d", allowedMunID), nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.MunicipiPublic(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("MunicipiPublic dins pais permes esperava 200, got %d", rr.Code)
	}
	allowedEditPrefix := fmt.Sprintf(`/territori/municipis/%d/edit?return_to=`, allowedMunID)
	if !strings.Contains(rr.Body.String(), allowedEditPrefix) {
		t.Fatalf("perfil de municipi dins pais permes hauria de mostrar Editar; falta %q", allowedEditPrefix)
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/municipis/%d/edit", allowedMunID), nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminEditMunicipi(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminEditMunicipi dins pais permes esperava 200, got %d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/municipis/%d", blockedMunID), nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.MunicipiPublic(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("MunicipiPublic fora ambit edit permes esperava 200 per view scoped, got %d", rr.Code)
	}
	blockedEditPrefix := fmt.Sprintf(`/territori/municipis/%d/edit?return_to=`, blockedMunID)
	if strings.Contains(rr.Body.String(), blockedEditPrefix) {
		t.Fatalf("perfil de municipi fora ambit edit no hauria de mostrar Editar")
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/municipis/%d/edit", blockedMunID), nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminEditMunicipi(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("AdminEditMunicipi fora ambit edit hauria de bloquejar amb 403, got %d", rr.Code)
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
