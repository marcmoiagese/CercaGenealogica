package integration

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF354ArxiuEntitatReligiosaSchemaSQLite(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_f35_4_arxiu_entitat_schema.sqlite3")

	if !f351SQLiteTableExists(t, database, "arxiu_entitat_religiosa") {
		t.Fatalf("taula arxiu_entitat_religiosa no creada")
	}
	got := f351SQLiteColumns(t, database, "arxiu_entitat_religiosa")
	for _, column := range []string{
		"id", "arxiu_id", "entitat_religiosa_id", "tipus_relacio",
		"any_inici", "any_fi", "observacions", "estat", "moderation_status",
		"created_by", "updated_by", "moderated_by", "moderated_at", "created_at", "updated_at",
	} {
		if !got[column] {
			t.Fatalf("arxiu_entitat_religiosa no te columna %s; columns=%v", column, got)
		}
	}
	for _, idx := range []string{
		"idx_arxiu_entitat_religiosa_arxiu",
		"idx_arxiu_entitat_religiosa_entitat",
		"idx_arxiu_entitat_religiosa_moderacio",
		"idx_arxiu_entitat_religiosa_tipus",
	} {
		if !f351SQLiteIndexExists(t, database, idx) {
			t.Fatalf("index esperat no creat: %s", idx)
		}
	}
	fks := f351SQLiteFKs(t, database, "arxiu_entitat_religiosa")
	for _, fk := range []string{
		"arxiu_id->arxius",
		"entitat_religiosa_id->entitat_religiosa",
	} {
		if !fks[fk] {
			t.Fatalf("FK esperada absent %s; fks=%v", fk, fks)
		}
	}
}

func TestF354ArxiuEntitatReligiosaSQLFilesAligned(t *testing.T) {
	root := findProjectRoot(t)
	for _, rel := range []string{"db/SQLite.sql", "db/PostgreSQL.sql", "db/MySQL.sql"} {
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("no s'ha pogut llegir %s: %v", rel, err)
		}
		src := string(body)
		for _, token := range []string{
			"CREATE TABLE IF NOT EXISTS arxiu_entitat_religiosa",
			"arxiu_id",
			"entitat_religiosa_id",
			"tipus_relacio",
			"any_inici",
			"any_fi",
			"moderation_status",
			"moderation_notes",
			"idx_arxiu_entitat_religiosa_arxiu",
			"idx_arxiu_entitat_religiosa_entitat",
			"idx_arxiu_entitat_religiosa_moderacio",
			"idx_arxiu_entitat_religiosa_tipus",
		} {
			if !strings.Contains(src, token) {
				t.Fatalf("%s no conte token F35-4 %q", rel, token)
			}
		}
		if strings.Contains(src, "llibre_entitat_"+"religiosa") {
			t.Fatalf("%s introdueix relacio llibre-entitat fora d'abast", rel)
		}
	}
}

func TestF354RArxiuEntitatReligiosaDBIsSeparatedPerMotor(t *testing.T) {
	root := findProjectRoot(t)
	sqlcommon := readProjectFileF354(t, root, "db/sqlcommon.go")
	for _, forbidden := range []string{
		"arxiu_entitat_religiosa",
		"ArxiuEntitatReligiosa",
		"ListArxiuEntitatsReligioses",
		"SaveArxiuEntitatReligiosa",
		"UpdateModeracioArxiuEntitatReligiosa",
		"UpdateArxiuEntitatReligiosaModeracio",
	} {
		if strings.Contains(sqlcommon, forbidden) {
			t.Fatalf("db/sqlcommon.go no ha de contenir referencia F35-4 %q", forbidden)
		}
	}

	body := readProjectFileF354(t, root, "db/arxiu_entitat_religiosa_per_motor.go")
	for _, required := range []string{
		"func sqliteListArxiuEntitatsReligioses(",
		"func sqliteGetArxiuEntitatReligiosa(",
		"func sqliteSaveArxiuEntitatReligiosa(",
		"func sqliteDeleteArxiuEntitatReligiosa(",
		"func sqliteUpdateArxiuEntitatReligiosaModeracio(",
		"func postgresListArxiuEntitatsReligioses(",
		"func postgresGetArxiuEntitatReligiosa(",
		"func postgresSaveArxiuEntitatReligiosa(",
		"func postgresDeleteArxiuEntitatReligiosa(",
		"func postgresUpdateArxiuEntitatReligiosaModeracio(",
		"func mysqlListArxiuEntitatsReligioses(",
		"func mysqlGetArxiuEntitatReligiosa(",
		"func mysqlSaveArxiuEntitatReligiosa(",
		"func mysqlDeleteArxiuEntitatReligiosa(",
		"func mysqlUpdateArxiuEntitatReligiosaModeracio(",
		"arxiu_id = ?",
		"entitat_religiosa_id = ?",
		"moderation_status = ?",
		"arxiu_id = $1",
		"entitat_religiosa_id = $2",
		"moderation_status = $3",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("falta contracte DB per motor F35-4R: %s", required)
		}
	}
	for _, forbidden := range []string{
		"formatPlaceholders",
		"type arxiuEntitatReligiosaQueries",
		"func listArxiuEntitatsReligioses(",
		"func getArxiuEntitatReligiosa(",
		"func saveArxiuEntitatReligiosa(",
		"func deleteArxiuEntitatReligiosa(",
		"func updateArxiuEntitatReligiosaModeracio(",
		"strings.Join",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("db/arxiu_entitat_religiosa_per_motor.go conserva helper comu prohibit: %s", forbidden)
		}
	}
}

func TestF354ArxiuEntitatReligiosaModerationAndProfiles(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4_arxiu_entitat_flow.sqlite3")
	session := f353YAdminSession(t, database, "arxiu_entitat")
	viewerSession := f354ArxiuEntitatViewerSession(t, database, "arxiu_entitat_viewer")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	arxiuID := f354CreateArxiu(t, database, "Arxiu F35-4 "+suffix)
	entitatID := f353YCreateEntitat(t, database, "Parroquia F35-4 "+suffix, "publicat")
	pendingEntitatID := f353YCreateEntitat(t, database, "Parroquia pendent F35-4 "+suffix, "pendent")

	relID, err := database.SaveArxiuEntitatReligiosa(&db.ArxiuEntitatReligiosa{
		ArxiuID:            arxiuID,
		EntitatReligiosaID: entitatID,
		TipusRelacio:       "custodia_documentacio",
		AnyInici:           sql.NullInt64{Int64: 1850, Valid: true},
		AnyFi:              sql.NullInt64{Int64: 1900, Valid: true},
		Observacions:       "relacio F35-4 " + suffix,
		Estat:              "actiu",
		ModeracioEstat:     "pendent",
	})
	if err != nil {
		t.Fatalf("SaveArxiuEntitatReligiosa: %v", err)
	}
	if list, err := database.ListArxiuEntitatsReligioses(arxiuID, 0, "publicat"); err != nil || len(list) != 0 {
		t.Fatalf("relacio pendent no s'ha de llistar com a publicada: list=%v err=%v", list, err)
	}
	arxiuPendingBody := f354Get(t, app.AdminShowArxiu, "/documentals/arxius/"+strconv.Itoa(arxiuID), viewerSession)
	if strings.Contains(arxiuPendingBody, "Parroquia F35-4 "+suffix) {
		t.Fatalf("la relacio pendent no ha d'apareixer al perfil d'arxiu")
	}
	entitatPendingBody := f354Get(t, app.AdminConfessionalEntityShow, "/confessional/entitats/"+strconv.Itoa(entitatID), viewerSession)
	if strings.Contains(entitatPendingBody, "Arxiu F35-4 "+suffix) {
		t.Fatalf("la relacio pendent no ha d'apareixer al perfil d'entitat religiosa")
	}

	body := f353YGet(t, app.AdminModeracioList, "/moderacio?type=arxiu_entitat_religiosa", session)
	if !strings.Contains(body, "Arxiu F35-4 "+suffix) || !strings.Contains(body, "Parroquia F35-4 "+suffix) {
		t.Fatalf("/moderacio ha de mostrar la relacio arxiu-entitat pendent, body=%s", body)
	}

	f353YPostModeracio(t, app.AdminModeracioAprovar, session, relID, "arxiu_entitat_religiosa", "aprovar", "")
	approved, err := database.GetArxiuEntitatReligiosa(relID)
	if err != nil {
		t.Fatalf("GetArxiuEntitatReligiosa aprovada: %v", err)
	}
	if approved.ModeracioEstat != "publicat" {
		t.Fatalf("estat aprovat=%q, want publicat", approved.ModeracioEstat)
	}

	arxiuBody := f354Get(t, app.AdminShowArxiu, "/documentals/arxius/"+strconv.Itoa(arxiuID), session)
	if !strings.Contains(arxiuBody, "Parroquia F35-4 "+suffix) || !strings.Contains(arxiuBody, "Custòdia de documentació") {
		t.Fatalf("el perfil d'arxiu ha de mostrar la relacio publicada, body=%s", arxiuBody)
	}
	entitatBody := f354Get(t, app.AdminConfessionalEntityShow, "/confessional/entitats/"+strconv.Itoa(entitatID), session)
	if !strings.Contains(entitatBody, "Arxiu F35-4 "+suffix) || !strings.Contains(entitatBody, "Custòdia de documentació") {
		t.Fatalf("el perfil d'entitat ha de mostrar la relacio publicada, body=%s", entitatBody)
	}

	form := url.Values{}
	form.Set("arxiu_id", strconv.Itoa(arxiuID))
	form.Set("entitat_religiosa_id", strconv.Itoa(pendingEntitatID))
	form.Set("tipus_relacio", "custodia_documentacio")
	body = f354PostArxiuEntitat(t, app.AdminSaveArxiuEntitatReligiosa, session, form)
	if !strings.Contains(body, "ha d&#39;estar publicada") && !strings.Contains(body, "ha d'estar publicada") {
		t.Fatalf("el POST manipulat amb entitat pendent ha de ser bloquejat, body=%s", body)
	}

	form = url.Values{}
	form.Set("arxiu_id", strconv.Itoa(arxiuID))
	form.Set("entitat_religiosa_id", strconv.Itoa(entitatID))
	form.Set("tipus_relacio", "tipus_manipulat")
	body = f354PostArxiuEntitat(t, app.AdminSaveArxiuEntitatReligiosa, session, form)
	if !strings.Contains(body, "tipus de relacio no es valid") && !strings.Contains(body, "tipus de relació no és valid") {
		t.Fatalf("el POST manipulat amb tipus invalid ha de ser bloquejat, body=%s", body)
	}

	form = url.Values{}
	form.Set("arxiu_id", strconv.Itoa(arxiuID))
	form.Set("entitat_religiosa_id", strconv.Itoa(entitatID))
	form.Set("tipus_relacio", "custodia_documentacio")
	form.Set("any_inici", "1850")
	form.Set("any_fi", "1900")
	body = f354PostArxiuEntitat(t, app.AdminSaveArxiuEntitatReligiosa, session, form)
	if !strings.Contains(body, "Ja existeix una relacio equivalent") && !strings.Contains(body, "Ja existeix una relació equivalent") {
		t.Fatalf("el duplicat equivalent ha de ser bloquejat, body=%s", body)
	}

	rejectedID, err := database.SaveArxiuEntitatReligiosa(&db.ArxiuEntitatReligiosa{
		ArxiuID:            arxiuID,
		EntitatReligiosaID: entitatID,
		TipusRelacio:       "context_religios",
		Estat:              "actiu",
		ModeracioEstat:     "pendent",
	})
	if err != nil {
		t.Fatalf("SaveArxiuEntitatReligiosa rebutjable: %v", err)
	}
	f353YPostModeracio(t, app.AdminModeracioRebutjar, session, rejectedID, "arxiu_entitat_religiosa", "rebutjar", "duplicada")
	rejected, err := database.GetArxiuEntitatReligiosa(rejectedID)
	if err != nil {
		t.Fatalf("GetArxiuEntitatReligiosa rebutjada: %v", err)
	}
	if rejected.ModeracioEstat != "rebutjat" || rejected.ModeracioMotiu == "" {
		t.Fatalf("relacio rebutjada mal desada: %+v", rejected)
	}

	if err := database.DeleteArxiuEntitatReligiosa(relID); err != nil {
		t.Fatalf("DeleteArxiuEntitatReligiosa: %v", err)
	}
}

func f354CreateArxiu(t *testing.T, database db.DB, name string) int {
	t.Helper()
	municipiID := f353YCreateMunicipi(t, database, "Municipi "+name)
	id, err := database.CreateArxiu(&db.Arxiu{
		Nom:            name,
		Tipus:          "parroquia",
		Acces:          "online",
		MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateArxiu: %v", err)
	}
	return id
}

func f354Get(t *testing.T, handler http.HandlerFunc, path string, session *http.Cookie) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	handler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET %s status=%d body=%s", path, rr.Code, rr.Body.String())
	}
	return rr.Body.String()
}

func f354PostArxiuEntitat(t *testing.T, handler http.HandlerFunc, session *http.Cookie, form url.Values) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/documentals/arxius/entitats-religioses/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	handler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("POST arxiu_entitat_religiosa status=%d body=%s", rr.Code, rr.Body.String())
	}
	return rr.Body.String()
}

func f354ArxiuEntitatViewerSession(t *testing.T, database db.DB, label string) *http.Cookie {
	t.Helper()
	viewer := createTestUser(t, database, "f35_4_viewer_"+label+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	policy := createPolicyWithGrant(t, database, "f35_4_viewer_policy_"+label+"_"+strconv.FormatInt(time.Now().UnixNano(), 10), "documentals.arxius.view")
	addGrantToPolicy(t, database, policy, "territori.confessional.entitats.view")
	assignPolicyToUser(t, database, viewer.ID, policy)
	return createSessionCookie(t, database, viewer.ID, "sess_f35_4_viewer_"+label+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))
}

func readProjectFileF354(t *testing.T, root, rel string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("no s'ha pogut llegir %s: %v", rel, err)
	}
	return string(body)
}
