package integration

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF353YConfessionalEntityModerationFlow(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3y_entities.sqlite3")
	session := f353YAdminSession(t, database, "entities")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	pendingName := "Parroquia F35-3Y pendent " + suffix
	pendingID := f353YCreateEntitat(t, database, pendingName, "pendent")
	publishedName := "Parroquia F35-3Y publicada " + suffix
	f353YCreateEntitat(t, database, publishedName, "publicat")

	body := f353YGet(t, app.AdminConfessionalSectionList, "/confessional/entitats", session)
	if strings.Contains(body, pendingName) {
		t.Fatalf("la llista normal no ha de mostrar entitats pendents")
	}
	if !strings.Contains(body, publishedName) {
		t.Fatalf("la llista normal ha de mostrar entitats publicades")
	}

	body = f353YGet(t, app.AdminModeracioList, "/moderacio?type=entitat_religiosa", session)
	if !strings.Contains(body, pendingName) {
		t.Fatalf("/moderacio ha de mostrar l'entitat religiosa pendent")
	}

	f353YPostModeracio(t, app.AdminModeracioAprovar, session, pendingID, "entitat_religiosa", "aprovar", "")
	got, err := database.GetEntitatReligiosa(pendingID)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa aprovada: %v", err)
	}
	if got.ModeracioEstat != "publicat" {
		t.Fatalf("estat aprovat=%q, want publicat", got.ModeracioEstat)
	}
	body = f353YGet(t, app.AdminConfessionalSectionList, "/confessional/entitats", session)
	if !strings.Contains(body, pendingName) {
		t.Fatalf("l'entitat aprovada ha d'apareixer a la llista normal")
	}

	rejectedName := "Parroquia F35-3Y rebutjada " + suffix
	rejectedID := f353YCreateEntitat(t, database, rejectedName, "pendent")
	f353YPostModeracio(t, app.AdminModeracioRebutjar, session, rejectedID, "entitat_religiosa", "rebutjar", "duplicada")
	rejected, err := database.GetEntitatReligiosa(rejectedID)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa rebutjada: %v", err)
	}
	if rejected.ModeracioEstat != "rebutjat" {
		t.Fatalf("estat rebutjat=%q, want rebutjat", rejected.ModeracioEstat)
	}
	body = f353YGet(t, app.AdminConfessionalSectionList, "/confessional/entitats", session)
	if strings.Contains(body, rejectedName) {
		t.Fatalf("l'entitat rebutjada no ha d'apareixer a la llista normal")
	}
}

func TestF353YPendingEntityIsNotSelectableAndManipulatedPostIsBlocked(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3y_selectors.sqlite3")
	session := f353YAdminSession(t, database, "selectors")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	pendingName := "Entitat F35-3Y no selectable " + suffix
	pendingID := f353YCreateEntitat(t, database, pendingName, "pendent")
	publishedName := "Entitat F35-3Y selectable " + suffix
	publishedID := f353YCreateEntitat(t, database, publishedName, "publicat")
	municipiID := f353YCreateMunicipi(t, database, "Municipi F35-3Y "+suffix)

	body := f353YGet(t, app.AdminNewConfessional, "/confessional/relacions-entitats/new", session)
	if strings.Contains(body, pendingName) {
		t.Fatalf("el selector de relacions entre entitats no ha de mostrar entitats pendents")
	}
	if !strings.Contains(body, publishedName) {
		t.Fatalf("el selector de relacions entre entitats ha de mostrar entitats publicades")
	}

	form := url.Values{}
	form.Set("kind", "rel_ent")
	form.Set("entitat_origen_id", strconv.Itoa(pendingID))
	form.Set("entitat_desti_id", strconv.Itoa(publishedID))
	form.Set("tipus_relacio", "f35_3y_manipulada")
	body = f353YPostConfessional(t, app.AdminSaveConfessional, session, form)
	if !strings.Contains(body, "han d&#39;estar publicades") && !strings.Contains(body, "han d'estar publicades") {
		t.Fatalf("el POST manipulat amb entitat pendent ha de ser bloquejat, body=%s", body)
	}

	form = url.Values{}
	form.Set("kind", "relacio")
	form.Set("municipi_id", strconv.Itoa(municipiID))
	form.Set("entitat_religiosa_id", strconv.Itoa(pendingID))
	form.Set("tipus_relacio", "f35_3y_territorial_manipulada")
	body = f353YPostConfessional(t, app.AdminSaveConfessional, session, form)
	if !strings.Contains(body, "no esta publicada") {
		t.Fatalf("el POST territorial manipulat amb entitat pendent ha de ser bloquejat, body=%s", body)
	}
}

func TestF353YConfessionalRelationModerationFlow(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3y_relations.sqlite3")
	session := f353YAdminSession(t, database, "relations")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	parentID := f353YCreateEntitat(t, database, "Diocesi F35-3Y "+suffix, "publicat")
	childID := f353YCreateEntitat(t, database, "Parroquia F35-3Y "+suffix, "publicat")
	municipiID := f353YCreateMunicipi(t, database, "Municipi rel F35-3Y "+suffix)

	relType := "jerarquia_f35_3y_" + suffix
	relID, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
		EntitatOrigenID: parentID,
		EntitatDestiID:  childID,
		TipusRelacio:    relType,
		ModeracioEstat:  "pendent",
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosaRelacio: %v", err)
	}
	body := f353YGet(t, app.AdminConfessionalSectionList, "/confessional/relacions-entitats", session)
	if strings.Contains(body, relType) {
		t.Fatalf("la relacio pendent entre entitats no ha d'apareixer a la llista normal")
	}
	body = f353YGet(t, app.AdminModeracioList, "/moderacio?type=entitat_religiosa_relacio", session)
	if !strings.Contains(body, relType) {
		t.Fatalf("/moderacio ha de mostrar la relacio pendent entre entitats")
	}
	f353YPostModeracio(t, app.AdminModeracioAprovar, session, relID, "entitat_religiosa_relacio", "aprovar", "")
	body = f353YGet(t, app.AdminConfessionalSectionList, "/confessional/relacions-entitats", session)
	if !strings.Contains(body, relType) {
		t.Fatalf("la relacio aprovada entre entitats ha d'apareixer a la llista normal")
	}

	territorialType := "territorial_f35_3y_" + suffix
	territorialID, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiID,
		EntitatReligiosaID: childID,
		TipusRelacio:       territorialType,
		ModeracioEstat:     "pendent",
	})
	if err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa: %v", err)
	}
	body = f353YGet(t, app.AdminConfessionalSectionList, "/confessional/municipis-entitats", session)
	if strings.Contains(body, territorialType) {
		t.Fatalf("la relacio territorial pendent no ha d'apareixer a la llista normal")
	}
	body = f353YGet(t, app.AdminModeracioList, "/moderacio?type=municipi_entitat_religiosa", session)
	if !strings.Contains(body, territorialType) {
		t.Fatalf("/moderacio ha de mostrar la relacio territorial pendent")
	}
	f353YPostModeracio(t, app.AdminModeracioAprovar, session, territorialID, "municipi_entitat_religiosa", "aprovar", "")
	body = f353YGet(t, app.AdminConfessionalSectionList, "/confessional/municipis-entitats", session)
	if !strings.Contains(body, territorialType) {
		t.Fatalf("la relacio territorial aprovada ha d'apareixer a la llista normal")
	}
}

func f353YAdminSession(t *testing.T, database db.DB, label string) *http.Cookie {
	t.Helper()
	admin := createTestUser(t, database, "f35_3y_admin_"+label+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, admin.ID, "admin")
	return createSessionCookie(t, database, admin.ID, "sess_f35_3y_"+label+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))
}

func f353YCreateEntitat(t *testing.T, database db.DB, name, status string) int {
	t.Helper()
	id, err := database.SaveEntitatReligiosa(&db.EntitatReligiosa{
		Codi:                   "f35_3y_" + strconv.FormatInt(time.Now().UnixNano(), 10),
		Nom:                    name,
		ReligioConfessioCodi:   "catolicisme_ritu_llati",
		NivellConfessionalCodi: "parroquia",
		Estat:                  "actiu",
		ModeracioEstat:         status,
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosa: %v", err)
	}
	return id
}

func f353YCreateMunicipi(t *testing.T, database db.DB, name string) int {
	t.Helper()
	id, err := database.CreateMunicipi(&db.Municipi{
		Nom:            name,
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi: %v", err)
	}
	return id
}

func f353YGet(t *testing.T, handler http.HandlerFunc, path string, session *http.Cookie) string {
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

func f353YPostConfessional(t *testing.T, handler http.HandlerFunc, session *http.Cookie, form url.Values) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/confessional/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	handler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("POST confessional status=%d body=%s", rr.Code, rr.Body.String())
	}
	return rr.Body.String()
}

func f353YPostModeracio(t *testing.T, handler http.HandlerFunc, session *http.Cookie, id int, objectType, action, motiu string) {
	t.Helper()
	csrf := "csrf_f35_3y_" + action + "_" + strconv.Itoa(id)
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("object_type", objectType)
	form.Set("return_to", "/moderacio?type="+url.QueryEscape(objectType))
	if motiu != "" {
		form.Set("motiu", motiu)
	}
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/moderacio/%d/%s", id, action), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()
	handler(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("POST moderacio %s %s:%d status=%d body=%s", action, objectType, id, rr.Code, rr.Body.String())
	}
}
