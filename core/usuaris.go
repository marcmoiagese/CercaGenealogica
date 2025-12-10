package core

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/mail"
	"os"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
	"golang.org/x/crypto/bcrypt"
)

type Usuari struct {
	ID            int
	Usuari        string // Nou camp per al nom d'usuari
	Nom           string
	Cognoms       string
	Correu        string
	Contrasenya   []byte // Hashed password
	DataNaixament time.Time
	Pais          string
	Estat         string
	Provincia     string
	Poblacio      string
	CodiPostal    string
	DataCreacio   time.Time
	Actiu         bool
}

type IndexPageData struct {
	CSRFToken string
	Error     string
}

type RegistrePageData struct {
	CSRFToken string
	Error     string
}

type RegenerarTokenPageData struct {
	CSRFToken string
	Error     string
}

type ActivacioPageData struct {
	CSRFToken string
	Activat   bool
}

type AvailabilityResponse struct {
	UsernameTaken bool `json:"usernameTaken"`
	EmailTaken    bool `json:"emailTaken"`
}

// ToDBUser – Converteix core.Usuari en db.User
func (u *Usuari) ToDBUser(passwordHash []byte) *db.User {
	return &db.User{
		Usuari:        u.Usuari,
		Name:          u.Nom,
		Surname:       u.Cognoms,
		Email:         u.Correu,
		Password:      passwordHash,
		DataNaixament: u.DataNaixament.Format("2006-01-02"),
		Pais:          u.Pais,
		Estat:         u.Estat,
		Provincia:     u.Provincia,
		Poblacio:      u.Poblacio,
		CodiPostal:    u.CodiPostal,
		Active:        u.Actiu,
	}
}

func (a *App) RegistrarUsuari(w http.ResponseWriter, r *http.Request) {
	ipStr := getIP(r)
	Infof("Iniciant registre d'usuari des de: %s", ipStr)
	lang := ResolveLang(r)

	// Validar CSRF
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		Errorf("Token CSRF invàlid o inexistent en registre")
		http.Error(w, "Error: accés no autoritzat", http.StatusForbidden)
		return
	}

	// Captura els camps del formulari
	r.ParseForm()
	nom := r.FormValue("nom")
	cognoms := r.FormValue("cognoms")
	email := r.FormValue("email")
	password := r.FormValue("contrassenya")
	confirmPassword := r.FormValue("confirmar_contrasenya")
	captcha := r.FormValue("captcha")
	csrf := r.FormValue("csrf_token")
	usuariForm := r.FormValue("usuari")
	acceptaCondicions := r.FormValue("accepta_condicions")

	// Logs per debugar
	Debugf("=== DEBUG REGISTRE ===")
	Debugf("Nom: '%s'", nom)
	Debugf("Cognoms: '%s'", cognoms)
	Debugf("Email: '%s'", email)
	Debugf("Contrasenya: '[oculta]'")
	Debugf("Confirmar contrasenya: '[oculta]'")
	Debugf("CAPTCHA: '%s'", captcha)
	Debugf("CSRF: '%s'", csrf)
	Debugf("Usuari: '%s'", usuariForm)
	Debugf("Accepta condicions: '%s'", acceptaCondicions)
	Debugf("======================")
	Debugf("Valor rebut per a usuari: %s", usuariForm)
	Debugf("Dades rebudes: nom=%s, cognoms=%s, email=%s", nom, cognoms, email)

	// Valida el token CSRF
	// (validat a l'inici amb validateCSRF)

	// Valida que s'acceptin les condicions d'ús
	if acceptaCondicions != "on" {
		Errorf("Error: no s'han acceptat les condicions d'ús")
		RenderTemplate(w, r, "registre-incorrecte.html", RegistrePageData{
			Error: T(lang, "error.accept.terms"),
		})
		return
	}

	// Valida format de correu electrònic
	if _, err := mail.ParseAddress(email); err != nil {
		Errorf("Error: correu electrònic invàlid: %s", email)
		RenderTemplate(w, r, "registre-incorrecte.html", RegistrePageData{
			Error: T(lang, "error.email.invalid"),
		})
		return
	}

	// Validacions bàsiques
	if password != confirmPassword {
		Errorf("Error: les contrasenyes no coincideixen")
		RenderTemplate(w, r, "registre-incorrecte.html", RegistrePageData{
			Error: T(lang, "error.password.mismatch"),
		})
		return
	}
	if captcha != "8" {
		Errorf("Error: CAPTCHA invàlid")
		RenderTemplate(w, r, "registre-incorrecte.html", RegistrePageData{
			Error: T(lang, "error.captcha.invalid"),
		})
		return
	}

	// Comprova duplicats
	if exists, err := a.DB.ExistsUserByUsername(usuariForm); err == nil && exists {
		RenderTemplate(w, r, "registre-incorrecte.html", RegistrePageData{
			Error: T(lang, "error.user.exists"),
		})
		return
	}
	if exists, err := a.DB.ExistsUserByEmail(email); err == nil && exists {
		RenderTemplate(w, r, "registre-incorrecte.html", RegistrePageData{
			Error: T(lang, "error.email.exists"),
		})
		return
	}

	// Genera hash de la contrasenya
	hash, err := generateHash(password)
	if err != nil {
		Errorf("Error generant hash: %v", err)
		http.Error(w, "Error intern", http.StatusInternalServerError)
		return
	}

	user := &Usuari{
		Usuari:        usuariForm,
		Nom:           nom,
		Cognoms:       cognoms,
		Correu:        email,
		Contrasenya:   hash,
		DataNaixament: ParseDate(r.FormValue("data_naixament")),
		DataCreacio:   time.Now(),
		Actiu:         false,
	}

	dbUser := user.ToDBUser(hash)
	Debugf("Convertint usuari: %+v", dbUser)

	err = a.DB.InsertUser(dbUser)
	if err != nil {
		Errorf("ERROR SQL: %v", err)
		RenderTemplate(w, r, "registre-incorrecte.html", RegistrePageData{
			Error: T(lang, "error.user.create"),
		})
		return
	}

	Debugf(" IP de la petició: %s", ipStr)

	Infof("Usuari creat correctament: %s", email)

	// Envia token d'activació
	token := generateToken(32)
	Debugf("Generat token d'activació: %s", token)
	Debugf("Intentant guardar token per a %s", email)
	err = a.DB.SaveActivationToken(email, token)
	if err != nil {
		Errorf("Error guardant token: %v", err)
		http.Error(w, "Error intern", http.StatusInternalServerError)
		return
	} else {
		Debugf("Token i expira_token guardats correctament per a %s", email)
	}

	Debugf("Token d'activació per a %s: %s", email, token)
	Debugf("URL d'activació: http://localhost:8080/activar?token=%s", token)

	// Opcional: envia correu d'activació
	a.sendActivationEmail(email, token)

	// Renderitza la pantalla de confirmació
	RenderTemplate(w, r, "registre-correcte.html", RegistrePageData{
		CSRFToken: "",
		Error:     "",
	})
}

//func isValidCSRF(token string) bool {
// Aquí pots fer servir un sistema real de tokens temporals
//return token == "token-segon"
//}

func generateHash(password string) ([]byte, error) {
	return bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
}

func generateToken(length int) string {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	result := make([]byte, length)
	for i := range result {
		num, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		result[i] = letters[num.Int64()]
	}
	return string(result)
}

/*func hashPassword(p string) ([]byte, error) {
	return bcrypt.GenerateFromPassword([]byte(p), bcrypt.DefaultCost)
}*/

func (a *App) sendActivationEmail(email, token string) {
	if !a.Mail.Enabled {
		Infof("MAIL_ENABLED està desactivat; no s'enviarà correu d'activació a %s", email)
		return
	}

	activationURL := fmt.Sprintf("http://localhost:8080/activar?token=%s", token)
	subject := "Activa el teu compte"
	body := fmt.Sprintf("Hola,\n\nPer activar el teu compte, fes clic a l'enllaç següent:\n%s\n\nSi no has sol·licitat el registre, pots ignorar aquest missatge.\n", activationURL)

	if err := a.Mail.Send(email, subject, body); err != nil {
		Errorf("No s'ha pogut enviar el correu d'activació a %s: %v", email, err)
		return
	}

	Infof("Correu d'activació enviat a %s", email)
}

// redact oculta valors sensibles quan es logueja un formulari
func redact(values []string, key string) []string {
	sensitive := map[string]bool{
		"contrassenya":          true,
		"confirmar_contrasenya": true,
		"password":              true,
		"pwd":                   true,
	}
	if sensitive[strings.ToLower(key)] {
		masked := make([]string, len(values))
		for i, v := range values {
			masked[i] = maskValue(v)
		}
		return masked
	}
	return values
}

func maskValue(v string) string {
	if v == "" {
		return ""
	}
	// Longitud aleatòria (6-12) independent de l'original
	min, max := int64(6), int64(12)
	nBig, err := rand.Int(rand.Reader, big.NewInt(max-min+1))
	if err != nil {
		return "******"
	}
	n := min + nBig.Int64()
	return strings.Repeat("*", int(n))
}

func ParseDate(dateStr string) time.Time {
	t, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return time.Time{}
	}
	return t
}

func (a *App) RegenerarTokenActivacio(w http.ResponseWriter, r *http.Request) {
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "Error: accés no autoritzat", http.StatusForbidden)
		return
	}
	email := r.URL.Query().Get("email")
	if email == "" {
		http.Error(w, "Cal proporcionar el correu electrònic", http.StatusBadRequest)
		return
	}

	usuari, err := a.DB.GetUserByEmail(email)
	if err != nil {
		http.Error(w, "Usuari no trobat", http.StatusNotFound)
		return
	}
	if usuari.Active {
		http.Error(w, "El compte ja està activat", http.StatusBadRequest)
		return
	}
	token := generateToken(32)
	err = a.DB.SaveActivationToken(email, token)
	if err != nil {
		http.Error(w, "No s'ha pogut regenerar el token", http.StatusInternalServerError)
		return
	}
	Infof("Token d'activació regenerat per a %s: %s", email, token)
	Debugf("URL d'activació: http://localhost:8080/activar?token=%s", token)
	RenderTemplate(w, r, "regenerar-token.html", RegenerarTokenPageData{
		Error: "",
	})
}

func (a *App) MostrarFormulariRegenerarToken(w http.ResponseWriter, r *http.Request) {
	RenderTemplate(w, r, "regenerar-token.html", RegenerarTokenPageData{})
}

func (a *App) ProcessarRegenerarToken(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		a.RegenerarTokenActivacio(w, r)
	} else {
		http.Redirect(w, r, "/regenerar-token", http.StatusSeeOther)
	}
}

func (a *App) ActivarUsuariHTTP(w http.ResponseWriter, r *http.Request) {
	// No exigim CSRF aquí perquè és GET via enllaç de correu
	token := r.URL.Query().Get("token")
	if token == "" {
		RenderTemplate(w, r, "activat-user.html", ActivacioPageData{
			Activat: false,
		})
		return
	}

	Infof("Intentant activar usuari amb token: %s", token)
	err := a.DB.ActivateUser(token)
	if err != nil {
		Errorf("Error activant usuari: %v", err)
		RenderTemplate(w, r, "activat-user.html", ActivacioPageData{
			Activat: false,
		})
		return
	}
	Infof("Usuari activat correctament amb token: %s", token)
	RenderTemplate(w, r, "activat-user.html", ActivacioPageData{
		Activat: true,
	})
}

// IniciarSessio – Autentica un usuari i crea una sessió
func (a *App) IniciarSessio(w http.ResponseWriter, r *http.Request) {
	Debugf("IniciarSessio cridada - Mètode: %s", r.Method)
	lang := ResolveLang(r)

	if r.Method != "POST" {
		Debugf("Mètode no permès: %s", r.Method)
		http.Error(w, "Mètode no permès", http.StatusMethodNotAllowed)
		return
	}

	// Validar CSRF
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "Error: accés no autoritzat", http.StatusForbidden)
		return
	}

	ipStr := getIP(r)
	Infof("Intent d'inici de sessió des de: %s", ipStr)

	// Verificar si l'usuari ja està autenticat
	if user, authenticated := a.VerificarSessio(r); authenticated {
		Infof("Usuari %s ja està autenticat, redirigint a /inici", user.Usuari)
		http.Redirect(w, r, "/inici", http.StatusSeeOther)
		return
	}

	// Captura els camps del formulari
	Debugf("Parsejant formulari...")
	Debugf("Content-Type: %s", r.Header.Get("Content-Type"))
	Debugf("Content-Length: %s", r.Header.Get("Content-Length"))

	// Parsejar el formulari primer
	ct := r.Header.Get("Content-Type")
	if strings.HasPrefix(ct, "multipart/form-data") {
		if err := r.ParseMultipartForm(2 << 20); err != nil { // 2MB
			Debugf("Error ParseMultipartForm: %v", err)
		}
	} else {
		if err := r.ParseForm(); err != nil {
			Debugf("Error ParseForm: %v", err)
		}
	}

	// Debug: veure tots els valors del formulari
	Debugf("Tots els valors del formulari:")
	for key, values := range r.Form {
		Debugf("  %s: %v", key, redact(values, key))
	}

	// Debug: veure també els valors de PostForm
	Debugf("Tots els valors de PostForm:")
	for key, values := range r.PostForm {
		Debugf("  %s: %v", key, redact(values, key))
	}

	if r.MultipartForm != nil {
		Debugf("Tots els valors de MultipartForm.Value:")
		for key, values := range r.MultipartForm.Value {
			Debugf("  %s: %v", key, redact(values, key))
		}
	}

	usernameOrEmail := r.FormValue("usuari")
	password := r.FormValue("contrassenya")
	captcha := r.FormValue("captcha")
	mantenirSessio := r.FormValue("mantenir_sessio")

	Debugf("Dades del formulari - Usuari: %s, Contrasenya: [%d chars], CAPTCHA: %s",
		usernameOrEmail, 0, captcha)

	// Validacions bàsiques
	if usernameOrEmail == "" || password == "" {
		Debugf("Validació fallida: usuari o contrasenya buits")
		RenderTemplate(w, r, "index.html", IndexPageData{
			Error: T(lang, "error.login.required"),
		})
		return
	}

	// Validar CAPTCHA
	if captcha != "8" {
		Debugf("CAPTCHA invàlid: %s (esperat: 8)", captcha)
		RenderTemplate(w, r, "index.html", IndexPageData{
			Error: T(lang, "error.captcha.invalid"),
		})
		return
	}

	Debugf("Validacions bàsiques passades, procedint a autenticar...")

	// Autenticar usuari
	user, err := a.DB.AuthenticateUser(usernameOrEmail, password)
	if err != nil {
		Debugf("Error d'autenticació per a %s: %v", usernameOrEmail, err)
		RenderTemplate(w, r, "index.html", IndexPageData{
			Error: T(lang, "error.login.invalid"),
		})
		return
	}

	Infof("Autenticació exitosa per a usuari: %s (ID: %d)", user.Usuari, user.ID)

	// Crear sessió
	sessionID := generateToken(32)
	sessionExpiry := time.Now().Add(24 * time.Hour) // 24 hores per defecte

	if mantenirSessio == "on" {
		sessionExpiry = time.Now().Add(7 * 24 * time.Hour) // 1 setmana si marca el checkbox
	}

	Debugf("Creant sessió amb ID: %s", sessionID)

	// Guardar sessió a la base de dades
	err = a.DB.SaveSession(sessionID, user.ID, sessionExpiry.Format("2006-01-02 15:04:05"))
	if err != nil {
		Errorf("Error guardant sessió: %v", err)
		http.Error(w, "Error intern del servidor", http.StatusInternalServerError)
		return
	}

	Debugf("Sessió guardada a la base de dades")

	// Crear cookie de sessió
	env := strings.ToLower(os.Getenv("ENVIRONMENT"))
	secure := true
	sameSite := http.SameSiteStrictMode
	if env == "development" {
		secure = r.TLS != nil
		sameSite = http.SameSiteLaxMode
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "cg_session",
		Value:    sessionID,
		Expires:  sessionExpiry,
		Path:     "/",
		HttpOnly: true,
		Secure:   secure,   // Secure en entorns reals; en dev depèn de TLS
		SameSite: sameSite, // Strict per reduir CSRF; Lax en dev per comoditat
	})

	Debugf("Cookie de sessió creada (Secure=%v, SameSite=Lax, Expires=%s)", secure, sessionExpiry.Format(time.RFC3339))

	// Redirigir a la pàgina privada
	Debugf("Redirigint a /inici")
	http.Redirect(w, r, "/inici", http.StatusSeeOther)
}

// VerificarSessio – Comprova si un usuari té una sessió vàlida
func (a *App) VerificarSessio(r *http.Request) (*db.User, bool) {
	cookie, err := r.Cookie("cg_session")
	if err != nil {
		Debugf("[VerificarSessio] No s'ha trobat cookie de sessió: %v", err)
		return nil, false
	}

	sessionID := cookie.Value
	if sessionID == "" {
		Debugf("[VerificarSessio] Cookie de sessió buida")
		return nil, false
	}

	Debugf("[VerificarSessio] Verificant sessió: %s", sessionID)

	// Buscar l'usuari associat a aquesta sessió
	user, err := a.DB.GetSessionUser(sessionID)
	if err != nil {
		Debugf("[VerificarSessio] Sessió no vàlida o expirada: %v", err)
		return nil, false
	}

	Debugf("[VerificarSessio] Sessió vàlida per a usuari: %s (ID: %d)", user.Usuari, user.ID)
	return user, true
}

// TancarSessio – elimina la sessió actual (cookie + BD) i redirigeix a l'inici
func (a *App) TancarSessio(w http.ResponseWriter, r *http.Request) {
	lang := ResolveLang(r)

	cookie, err := r.Cookie("cg_session")
	if err != nil || cookie.Value == "" {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}

	sessionID := cookie.Value

	if err := a.DB.DeleteSession(sessionID); err != nil {
		Errorf("[logout] error eliminant sessió %s: %v", sessionID, err)
	}

	// Esborra la cookie
	env := strings.ToLower(os.Getenv("ENVIRONMENT"))
	secure := true
	sameSite := http.SameSiteStrictMode
	if env == "development" {
		secure = r.TLS != nil
		sameSite = http.SameSiteLaxMode
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "cg_session",
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: sameSite,
		Secure:   secure,
	})

	// Missatge opcional via querystring o flash; per ara només redirigim
	redirectTarget := "/"
	// Si vols redirigir a login amb idioma, podríem fer servir lang
	Infof("[logout] sessió %s tancada, redirigint a %s (lang=%s)", sessionID, redirectTarget, lang)
	http.Redirect(w, r, redirectTarget, http.StatusSeeOther)
}

// CheckAvailability – endpoint AJAX per validar si usuari/correu existeixen.
func (a *App) CheckAvailability(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Mètode no permès", http.StatusMethodNotAllowed)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) && !validateCSRF(r, r.Header.Get("X-CSRF-Token")) {
		http.Error(w, "Error: accés no autoritzat", http.StatusForbidden)
		return
	}

	username := r.FormValue("username")
	email := r.FormValue("email")

	resp := AvailabilityResponse{}

	if username != "" {
		if exists, err := a.DB.ExistsUserByUsername(username); err == nil {
			resp.UsernameTaken = exists
		}
	}
	if email != "" {
		if exists, err := a.DB.ExistsUserByEmail(email); err == nil {
			resp.EmailTaken = exists
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
