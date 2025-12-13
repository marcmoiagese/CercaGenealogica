package core

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/mail"
	"net/url"
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
	Success   string
}

type RecuperarResultPageData struct {
	CSRFToken string
	Error     string
	Success   string
}

type UpdateProfileResponse struct {
	Success string
	Error   string
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
		Address:       "",
		Employment:    "",
		Profession:    "",
		Phone:         "",
		PreferredLang: "",
		SpokenLangs:   "",
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

	// Crea configuració de privacitat per defecte
	if createdUser, err := a.DB.GetUserByEmail(email); err == nil && createdUser != nil {
		if err := a.DB.CreatePrivacyDefaults(createdUser.ID); err != nil {
			Errorf("No s'ha pogut crear privacitat per defecte per a %s: %v", email, err)
		}
	}

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
	a.sendActivationEmail(email, token, lang, "email.activation")

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

func (a *App) sendActivationEmail(email, token, lang, keyPrefix string) {
	if !a.Mail.Enabled {
		Infof("MAIL_ENABLED està desactivat; no s'enviarà correu d'activació a %s", email)
		return
	}

	activationURL := fmt.Sprintf("http://localhost:8080/activar?token=%s", token)
	subject := T(lang, keyPrefix+".subject")
	body := fmt.Sprintf(T(lang, keyPrefix+".body"), activationURL)

	if err := a.Mail.Send(email, subject, body); err != nil {
		Errorf("No s'ha pogut enviar el correu d'activació a %s: %v", email, err)
		return
	}

	Infof("Correu d'activació enviat a %s", email)
}

func (a *App) sendPasswordResetEmail(email, url, lang string) {
	if !a.Mail.Enabled {
		Infof("MAIL_ENABLED està desactivat; no s'enviarà correu de recuperació a %s", email)
		return
	}
	subject := T(lang, "email.reset.subject")
	body := fmt.Sprintf(T(lang, "email.reset.body"), url)
	if err := a.Mail.Send(email, subject, body); err != nil {
		Errorf("No s'ha pogut enviar el correu de recuperació a %s: %v", email, err)
		return
	}
	Infof("Correu de recuperació enviat a %s", email)
}

func (a *App) sendPasswordResetCompletedEmail(email, password, lang string) {
	if !a.Mail.Enabled {
		Infof("MAIL_ENABLED està desactivat; no s'enviarà correu amb la nova contrasenya a %s", email)
		return
	}
	subject := T(lang, "email.reset.complete.subject")
	body := fmt.Sprintf(T(lang, "email.reset.complete.body"), password)
	if err := a.Mail.Send(email, subject, body); err != nil {
		Errorf("No s'ha pogut enviar el correu amb la nova contrasenya a %s: %v", email, err)
		return
	}
	Infof("Correu amb nova contrasenya enviat a %s", email)
}

func (a *App) sendEmailChangeConfirm(email, token, lang string) {
	if !a.Mail.Enabled {
		return
	}
	url := fmt.Sprintf("http://localhost:8080/perfil/email-confirm?token=%s", token)
	subject := T(lang, "email.change.confirm.subject")
	body := fmt.Sprintf(T(lang, "email.change.confirm.body"), url)
	if err := a.Mail.Send(email, subject, body); err != nil {
		Errorf("No s'ha pogut enviar correu de confirmació de canvi d'email a %s: %v", email, err)
	}
}

func (a *App) sendEmailChangeRevert(oldEmail, newEmail, token, lang string) {
	if !a.Mail.Enabled {
		return
	}
	url := fmt.Sprintf("http://localhost:8080/perfil/email-revert?token=%s", token)
	subject := T(lang, "email.change.revert.subject")
	body := fmt.Sprintf(T(lang, "email.change.revert.body"), newEmail, url)
	if err := a.Mail.Send(oldEmail, subject, body); err != nil {
		Errorf("No s'ha pogut enviar correu de revert de canvi d'email a %s: %v", oldEmail, err)
	}
}

func (a *App) sendPasswordChangedEmail(email, lang string) {
	if !a.Mail.Enabled {
		return
	}
	subject := T(lang, "email.password.changed.subject")
	body := T(lang, "email.password.changed.body")
	if err := a.Mail.Send(email, subject, body); err != nil {
		Errorf("No s'ha pogut enviar correu de canvi de contrasenya a %s: %v", email, err)
	}
}

func generateSecurePassword(length int) string {
	lower := "abcdefghijklmnopqrstuvwxyz"
	upper := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	digits := "0123456789"
	symbols := "!@#$%^&*()-_=+[]{}<>?"
	all := lower + upper + digits + symbols

	result := make([]byte, length)

	// Garantir almenys un caràcter de cada grup principal
	classes := []string{lower, upper, digits, symbols}
	for i := 0; i < 4 && i < length; i++ {
		num, _ := rand.Int(rand.Reader, big.NewInt(int64(len(classes[i]))))
		result[i] = classes[i][num.Int64()]
	}

	for i := 4; i < length; i++ {
		num, _ := rand.Int(rand.Reader, big.NewInt(int64(len(all))))
		result[i] = all[num.Int64()]
	}

	// barreja aleatòriament
	for i := len(result) - 1; i > 0; i-- {
		jBig, _ := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		j := int(jBig.Int64())
		result[i], result[j] = result[j], result[i]
	}

	return string(result)
}

func writeJSONMessage(w http.ResponseWriter, ok bool, msg string) {
	w.Header().Set("Content-Type", "application/json")
	resp := map[string]string{
		"message": msg,
	}
	if !ok {
		resp["error"] = msg
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func wantsJSON(r *http.Request) bool {
	accept := r.Header.Get("Accept")
	xrw := strings.ToLower(r.Header.Get("X-Requested-With"))
	ct := r.Header.Get("Content-Type")
	if strings.Contains(accept, "application/json") || strings.Contains(ct, "application/json") {
		return true
	}
	if xrw == "xmlhttprequest" {
		return true
	}
	return false
}

func writeRecoverResponse(w http.ResponseWriter, r *http.Request, lang string, status int, ok bool, msg string) {
	if wantsJSON(r) {
		w.WriteHeader(status)
		writeJSONMessage(w, ok, msg)
		return
	}

	if status >= 400 {
		w.WriteHeader(status)
	}
	RenderTemplate(w, r, "recover-result.html", RecuperarResultPageData{
		Error:   ifThen(!ok && status >= 400, msg, ""),
		Success: ifThen(ok, msg, ""),
	})
}

func ifThen(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
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
		lang := ResolveLang(r)
		w.WriteHeader(http.StatusForbidden)
		RenderTemplate(w, r, "regenerar-token.html", RegenerarTokenPageData{
			Error: T(lang, "error.csrf"),
		})
		return
	}
	lang := ResolveLang(r)
	email := strings.TrimSpace(r.FormValue("email"))
	if email == "" {
		w.WriteHeader(http.StatusBadRequest)
		RenderTemplate(w, r, "regenerar-token.html", RegenerarTokenPageData{
			Error: T(lang, "regenerate.error.emailRequired"),
		})
		return
	}

	usuari, err := a.DB.GetUserByEmail(email)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		RenderTemplate(w, r, "regenerar-token.html", RegenerarTokenPageData{
			Error: T(lang, "regenerate.error.notFound"),
		})
		return
	}
	if usuari.Active {
		w.WriteHeader(http.StatusBadRequest)
		RenderTemplate(w, r, "regenerar-token.html", RegenerarTokenPageData{
			Error: T(lang, "regenerate.error.alreadyActive"),
		})
		return
	}
	token := generateToken(32)
	err = a.DB.SaveActivationToken(email, token)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		RenderTemplate(w, r, "regenerar-token.html", RegenerarTokenPageData{
			Error: T(lang, "regenerate.error.save"),
		})
		return
	}
	Infof("Token d'activació regenerat per a %s: %s", email, token)
	Debugf("URL d'activació: http://localhost:8080/activar?token=%s", token)

	// Envia nou correu d'activació amb la mateixa lògica de localització
	a.sendActivationEmail(email, token, lang, "email.activation.regen")

	RenderTemplate(w, r, "regenerar-token.html", RegenerarTokenPageData{
		Success: T(lang, "regenerate.success"),
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

// Perfil – mostra la pàgina d'ajustos de compte per a usuaris autenticats.
func (a *App) Perfil(w http.ResponseWriter, r *http.Request) {
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	lang := ResolveLang(r)
	if pref := strings.TrimSpace(user.PreferredLang); pref != "" && isSupportedLang(pref) {
		lang = pref
		setLangCookie(w, r, lang)
	}

	var privacy *db.PrivacySettings
	if p, err := a.DB.GetPrivacySettings(user.ID); err == nil {
		privacy = p
	}
	if privacy == nil {
		privacy = defaultPrivacySettings()
	}

	memberSince := formatDateDisplay(user.CreatedAt)
	birthInput := formatDateInput(user.DataNaixament)

	spokenSlice := []string{}
	if strings.TrimSpace(user.SpokenLangs) != "" {
		for _, v := range strings.Split(user.SpokenLangs, ",") {
			if t := strings.TrimSpace(v); t != "" {
				spokenSlice = append(spokenSlice, t)
			}
		}
	}
	spokenSet := map[string]bool{}
	for _, v := range spokenSlice {
		spokenSet[v] = true
	}

	activeTab := r.URL.Query().Get("tab")
	switch activeTab {
	case "generals", "contrasenya", "privacitat", "eliminar":
	default:
		activeTab = "generals"
	}

	canManageArxius := a.CanManageArxius(user)

	RenderPrivateTemplateLang(w, r, "perfil.html", lang, map[string]interface{}{
		"User":               user,
		"Privacy":            privacy,
		"MemberSince":        memberSince,
		"BirthInputValue":    birthInput,
		"MemberSinceDisplay": memberSince,
		"Success":            r.URL.Query().Get("success"),
		"Error":              r.URL.Query().Get("error"),
		"ActiveTab":          activeTab,
		"LangOptions":        []string{"cat", "en", "oc"},
		"SpokenSlice":        spokenSlice,
		"SpokenSet":          spokenSet,
		"CanManageArxius":    canManageArxius,
	})
}

func formatDateInput(dateStr string) string {
	if dateStr == "" {
		return ""
	}
	layouts := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, dateStr); err == nil {
			return t.Format("2006-01-02")
		}
	}
	return ""
}

func formatDateDisplay(dateStr string) string {
	if dateStr == "" {
		return ""
	}
	layouts := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, dateStr); err == nil {
			return t.Format("02/01/2006")
		}
	}
	return dateStr
}

func defaultPrivacySettings() *db.PrivacySettings {
	return &db.PrivacySettings{
		NomVisibility:       "private",
		CognomsVisibility:   "private",
		EmailVisibility:     "private",
		BirthVisibility:     "private",
		PaisVisibility:      "public",
		EstatVisibility:     "private",
		ProvinciaVisibility: "private",
		PoblacioVisibility:  "private",
		PostalVisibility:    "private",
		AddressVisibility:   "private",
		EmploymentVisibility: "private",
		ProfessionVisibility: "private",
		PhoneVisibility:     "private",
		PreferredLangVisibility: "private",
		SpokenLangsVisibility: "private",
		ShowActivity:        true,
		ProfilePublic:       true,
		NotifyEmail:         true,
		AllowContact:        true,
	}
}

func resolveUserLang(r *http.Request, user *db.User) string {
	lang := ResolveLang(r)
	if user != nil {
		if l := normalizeLang(strings.TrimSpace(user.PreferredLang)); l != "" && isSupportedLang(l) {
			return l
		}
	}
	return lang
}

func setLangCookie(w http.ResponseWriter, r *http.Request, lang string) {
	if lang == "" {
		return
	}
	env := strings.ToLower(os.Getenv("ENVIRONMENT"))
	secure := true
	sameSite := http.SameSiteStrictMode
	if env == "development" {
		secure = r.TLS != nil
		sameSite = http.SameSiteLaxMode
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "lang",
		Value:    lang,
		Path:     "/",
		HttpOnly: false,
		SameSite: sameSite,
		Secure:   secure,
	})
}

// ActualitzarPerfilDades – Desa dades generals i privacitat; gestiona canvi de correu amb confirmació.
func (a *App) ActualitzarPerfilDades(w http.ResponseWriter, r *http.Request) {
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	lang := ResolveLang(r)
	if pref := strings.TrimSpace(user.PreferredLang); pref != "" && isSupportedLang(pref) {
		lang = pref
		setLangCookie(w, r, lang)
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/perfil?tab=generals", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Redirect(w, r, "/perfil?tab=generals&error="+url.QueryEscape(T(lang, "error.csrf")), http.StatusSeeOther)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/perfil?tab=generals&error="+url.QueryEscape(T(lang, "error.user.create")), http.StatusSeeOther)
		return
	}

	originalEmail := user.Email
	newEmail := strings.TrimSpace(r.FormValue("correu"))

	user.Name = r.FormValue("nom")
	user.Surname = r.FormValue("cognoms")
	user.DataNaixament = r.FormValue("data_naixement")
	user.Pais = r.FormValue("pais")
	user.Estat = r.FormValue("estat")
	user.Provincia = r.FormValue("provincia")
	user.Poblacio = r.FormValue("poblacio")
	user.CodiPostal = r.FormValue("codi_postal")
	user.Address = r.FormValue("adreca")
	user.Employment = r.FormValue("situacio_laboral")
	user.Profession = r.FormValue("professio")
	user.Phone = r.FormValue("telefon")
	user.PreferredLang = r.FormValue("idioma_preferit")
	user.SpokenLangs = strings.TrimSpace(r.FormValue("idiomes_parla"))

	if err := a.DB.UpdateUserProfile(user); err != nil {
		http.Redirect(w, r, "/perfil?tab=generals&error="+url.QueryEscape(T(lang, "error.user.create")), http.StatusSeeOther)
		return
	}

	privacy := defaultPrivacySettings()
	if current, err := a.DB.GetPrivacySettings(user.ID); err == nil && current != nil {
		privacy = current
	}
	// Actualitza només els camps presents al formulari de dades generals
	privacy.UserID = user.ID
	privacy.NomVisibility = visibilityValue(r.FormValue("nom_visibility"))
	privacy.CognomsVisibility = visibilityValue(r.FormValue("cognoms_visibility"))
	privacy.EmailVisibility = visibilityValue(r.FormValue("correu_visibility"))
	privacy.BirthVisibility = visibilityValue(r.FormValue("naixement_visibility"))
	privacy.PaisVisibility = visibilityValue(r.FormValue("pais_visibility"))
	privacy.EstatVisibility = visibilityValue(r.FormValue("estat_visibility"))
	privacy.ProvinciaVisibility = visibilityValue(r.FormValue("provincia_visibility"))
	privacy.PoblacioVisibility = visibilityValue(r.FormValue("poblacio_visibility"))
	privacy.PostalVisibility = visibilityValue(r.FormValue("codi_postal_visibility"))
	privacy.AddressVisibility = visibilityValue(r.FormValue("adreca_visibility"))
	privacy.EmploymentVisibility = visibilityValue(r.FormValue("situacio_visibility"))
	privacy.ProfessionVisibility = visibilityValue(r.FormValue("professio_visibility"))
	privacy.PhoneVisibility = visibilityValue(r.FormValue("telefon_visibility"))
	privacy.PreferredLangVisibility = visibilityValue(r.FormValue("idioma_preferit_visibility"))
	privacy.SpokenLangsVisibility = visibilityValue(r.FormValue("idiomes_parla_visibility"))
	privacy.ShowActivity = r.FormValue("mostrar_estadistiques_public") == "on"
	// ProfilePublic, NotifyEmail i AllowContact provenen del formulari de privacitat; els mantenim.

	_ = a.DB.SavePrivacySettings(user.ID, privacy)

	// Si l'email ha canviat, iniciar procés de confirmació
	if newEmail != "" && newEmail != originalEmail {
		if _, err := mail.ParseAddress(newEmail); err == nil {
			confirmToken := generateToken(32)
			revertToken := generateToken(32)
			expConfirm := time.Now().Add(24 * time.Hour).Format("2006-01-02 15:04:05")
			expRevert := time.Now().Add(365 * 24 * time.Hour).Format("2006-01-02 15:04:05")
			if err := a.DB.CreateEmailChange(user.ID, newEmail, confirmToken, expConfirm, revertToken, expRevert, lang); err == nil {
				a.sendEmailChangeConfirm(newEmail, confirmToken, lang)
				http.Redirect(w, r, "/perfil?tab=generals&success="+url.QueryEscape(T(lang, "profile.email.change.pending")), http.StatusSeeOther)
				return
			}
		}
		http.Redirect(w, r, "/perfil?tab=generals&error="+url.QueryEscape(T(lang, "profile.email.change.invalid")), http.StatusSeeOther)
		return
	}

	http.Redirect(w, r, "/perfil?tab=generals&success="+url.QueryEscape(T(lang, "profile.save.success")), http.StatusSeeOther)
}

// ActualitzarPerfilPrivacitat – desa només preferències de privacitat/comunicacions.
func (a *App) ActualitzarPerfilPrivacitat(w http.ResponseWriter, r *http.Request) {
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	lang := ResolveLang(r)
	if pref := strings.TrimSpace(user.PreferredLang); pref != "" && isSupportedLang(pref) {
		lang = pref
		setLangCookie(w, r, lang)
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/perfil?tab=privacitat", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Redirect(w, r, "/perfil?tab=privacitat&error="+url.QueryEscape(T(lang, "error.csrf")), http.StatusSeeOther)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/perfil?tab=privacitat&error="+url.QueryEscape(T(lang, "error.user.create")), http.StatusSeeOther)
		return
	}

	privacy, err := a.DB.GetPrivacySettings(user.ID)
	if err != nil || privacy == nil {
		privacy = defaultPrivacySettings()
	}

	privacy.ProfilePublic = r.FormValue("perfil_visibility") != "private"
	privacy.NotifyEmail = r.FormValue("notificacions_correu") == "on"
	privacy.AllowContact = r.FormValue("contacte_altres_usuaris") == "on"

	if err := a.DB.SavePrivacySettings(user.ID, privacy); err != nil {
		http.Redirect(w, r, "/perfil?tab=privacitat&error="+url.QueryEscape(T(lang, "profile.save.error")), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/perfil?tab=privacitat&success="+url.QueryEscape(T(lang, "profile.save.success")), http.StatusSeeOther)
}

// ActualitzarPerfilContrasenya – valida la contrasenya actual i actualitza a una de nova.
func (a *App) ActualitzarPerfilContrasenya(w http.ResponseWriter, r *http.Request) {
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	lang := ResolveLang(r)
	if pref := strings.TrimSpace(user.PreferredLang); pref != "" && isSupportedLang(pref) {
		lang = pref
		setLangCookie(w, r, lang)
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/perfil?tab=contrasenya", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Redirect(w, r, "/perfil?tab=contrasenya&error="+url.QueryEscape(T(lang, "error.csrf")), http.StatusSeeOther)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/perfil?tab=contrasenya&error="+url.QueryEscape(T(lang, "profile.password.error.generic")), http.StatusSeeOther)
		return
	}

	current := r.FormValue("contrasenya_actual")
	newPass := r.FormValue("nova_contrasenya")
	confirm := r.FormValue("confirmar_contrasenya")

	if current == "" || newPass == "" || confirm == "" {
		http.Redirect(w, r, "/perfil?tab=contrasenya&error="+url.QueryEscape(T(lang, "profile.password.error.required")), http.StatusSeeOther)
		return
	}
	if newPass != confirm {
		http.Redirect(w, r, "/perfil?tab=contrasenya&error="+url.QueryEscape(T(lang, "profile.password.error.mismatch")), http.StatusSeeOther)
		return
	}
	if err := bcrypt.CompareHashAndPassword(user.Password, []byte(current)); err != nil {
		http.Redirect(w, r, "/perfil?tab=contrasenya&error="+url.QueryEscape(T(lang, "profile.password.error.current")), http.StatusSeeOther)
		return
	}
	if err := bcrypt.CompareHashAndPassword(user.Password, []byte(newPass)); err == nil {
		http.Redirect(w, r, "/perfil?tab=contrasenya&error="+url.QueryEscape(T(lang, "profile.password.error.same")), http.StatusSeeOther)
		return
	}

	hash, err := generateHash(newPass)
	if err != nil {
		http.Redirect(w, r, "/perfil?tab=contrasenya&error="+url.QueryEscape(T(lang, "profile.password.error.generic")), http.StatusSeeOther)
		return
	}
	if err := a.DB.UpdateUserPassword(user.ID, hash); err != nil {
		http.Redirect(w, r, "/perfil?tab=contrasenya&error="+url.QueryEscape(T(lang, "profile.password.error.generic")), http.StatusSeeOther)
		return
	}

	a.sendPasswordChangedEmail(user.Email, lang)
	http.Redirect(w, r, "/perfil?tab=contrasenya&success="+url.QueryEscape(T(lang, "profile.password.success")), http.StatusSeeOther)
}

func visibilityValue(v string) string {
	if strings.ToLower(v) == "public" {
		return "public"
	}
	return "private"
}

func (a *App) ConfirmarCanviEmail(w http.ResponseWriter, r *http.Request) {
	lang := ResolveLang(r)
	token := r.URL.Query().Get("token")
	if token == "" {
		http.Redirect(w, r, "/perfil?error="+url.QueryEscape(T(lang, "profile.email.change.invalid")), http.StatusSeeOther)
		return
	}
	change, err := a.DB.ConfirmEmailChange(token)
	if err != nil {
		http.Redirect(w, r, "/perfil?error="+url.QueryEscape(T(lang, "profile.email.change.invalid")), http.StatusSeeOther)
		return
	}
	if err := a.DB.UpdateUserEmail(change.UserID, change.NewEmail); err != nil {
		http.Redirect(w, r, "/perfil?error="+url.QueryEscape(T(lang, "profile.email.change.invalid")), http.StatusSeeOther)
		return
	}
	if helper, ok := a.DB.(interface{ markEmailChangeConfirmed(id int) error }); ok {
		_ = helper.markEmailChangeConfirmed(change.ID)
	}
	a.sendEmailChangeRevert(change.OldEmail, change.NewEmail, change.TokenRevert, change.Lang)
	http.Redirect(w, r, "/perfil?success="+url.QueryEscape(T(change.Lang, "profile.email.change.confirmed")), http.StatusSeeOther)
}

func (a *App) RevertirCanviEmail(w http.ResponseWriter, r *http.Request) {
	lang := ResolveLang(r)
	token := r.URL.Query().Get("token")
	if token == "" {
		http.Redirect(w, r, "/perfil?error="+url.QueryEscape(T(lang, "profile.email.change.invalid")), http.StatusSeeOther)
		return
	}
	change, err := a.DB.RevertEmailChange(token)
	if err != nil {
		http.Redirect(w, r, "/perfil?error="+url.QueryEscape(T(lang, "profile.email.change.invalid")), http.StatusSeeOther)
		return
	}
	if err := a.DB.UpdateUserEmail(change.UserID, change.OldEmail); err != nil {
		http.Redirect(w, r, "/perfil?error="+url.QueryEscape(T(lang, "profile.email.change.invalid")), http.StatusSeeOther)
		return
	}
	if helper, ok := a.DB.(interface{ markEmailChangeReverted(id int) error }); ok {
		_ = helper.markEmailChangeReverted(change.ID)
	}
	http.Redirect(w, r, "/perfil?success="+url.QueryEscape(T(change.Lang, "profile.email.change.reverted")), http.StatusSeeOther)
}

// GestionarRecuperacio gestiona POST de sol·licitud i GET del token de recuperació
func (a *App) GestionarRecuperacio(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		a.SolicitarRecuperarContrasenya(w, r)
		return
	}
	if r.Method == "GET" && r.URL.Query().Get("token") != "" {
		a.ValidarRecuperarContrasenya(w, r)
		return
	}
	http.Error(w, "Mètode no permès", http.StatusMethodNotAllowed)
}

// SolicitarRecuperarContrasenya processa la petició de recuperació.
func (a *App) SolicitarRecuperarContrasenya(w http.ResponseWriter, r *http.Request) {
	lang := ResolveLang(r)

	if !validateCSRF(r, r.FormValue("csrf_token")) {
		w.WriteHeader(http.StatusForbidden)
		writeRecoverResponse(w, r, lang, http.StatusForbidden, false, T(lang, "error.csrf"))
		return
	}

	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeRecoverResponse(w, r, lang, http.StatusBadRequest, false, T(lang, "recover.error.generic"))
		return
	}

	email := strings.TrimSpace(r.FormValue("email"))
	captcha := r.FormValue("captcha")

	if email == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeRecoverResponse(w, r, lang, http.StatusBadRequest, false, T(lang, "regenerate.error.emailRequired"))
		return
	}
	if _, err := mail.ParseAddress(email); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeRecoverResponse(w, r, lang, http.StatusBadRequest, false, T(lang, "error.email.invalid"))
		return
	}
	if captcha != "8" {
		w.WriteHeader(http.StatusBadRequest)
		writeRecoverResponse(w, r, lang, http.StatusBadRequest, false, T(lang, "error.captcha.invalid"))
		return
	}

	token := generateToken(32)
	expiry := time.Now().Add(24 * time.Hour).Format("2006-01-02 15:04:05")

	created, err := a.DB.CreatePasswordReset(email, token, expiry, lang)
	if err != nil {
		Errorf("Error creant sol·licitud de recuperació per %s: %v", email, err)
		// No revelem l'error, retornem missatge genèric
		writeRecoverResponse(w, r, lang, http.StatusOK, true, T(lang, "recover.info.sent"))
		return
	}

	if created {
		resetURL := fmt.Sprintf("http://localhost:8080/recuperar?token=%s", token)
		a.sendPasswordResetEmail(email, resetURL, lang)
	}

	writeRecoverResponse(w, r, lang, http.StatusOK, true, T(lang, "recover.info.sent"))
}

// ValidarRecuperarContrasenya valida el token i genera una nova contrasenya enviada per correu.
func (a *App) ValidarRecuperarContrasenya(w http.ResponseWriter, r *http.Request) {
	lang := ResolveLang(r)
	token := strings.TrimSpace(r.URL.Query().Get("token"))
	if token == "" {
		w.WriteHeader(http.StatusBadRequest)
		RenderTemplate(w, r, "recover-result.html", RecuperarResultPageData{
			Error: T(lang, "recover.result.invalid"),
		})
		return
	}

	req, err := a.DB.GetPasswordReset(token)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		RenderTemplate(w, r, "recover-result.html", RecuperarResultPageData{
			Error: T(lang, "recover.result.invalid"),
		})
		return
	}

	newPass := generateSecurePassword(16)
	hash, err := generateHash(newPass)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		RenderTemplate(w, r, "recover-result.html", RecuperarResultPageData{
			Error: T(lang, "recover.result.error"),
		})
		return
	}

	if err := a.DB.UpdateUserPassword(req.UserID, hash); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		RenderTemplate(w, r, "recover-result.html", RecuperarResultPageData{
			Error: T(lang, "recover.result.error"),
		})
		return
	}

	// Marquem la petició com usada (best-effort)
	if err := a.DB.MarkPasswordResetUsed(req.ID); err != nil {
		Errorf("No s'ha pogut marcar password_resets %d com usada: %v", req.ID, err)
	}

	resetLang := req.Lang
	if resetLang == "" {
		resetLang = lang
	}
	a.sendPasswordResetCompletedEmail(req.Email, newPass, resetLang)

	RenderTemplate(w, r, "recover-result.html", RecuperarResultPageData{
		Success: T(lang, "recover.result.sent"),
	})
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
