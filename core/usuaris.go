package core

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
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

func RegistrarUsuari(w http.ResponseWriter, r *http.Request) {
	ipStr := getIP(r)
	log.Printf(" Iniciant registre d'usuari des de: %s", ipStr)

	// Inicialitza la configuració i la base de dades
	config := cnf.LoadConfig("cnf/config.cfg")
	dbInstance, err := db.NewDB(config)
	if err != nil {
		log.Printf("Error inicialitzant la base de dades: %v", err)
		http.Error(w, "Error intern del servidor", http.StatusInternalServerError)
		return
	}
	defer dbInstance.Close()

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
	log.Printf("Valor rebut per a usuari: %s", usuariForm)

	log.Printf("Dades rebudes: nom=%s, cognoms=%s, email=%s", nom, cognoms, email)

	// Comprova si ja està bloquejat per IP
	ip := strings.Split(r.RemoteAddr, ":")[0]
	if IsBlocked(ip) {
		http.Error(w, "Accés denegat", http.StatusForbidden)
		return
	}

	// Aplica límit d'intents
	if !ApplyRateLimit(ip) {
		http.Error(w, "Massa peticions. Intental-ho més tard.", http.StatusTooManyRequests)
		return
	}

	// Valida el token CSRF
	if csrf == "" || !isValidCSRF(csrf) {
		log.Printf(" Token CSRF invàlid: %s", csrf)
		http.Error(w, "Error: accés no autoritzat", http.StatusForbidden)
		return
	}

	// Valida que s'acceptin les condicions d'ús
	if acceptaCondicions != "on" {
		log.Println("Error: no s'han acceptat les condicions d'ús")
		RenderTemplate(w, "registre-incorrecte.html", map[string]interface{}{
			"Error":     "Has d'acceptar les condicions d'ús per continuar",
			"CSRFToken": "token-segon",
		})
		return
	}

	// Validacions bàsiques
	if password != confirmPassword {
		log.Println("Error: les contrasenyes no coincideixen")
		RenderTemplate(w, "registre-incorrecte.html", map[string]interface{}{
			"Error":     "Les contrasenyes no coincideixen",
			"CSRFToken": "token-segon",
		})
		return
	}
	if captcha != "8" {
		log.Println("Error: CAPTCHA invàlid")
		RenderTemplate(w, "registre-incorrecte.html", map[string]interface{}{
			"Error":     "CAPTCHA invàlid",
			"CSRFToken": "token-segon",
		})
		return
	}

	// Genera hash de la contrasenya
	hash, err := generateHash(password)
	if err != nil {
		log.Printf("Error generant hash: %v", err)
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
	log.Printf("Convertint usuari: %+v", dbUser)

	err = dbInstance.InsertUser(dbUser)
	if err != nil {
		log.Printf("ERROR SQL: %v", err)
		RenderTemplate(w, "registre-incorrecte.html", map[string]interface{}{
			"Error":     "Error en crear l'usuari. Potser ja existeix un usuari amb aquest nom o correu electrònic.",
			"CSRFToken": "token-segon",
		})
		return
	}

	log.Printf(" IP de la petició: %s", ipStr)

	log.Printf(" Usuari creat correctament: %s", email)

	// Envia token d'activació
	token := generateToken(32)
	log.Printf("Generat token d'activació: %s", token)
	log.Printf("Intentant guardar token per a %s", email)
	err = dbInstance.SaveActivationToken(email, token)
	if err != nil {
		log.Printf("Error guardant token: %v", err)
		http.Error(w, "Error intern", http.StatusInternalServerError)
		return
	} else {
		log.Printf("Token i expira_token guardats correctament per a %s", email)
	}

	log.Printf("Token d'activació per a %s: %s", email, token)
	log.Printf("URL d'activació: http://localhost:8080/activar?token=%s", token)

	// Opcional: envia correu d'activació
	sendActivationEmail(email, token)

	// Renderitza la pantalla de confirmació
	RenderTemplate(w, "registre-correcte.html", map[string]interface{}{
		"Email":     email,
		"CSRFToken": "token-segon",
	})
}

func isValidCSRF(token string) bool {
	// Aquí pots fer servir un sistema real de tokens temporals
	return token == "token-segon"
}

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

func hashPassword(p string) ([]byte, error) {
	return bcrypt.GenerateFromPassword([]byte(p), bcrypt.DefaultCost)
}

func sendActivationEmail(email, token string) {
	// Simula l'enviament d'un correu
	log.Printf("Enviat token a %s: %s", email, token)
	// Aquí podries cridar a SendGrid, SMTP, etc.
}

func ParseDate(dateStr string) time.Time {
	t, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return time.Time{}
	}
	return t
}

func RegenerarTokenActivacio(w http.ResponseWriter, r *http.Request) {
	email := r.URL.Query().Get("email")
	if email == "" {
		http.Error(w, "Cal proporcionar el correu electrònic", http.StatusBadRequest)
		return
	}
	config := cnf.LoadConfig("cnf/config.cfg")
	dbInstance, err := db.NewDB(config)
	if err != nil {
		http.Error(w, "Error intern del servidor", http.StatusInternalServerError)
		return
	}
	defer dbInstance.Close()

	usuari, err := dbInstance.GetUserByEmail(email)
	if err != nil {
		http.Error(w, "Usuari no trobat", http.StatusNotFound)
		return
	}
	if usuari.Active {
		http.Error(w, "El compte ja està activat", http.StatusBadRequest)
		return
	}
	token := generateToken(32)
	err = dbInstance.SaveActivationToken(email, token)
	if err != nil {
		http.Error(w, "No s'ha pogut regenerar el token", http.StatusInternalServerError)
		return
	}
	log.Printf("Token d'activació regenerat per a %s: %s", email, token)
	log.Printf("URL d'activació: http://localhost:8080/activar?token=%s", token)
	fmt.Fprint(w, "S'ha regenerat el token d'activació. Revisa el teu correu o contacta amb l'administrador.")
}

func MostrarFormulariRegenerarToken(w http.ResponseWriter, r *http.Request) {
	RenderTemplate(w, "regenerar-token.html", map[string]interface{}{
		"CSRFToken": "token-segon",
	})
}

func ProcessarRegenerarToken(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		RegenerarTokenActivacio(w, r)
	} else {
		http.Redirect(w, r, "/regenerar-token", http.StatusSeeOther)
	}
}

func ActivarUsuariHTTP(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	if token == "" {
		RenderTemplate(w, "activat-user.html", map[string]interface{}{
			"Activat":   false,
			"CSRFToken": "token-segon",
		})
		return
	}
	config := cnf.LoadConfig("cnf/config.cfg")
	dbInstance, err := db.NewDB(config)
	if err != nil {
		log.Printf("Error inicialitzant la base de dades: %v", err)
		RenderTemplate(w, "activat-user.html", map[string]interface{}{
			"Activat":   false,
			"CSRFToken": "token-segon",
		})
		return
	}
	defer dbInstance.Close()

	log.Printf("Intentant activar usuari amb token: %s", token)
	err = dbInstance.ActivateUser(token)
	if err != nil {
		log.Printf("Error activant usuari: %v", err)
		RenderTemplate(w, "activat-user.html", map[string]interface{}{
			"Activat":   false,
			"CSRFToken": "token-segon",
		})
		return
	}
	log.Printf("Usuari activat correctament amb token: %s", token)
	RenderTemplate(w, "activat-user.html", map[string]interface{}{
		"Activat":   true,
		"CSRFToken": "token-segon",
	})
}
