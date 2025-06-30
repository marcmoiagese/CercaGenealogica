package core

import (
	"html/template"
	"log"
	"net/http"
)

var templates *template.Template

// DataContext – Dades compartides per a totes les plantilles
type DataContext struct {
	UserLoggedIn bool
	Data         interface{}
}

func init() {
	// Carrega totes les plantilles HTML
	var err error
	templates, err = template.ParseGlob("../templates/*.html")
	if err != nil {
		log.Fatal("Error carregant plantilles:", err)
	}

	// Afegeix subplantilles (headers, menus, etc.)
	templates, err = templates.ParseGlob("../templates/layouts/*.html")
	if err != nil {
		log.Fatal("Error afegint layouts:", err)
	}
}

// RenderTemplate – Funció genèrica per renderitzar una plantilla
func RenderTemplate(w http.ResponseWriter, tmpl string, data interface{}) {
	err := templates.ExecuteTemplate(w, tmpl+".html", &DataContext{
		UserLoggedIn: false,
		Data:         data,
	})
	if err != nil {
		log.Printf("Error renderitzant plantilla %s: %v", tmpl, err)
		http.Error(w, "Error intern del servidor", http.StatusInternalServerError)
	}
}

// RenderPrivateTemplate – Per usuari logat
func RenderPrivateTemplate(w http.ResponseWriter, tmpl string, data interface{}) {
	err := templates.ExecuteTemplate(w, tmpl+".html", &DataContext{
		UserLoggedIn: true,
		Data:         data,
	})
	if err != nil {
		log.Printf("Error renderitzant la plantilla %s: %v", tmpl, err)
		http.Error(w, "Error intern del servidor", http.StatusInternalServerError)
	}
}
