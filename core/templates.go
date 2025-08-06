package core

import (
	"html/template"
	"log"
	"net/http"
)

var Templates *template.Template

type DataContext struct {
	UserLoggedIn bool
	Data         interface{}
}

func init() {
	var err error

	Templates = template.Must(template.New("").ParseGlob("templates/*.html"))
	if err != nil {
		log.Fatal("Error carregant plantilles:", err)
	}

	Templates = template.Must(Templates.ParseGlob("templates/layouts/*.html"))
	if err != nil {
		log.Fatal("Error afegint layouts:", err)
	}

	log.Println("Plantilles carregades:")
	for _, t := range Templates.Templates() {
		log.Printf(" - %q", t.Name())
	}
}

func RenderTemplate(w http.ResponseWriter, templateName string, data map[string]interface{}) {
	err := Templates.ExecuteTemplate(w, templateName, &DataContext{
		UserLoggedIn: false,
		Data:         data,
	})
	if err != nil {
		log.Printf("Error renderitzant plantilla %s: %v", templateName, err)
		http.Error(w, "Error intern del servidor", http.StatusInternalServerError)
	}
}

func RenderPrivateTemplate(w http.ResponseWriter, tmpl string, data interface{}) {
	err := Templates.ExecuteTemplate(w, "base.html", &DataContext{
		UserLoggedIn: true,
		Data:         data,
	})
	if err != nil {
		log.Printf("Error renderitzant plantilla %s: %v", tmpl, err)
		http.Error(w, "Error intern del servidor", http.StatusInternalServerError)
	}
}
