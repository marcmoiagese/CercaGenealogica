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

// Funcions personalitzades per a les plantilles
var templateFuncs = template.FuncMap{
	"default": func(value interface{}, defaultValue interface{}) interface{} {
		if value == nil || value == "" {
			return defaultValue
		}
		return value
	},
}

func init() {
	var err error

	// Crear template amb funcions personalitzades
	Templates = template.New("").Funcs(templateFuncs)

	// Carregar plantilles
	Templates = template.Must(Templates.ParseGlob("templates/*.html"))
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
		// No cridem http.Error() aquí per evitar "superfluous response.WriteHeader call"
		// ja que ExecuteTemplate ja ha escrit al ResponseWriter
		return
	}
}

func RenderPrivateTemplate(w http.ResponseWriter, tmpl string, data interface{}) {
	err := Templates.ExecuteTemplate(w, tmpl, &DataContext{
		UserLoggedIn: true,
		Data:         data,
	})
	if err != nil {
		log.Printf("Error renderitzant plantilla %s: %v", tmpl, err)
		// No cridem http.Error() aquí per evitar "superfluous response.WriteHeader call"
		// ja que ExecuteTemplate ja ha escrit al ResponseWriter
		return
	}
}
