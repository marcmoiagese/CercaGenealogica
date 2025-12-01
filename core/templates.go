package core

import (
	"html/template"
	"log"
	"net/http"
	"reflect"
)

var Templates *template.Template

type DataContext struct {
	UserLoggedIn bool
	Lang         string
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
	"t": func(lang, key string) string {
		return T(lang, key)
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

	Infof("Plantilles carregades:")
	for _, t := range Templates.Templates() {
		Debugf(" - %q", t.Name())
	}
}

// LogLoadedTemplates – permet registrar plantilles carregades quan es canvia el nivell de log
func LogLoadedTemplates() {
	if Templates == nil {
		return
	}
	Infof("Plantilles carregades:")
	for _, t := range Templates.Templates() {
		Debugf(" - %q", t.Name())
	}
}

func RenderTemplate(w http.ResponseWriter, r *http.Request, templateName string, data interface{}) {
	lang := ResolveLang(r)
	csrfToken, _ := ensureCSRF(w, r)
	data = injectCSRFToken(data, csrfToken)
	err := Templates.ExecuteTemplate(w, templateName, &DataContext{
		UserLoggedIn: false,
		Lang:         lang,
		Data:         data,
	})
	if err != nil {
		Errorf("Error renderitzant plantilla %s: %v", templateName, err)
		// No cridem http.Error() aquí per evitar "superfluous response.WriteHeader call"
		// ja que ExecuteTemplate ja ha escrit al ResponseWriter
		return
	}
}

func RenderPrivateTemplate(w http.ResponseWriter, r *http.Request, tmpl string, data interface{}) {
	lang := ResolveLang(r)
	csrfToken, _ := ensureCSRF(w, r)
	data = injectCSRFToken(data, csrfToken)
	err := Templates.ExecuteTemplate(w, tmpl, &DataContext{
		UserLoggedIn: true,
		Lang:         lang,
		Data:         data,
	})
	if err != nil {
		Errorf("Error renderitzant plantilla %s: %v", tmpl, err)
		// No cridem http.Error() aquí per evitar "superfluous response.WriteHeader call"
		// ja que ExecuteTemplate ja ha escrit al ResponseWriter
		return
	}
}

// injectCSRFToken insereix CSRFToken i retorna el data (map o struct) amb el token aplicat.
func injectCSRFToken(data interface{}, token string) interface{} {
	if data == nil {
		return map[string]interface{}{"CSRFToken": token}
	}
	if m, ok := data.(map[string]interface{}); ok {
		m["CSRFToken"] = token
		return m
	}

	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return map[string]interface{}{"CSRFToken": token}
		}
		elem := v.Elem()
		if elem.Kind() == reflect.Struct {
			field := elem.FieldByName("CSRFToken")
			if field.IsValid() && field.CanSet() && field.Kind() == reflect.String {
				field.SetString(token)
				return data
			}
		}
	}

	if v.Kind() == reflect.Struct {
		// crear còpia addressable
		copyVal := reflect.New(v.Type()).Elem()
		copyVal.Set(v)
		field := copyVal.FieldByName("CSRFToken")
		if field.IsValid() && field.CanSet() && field.Kind() == reflect.String {
			field.SetString(token)
			// retorna pointer a la còpia per mantenir addressable
			ptr := copyVal.Addr().Interface()
			return ptr
		}
	}

	return data
}
