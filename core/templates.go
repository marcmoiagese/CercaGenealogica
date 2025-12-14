package core

import (
	"database/sql"
	"html/template"
	"log"
	"net/http"
	"path/filepath"
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
	"index": func(m map[string]interface{}, k string) interface{} {
		if m == nil {
			return nil
		}
		return m[k]
	},
	"list": func(values ...string) []string {
		return values
	},
	"add": func(a, b int) int {
		return a + b
	},
	// idx: accés segur a slices/arrays/mapes via reflect
	"idx": func(collection interface{}, index int) interface{} {
		v := reflect.ValueOf(collection)
		if !v.IsValid() {
			return nil
		}
		switch v.Kind() {
		case reflect.Slice, reflect.Array:
			if index >= 0 && index < v.Len() {
				return v.Index(index).Interface()
			}
		case reflect.Map:
			key := reflect.ValueOf(index)
			if v.MapIndex(key).IsValid() {
				return v.MapIndex(key).Interface()
			}
		}
		return nil
	},
	"int": func(v interface{}) int {
		switch t := v.(type) {
		case int:
			return t
		case int8:
			return int(t)
		case int16:
			return int(t)
		case int32:
			return int(t)
		case int64:
			return int(t)
		case uint:
			return int(t)
		case uint8:
			return int(t)
		case uint16:
			return int(t)
		case uint32:
			return int(t)
		case uint64:
			return int(t)
		case sql.NullInt64:
			if t.Valid {
				return int(t.Int64)
			}
			return 0
		default:
			return 0
		}
	},
}

func init() {
	// Crear template amb funcions personalitzades
	Templates = template.New("").Funcs(templateFuncs)

	// Carregar plantilles
	parsePattern := func(pattern string) {
		matches, gerr := filepath.Glob(pattern)
		if gerr != nil {
			log.Printf("Error buscant plantilles amb patró %s: %v", pattern, gerr)
			return
		}
		if len(matches) == 0 {
			Debugf("Cap plantilla trobada per al patró %s (omitint)", pattern)
			return
		}
		Templates = template.Must(Templates.ParseFiles(matches...))
	}

	parsePattern("templates/*.html")
	parsePattern("templates/layouts/*.html")
	parsePattern("templates/admin/*.html")

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
	data = injectUserIfMissing(r, injectCSRFToken(data, csrfToken))
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

// RenderPrivateTemplateLang permet forçar l'idioma (p.ex. idioma preferit de l'usuari logat).
func RenderPrivateTemplateLang(w http.ResponseWriter, r *http.Request, tmpl string, lang string, data interface{}) {
	csrfToken, _ := ensureCSRF(w, r)
	data = injectUserIfMissing(r, injectCSRFToken(data, csrfToken))
	err := Templates.ExecuteTemplate(w, tmpl, &DataContext{
		UserLoggedIn: true,
		Lang:         lang,
		Data:         data,
	})
	if err != nil {
		Errorf("Error renderitzant plantilla %s: %v", tmpl, err)
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

// injectUserIfMissing intenta inserir l'usuari obtingut del context (RequireLogin/requirePermission)
// quan el handler no l'ha passat explícitament al mapa de dades. Només actua sobre map[string]interface{}.
func injectUserIfMissing(r *http.Request, data interface{}) interface{} {
	m, ok := data.(map[string]interface{})
	if !ok {
		return data
	}
	if _, found := m["User"]; found {
		return data
	}
	if u := userFromContext(r); u != nil {
		m["User"] = u
	}
	return m
}
