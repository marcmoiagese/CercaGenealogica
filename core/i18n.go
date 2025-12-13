package core

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

const defaultLang = "cat"
type langOverrideKey struct{}

var (
	translations   = make(map[string]map[string]string)
	supportedLangs = map[string]struct{}{
		"cat": {},
		"en":  {},
		"oc":  {},
	}
	loadOnce sync.Once
	loadErr  error
)

func loadTranslationsOnce() {
	loadOnce.Do(func() {
		// Primer intent: directori de treball actual
		loadErr = loadTranslationsFromDir("locales")

		// Segon intent: ruta relativa al fitxer actual (per quan l'exe s'executa des d'un tmp)
		if translations[defaultLang] == nil {
			if _, file, _, ok := runtime.Caller(0); ok {
				baseDir := filepath.Dir(file)
				altLocales := filepath.Join(baseDir, "..", "locales")
				if err := loadTranslationsFromDir(altLocales); err != nil && loadErr == nil {
					loadErr = err
				}
			}
		}

		if loadErr != nil && len(translations) == 0 {
			Errorf("No s'han pogut carregar les traduccions: %v", loadErr)
		}

		if translations[defaultLang] == nil {
			Errorf("Atenció: no s'ha carregat cap traducció per a l'idioma per defecte (%s)", defaultLang)
		}
	})
}

func loadTranslationsFromDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		lang := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
		lang = strings.ToLower(lang)
		if !isSupportedLang(lang) {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		file, err := os.Open(path)
		if err != nil {
			Errorf("No s'ha pogut obrir el fitxer de traducció %s: %v", path, err)
			continue
		}

		var data map[string]string
		if err := json.NewDecoder(file).Decode(&data); err != nil {
			Errorf("No s'ha pogut parsejar %s: %v", path, err)
			_ = file.Close()
			continue
		}
		_ = file.Close()

		translations[lang] = data
		Infof("Traduccions carregades per a %s (%d claus)", lang, len(data))
	}

	return nil
}

func isSupportedLang(lang string) bool {
	_, ok := supportedLangs[lang]
	return ok
}

// normalizeLang retorna un codi d'idioma suportat, o el per defecte si no coincideix.
func normalizeLang(lang string) string {
	lang = strings.ToLower(strings.TrimSpace(lang))

	// Normalitza codis comuns o variants
	switch lang {
	case "ca":
		lang = "cat"
	case "en-us", "en-gb", "en-uk", "en-ca":
		lang = "en"
	case "oc", "oci", "oc-fr":
		lang = "oc"
	}

	if isSupportedLang(lang) {
		return lang
	}
	return defaultLang
}

// T retorna la traducció per a una clau. Si no es troba, retorna la clau literal.
func T(lang, key string) string {
	loadTranslationsOnce()

	lang = normalizeLang(lang)
	if m, ok := translations[lang]; ok {
		if val, ok := m[key]; ok && val != "" {
			return val
		}
	}

	if lang != defaultLang {
		if m, ok := translations[defaultLang]; ok {
			if val, ok := m[key]; ok && val != "" {
				return val
			}
		}
	}

	return key
}

// AvailableLangs retorna la llista d'idiomes suportats.
func AvailableLangs() []string {
	result := make([]string, 0, len(supportedLangs))
	for lang := range supportedLangs {
		result = append(result, lang)
	}
	return result
}

// ResolveLang determina l'idioma preferit a partir de la cookie o retorna el per defecte.
func ResolveLang(r *http.Request) string {
	if r == nil {
		return defaultLang
	}

	// Permet passar un valor preestablert al context (p.ex. idioma preferit de l'usuari logat)
	if v := r.Context().Value(langOverrideKey{}); v != nil {
		if lang, ok := v.(string); ok && isSupportedLang(lang) {
			return lang
		}
	}

	if c, err := r.Cookie("lang"); err == nil && c != nil {
		lang := normalizeLang(c.Value)
		if isSupportedLang(lang) {
			return lang
		}
	}

	// Fallback: Accept-Language
	if al := r.Header.Get("Accept-Language"); al != "" {
		langs := strings.Split(al, ",")
		for _, l := range langs {
			parts := strings.SplitN(l, ";", 2)
			code := strings.TrimSpace(parts[0])
			if code == "" {
				continue
			}
			norm := normalizeLang(code)
			if isSupportedLang(norm) {
				return norm
			}
		}
	}

	return defaultLang
}

// ResolveLangForUser retorna l'idioma de l'usuari si és vàlid, altrament fa servir ResolveLang.
func ResolveLangForUser(r *http.Request, pref string) string {
	if l := normalizeLang(strings.TrimSpace(pref)); l != "" && isSupportedLang(l) {
		return l
	}
	return ResolveLang(r)
}

// IsSupportedLang exposa la comprovació de llengua suportada.
func IsSupportedLang(lang string) bool {
	return isSupportedLang(lang)
}
