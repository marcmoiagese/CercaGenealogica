package core

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

var allowedFiles = map[string]bool{
	"css/estils.css":          true,
	"css/menu.css":            true,
	"css/perfil-dropdown.css": true,
	"js/login-modal.js":       true,
	"js/perfil-dropdown.js":   true,
	"img/logo.png":            true,
}

func serveStatic(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/static/"):]
	realPath := filepath.Join("static", path)

	// Bloqueja Path Traversal
	if strings.Contains(path, "..") || strings.HasPrefix(path, "/") {
		log.Printf("Path traversal detectat: %s", path)
		http.Error(w, "Accés denegat", http.StatusForbidden)
		return
	}

	// No permet llistar carpetes
	info, err := os.Stat(realPath)
	if err == nil && info.IsDir() {
		log.Printf("Intent de llistar carpeta: %s", realPath)
		http.Error(w, "Accés denegat", http.StatusForbidden)
		return
	}

	// Si no existeix, error 404
	if _, err := os.Stat(realPath); os.IsNotExist(err) {
		log.Printf("Fitxer no trobat: %s", realPath)
		http.Error(w, "Fitxer no trobat", http.StatusNotFound)
		return
	}

	// Verifica si el camí està a la llista blanca
	if !allowedFiles[path] {
		log.Printf("Fitxer no autoritzat: %s", path)
		http.Error(w, "Aquest recurs no es pot servir", http.StatusForbidden)
		return
	}

	// Serveix el fitxer
	http.ServeFile(w, r, realPath)
}

func secureHeaders(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Impedeix indexació i scraping agressiu
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-Download-Options", "noopen")
		w.Header().Set("X-Permitted-Cross-Domain-Policies", "none")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		w.Header().Set("X-Robots-Tag", "noindex, nofollow, nosnippet, noarchive")

		// Bloqueja headers sospitosos
		userAgent := r.UserAgent()
		if strings.Contains(userAgent, "wget") || strings.Contains(userAgent, "curl") {
			log.Printf("Intent de scraping bloquejat: %s", userAgent)
			http.Error(w, "Accés denegat", http.StatusForbidden)
			return
		}

		next(w, r)
	}
}
