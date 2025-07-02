package core

import (
	"log"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
)

var blockedUserAgents = []string{
	"wget", "curl", "PostmanRuntime", "python-requests", "bot", "scrubby", "Yandex",
}

func InitWebServer() {
	log.Println("Iniciant servidor amb mesures de seguretat...")
}

// var rateLimiter = make(map[string]int)
func rateLimit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip, ok := netip.ParseAddr(r.RemoteAddr)
		if !ok {
			http.Error(w, "IP invàlida", http.StatusBadRequest)
			return
		}

		// Bloqueja IPs prohibides
		blockedIps := cnf.Config["BLOCKED_IPS"]
		for _, b := range strings.Split(blockedIps, ",") {
			blockedIP, _ := netip.ParseAddr(b)
			if blockedIP.IsValid() && ip == blockedIP {
				log.Printf("Accés denegat per IP bloquejada: %s", ip)
				http.Error(w, "Accés denegat", http.StatusForbidden)
				return
			}
		}

		// Rate Limiting
		val, loaded := rateLimiter.Load(ip.String())

		if loaded {
			last := val.(time.Time)
			if time.Since(last) < 1*time.Second {
				http.Error(w, "Massa peticions", http.StatusTooManyRequests)
				return
			}
		}

		next(w, r)
	}
}

func init() {
	cnf.LoadConfig("cnf/config.cfg")
}

func blockIPs(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip, ok := netip.ParseAddr(r.RemoteAddr)
		if !ok {
			http.Error(w, "IP invàlida", http.StatusBadRequest)
			return
		}

		blockedIps := cnf.Config["BLOCKED_IPS"]
		for _, ipStr := range strings.Split(blockedIps, ",") {
			if ip.Equal(netip.MustParseAddr(ipStr)) {
				log.Printf("Accés denegat per IP bloquejada: %s", ip)
				http.Error(w, "Accés denegat", http.StatusForbidden)
				return
			}
		}

		next(w, r)
	}
}

func rateLimit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip := r.RemoteAddr

		if lastTime, ok := rateLimiter.Load(ip); ok {
			last := lastTime.(time.Time)
			if time.Since(last) < 1*time.Second {
				http.Error(w, "Massa peticions", http.StatusTooManyRequests)
				return
			}
		}

		rateLimiter.Store(ip.String(), time.Now())
		next(w, r)
	}
}

var allowedFiles = map[string]bool{
	"css/estils.css":          true,
	"css/menu.css":            true,
	"css/perfil-dropdown.css": true,
	"js/login-modal.js":       true,
	"js/perfil-dropdown.js":   true,
	"img/logo.png":            true,
}

func ServeStatic(w http.ResponseWriter, r *http.Request) {
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

func SecureHeaders(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Content Security Policy
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self' https://cdnjs.cloudflare.com ; style-src 'self' 'unsafe-inline'; img-src 'self'; font-src 'self'; connect-src 'self';")

		// HSTS - Força HTTPS
		if os.Getenv("ENVIRONMENT") != "development" {
			w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
		}

		// Protecció XSS
		w.Header().Set("X-XSS-Protection", "1; mode=block")

		// CORS
		w.Header().Set("Access-Control-Allow-Origin", "https://genealogia.cat ")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Authorization")

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

		// Bloqueja User Agents sospitosos
		userAgent := r.UserAgent()
		for _, agent := range blockedUserAgents {
			if strings.Contains(userAgent, agent) {
				log.Printf("Scraper bloquejat: %s", userAgent)
				http.Error(w, "Accés denegat", http.StatusForbidden)
				return
			}
		}

		// Evita descàrrega directa sense referer vàlid
		referer := r.Header.Get("Referer")
		if !strings.HasPrefix(referer, "http://localhost") && !strings.HasPrefix(referer, "https://genealogia.cat ") {
			log.Printf("Accés sense referer vàlid: %s", referer)
			http.Error(w, "Accés denegat", http.StatusForbidden)
			return
		}

		// Força HTTPS en producció
		if r.URL.Scheme != "https" && os.Getenv("ENVIRONMENT") != "development" {
			log.Printf("Redirigint a HTTPS: %s", r.Host+r.URL.Path)
			http.Redirect(w, r, "https://"+r.Host+r.URL.Path, http.StatusMovedPermanently)
			return
		}

		next(w, r)
	}
}

var ipLock chan struct{} = make(chan struct{}, 1)

func init() {
	ipLock <- struct{}{}
}

func applyRateLimit(ip string) bool {
	val, loaded := rateLimiter.LoadOrStore(ip, time.Now())
	if loaded {
		last := val.(time.Time)
		if time.Since(last) < 1*time.Second {
			return false
		}
	}
	rateLimiter.Store(ip, time.Now())
	return true
}

func isBlocked(ip netip.Addr) bool {
	blockedIps := cnf.Config["BLOCKED_IPS"]
	for _, ipStr := range strings.Split(blockedIps, ",") {
		blockedIP, _ := netip.ParseAddr(ipStr)
		if ip == blockedIP {
			return true
		}
	}
	return false
}

func applyMiddleware(fn http.HandlerFunc, middlewares ...func(http.HandlerFunc) http.HandlerFunc) http.HandlerFunc {
	for _, mw := range middlewares {
		fn = mw(fn)
	}
	return fn
}
