// core/webserver.go
package core

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
)

var blockedUserAgents = []string{
	"wget", "curl", "PostmanRuntime", "python-requests", "bot", "scrubby", "Yandex",
}

var allowedFiles = map[string]bool{
	"css/estils.css":          true,
	"css/menu.css":            true,
	"css/perfil-dropdown.css": true,
	"css/login-modal.css":     true,
	"js/login-modal.js":       true,
	"js/perfil-dropdown.js":   true,
	"js/idioma.js":            true,
	"img/logo.png":            true,
}

// rateLimiter – Usarem sync.Map per compartir entre goroutines
var rateLimiter = struct {
	m  map[string]time.Time
	mu sync.Mutex
}{
	m: make(map[string]time.Time),
}

func applyMiddleware(fn http.HandlerFunc, middlewares ...func(http.HandlerFunc) http.HandlerFunc) http.HandlerFunc {
	for _, mw := range middlewares {
		fn = mw(fn)
	}
	return fn
}

// blockIPs – Bloqueja accés per IP o rang d'IPs
func BlockIPs(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ipStr := getIP(r)

		blockedIps := strings.Split(cnf.Config["BLOCKED_IPS"], ",")
		for _, b := range blockedIps {
			if ipStr == b {
				log.Printf("Accés denegat per IP bloquejada: %s", ipStr)
				http.Error(w, "Accés denegat", http.StatusForbidden)
				return
			}
		}

		next(w, r)
	}
}

// rateLimit – Permet una petició cada 200ms (5 peticions/segon com a màxim)
func RateLimit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ipStr := getIP(r)

		rateLimiter.mu.Lock()
		defer rateLimiter.mu.Unlock()
		lastTime, exists := rateLimiter.m[ipStr]
		if exists && time.Since(lastTime) < 200*time.Second {
			//rateLimiter.mu.Unlock()
			log.Printf("Massa peticions des de l'IP: %s", ipStr)
			http.Error(w, "Massa peticions", http.StatusTooManyRequests)
			return
		}
		rateLimiter.m[ipStr] = time.Now()
		//rateLimiter.mu.Unlock()

		next(w, r)
	}
}

// serveStatic – Serveix només recursos autoritzats
func ServeStatic(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/static/"):]
	realPath := filepath.Join("static", path)

	// Bloqueja Path Traversal
	if strings.Contains(path, "..") || strings.HasPrefix(path, "/") {
		log.Printf("Intent de path traversal: %s", realPath)
		http.Error(w, "Accés denegat", http.StatusForbidden)
		return
	}

	// No permet llistar carpetes
	info, err := os.Stat(realPath)
	if err == nil && info.IsDir() {
		ipStr := strings.Split(r.RemoteAddr, ":")[0]
		log.Printf("Intent de llistar carpeta: %s - IP: %s", realPath, ipStr)
		http.Error(w, "Accés denegat", http.StatusForbidden)
		return
	}

	// Si no existeix
	if _, err := os.Stat(realPath); os.IsNotExist(err) {
		http.Error(w, "Fitxer no trobat", http.StatusNotFound)
		return
	}

	// Verifica si el camí està autoritzat
	if !allowedFiles[path] {
		//ipStr := strings.Split(r.RemoteAddr, ":")[0]
		log.Printf("Fitxer no autoritzat: %s", path)
		http.Error(w, "Aquest recurs no es pot servir", http.StatusForbidden)
		return
	}

	// Obtenir IP netament
	ipStr := getIP(r)

	// Aplica Content-Type manualment
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".js":
		w.Header().Set("Content-Type", "application/javascript")
	case ".css":
		w.Header().Set("Content-Type", "text/css")
	case ".png":
		w.Header().Set("Content-Type", "image/png")
	case ".jpg", ".jpeg":
		w.Header().Set("Content-Type", "image/jpeg")
	case ".gif":
		w.Header().Set("Content-Type", "image/gif")
	default:
		// Deixa que Go dedueixi el Content-Type automàticament si no és JS/CSS/PNG...
		// O bé bloqueja extensions desconegudes
		if ext != ".html" && ext != ".ico" && ext != ".svg" {
			log.Printf("Extensió no permesa: %s", ext)
			http.Error(w, "Tipus de fitxer no suportat", http.StatusForbidden)
			return
		}
	}

	// Aplica referer check només per JS/CSS
	if strings.HasSuffix(path, ".js") || strings.HasSuffix(path, ".css") {
		referer := r.Header.Get("Referer")
		if referer != "" {
			if !strings.HasPrefix(referer, "http://localhost") && !strings.HasPrefix(referer, "https://genealogia.cat ") {
				log.Printf("Accés amb referer invàlid: %s - IP: %s", referer, ipStr)
				http.Error(w, "Accés denegat", http.StatusForbidden)
				return
			}
		}
	}

	// Serveix el fitxer
	http.ServeFile(w, r, realPath)
}

// SecureHeadersMiddleware – Aplica headers de seguretat a totes les rutes
func SecureHeaders(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Content Security Policy
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self' https://cdnjs.cloudflare.com ; style-src 'self' 'unsafe-inline'; img-src 'self'; font-src 'self'; connect-src 'self';")
		//w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")

		// HSTS - Força HTTPS
		if os.Getenv("ENVIRONMENT") != "development" {
			w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
		}

		// Protecció XSS
		w.Header().Set("X-XSS-Protection", "1; mode=block")

		// MIME Sniffing
		w.Header().Set("X-Content-Type-Options", "nosniff")

		// Evita que la web s'incrusti en altres webs
		w.Header().Set("X-Frame-Options", "DENY")

		// CORS
		w.Header().Set("Access-Control-Allow-Origin", "https://genealogia.cat ")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Authorization")

		// Restringeix el Referer per seguretat: envia l'origen (domini) en peticions cross-origin, evitant filtrar URLs completes.
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")

		// Només quan volem que els cercadors no ens indexin
		w.Header().Set("X-Robots-Tag", "noindex, nofollow, nosnippet, noarchive")

		ipStr := strings.Split(r.RemoteAddr, ":")[0]

		// Bloqueja User-Agent sospitosos
		userAgent := r.UserAgent()
		for _, agent := range blockedUserAgents {
			if strings.Contains(userAgent, agent) {
				log.Printf("Scraper bloquejat: %s - IP: %s", userAgent, ipStr)
				http.Error(w, "Accés denegat", http.StatusForbidden)
				return
			}
		}

		// Referer check només per JS/CSS
		referer := r.Header.Get("Referer")
		if referer != "" && !strings.HasPrefix(referer, "http://localhost") && !strings.HasPrefix(referer, "https://genealogia.cat ") {
			log.Printf("Accés amb referer invàlid: %s - IP: %s", referer, ipStr)
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

// InitWebServer – Carrega configuració de seguretat i inicia el servidor
func InitWebServer() {
	log.Println("Iniciant servidor amb mesures de seguretat...")
	cnf.LoadConfig("cnf/config.cfg")
}

func IsBlocked(ip string) bool {
	blockedIps := strings.Split(cnf.Config["BLOCKED_IPS"], ",")
	for _, b := range blockedIps {
		if ip == b {
			return true
		}
	}
	return false
}

func ApplyRateLimit(ip string) bool {
	rateLimiter.mu.Lock()
	defer rateLimiter.mu.Unlock()

	lastTime, exists := rateLimiter.m[ip]
	if exists && time.Since(lastTime) < 1*time.Second {
		return false // Massa peticions
	}
	rateLimiter.m[ip] = time.Now()
	return true // Es pot continuar
}

func getIP(r *http.Request) string {
	log.Printf("[getIP] RemoteAddr rebut: %v", r.RemoteAddr)
	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded != "" {
		log.Printf("[getIP] X-Forwarded-For: %v", forwarded)
		ip := strings.Split(forwarded, ",")[0]
		log.Printf("[getIP] IP parsejada de X-Forwarded-For: %v", ip)
		return ip
	}
	realIP := r.Header.Get("X-Real-IP")
	if realIP != "" {
		log.Printf("[getIP] X-Real-IP: %v", realIP)
		return realIP
	}
	ipPort := r.RemoteAddr
	ip := strings.Split(ipPort, ":")[0]
	log.Printf("[getIP] IP parsejada de RemoteAddr: %v", ip)
	return ip
}
