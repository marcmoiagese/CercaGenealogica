// core/webserver.go
package core

import (
	"net/http"
	"os"
	"path/filepath"
	"runtime"
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
	"css/registre.css":        true,
	"css/regenerar-token.css": true,
	"js/login-modal.js":       true,
	"js/perfil-dropdown.js":   true,
	"js/idioma.js":            true,
	"img/logo.png":            true,
	"js/menu.js":              true,
}

// rateLimiter – Usarem sync.Map per compartir entre goroutines
// Implementació de Token Bucket per limitar per IP/ruta amb burst
type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	capacity   float64
	fillRate   float64 // tokens per segon
	lastRefill time.Time
}

func newTokenBucket(rate, burst float64) *tokenBucket {
	return &tokenBucket{
		tokens:     burst,
		capacity:   burst,
		fillRate:   rate,
		lastRefill: time.Now(),
	}
}

func (tb *tokenBucket) allow(n float64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens += elapsed * tb.fillRate
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
	tb.lastRefill = now

	if tb.tokens >= n {
		tb.tokens -= n
		return true
	}
	return false
}

// Gestor global de buckets per clau (ruta + IP o sessió)
var bucketRegistry = struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
}{
	buckets: make(map[string]*tokenBucket),
}

type routeLimitConfig struct {
	rate  float64 // tokens/segon
	burst float64 // capacitat
}

// Config per ruta (prefix). Default si no hi ha match.
var routeLimits = map[string]routeLimitConfig{
	"/static/":  {rate: 20, burst: 30}, // permet descarregar molts recursos en carregar pàgina
	"/login":    {rate: 5, burst: 10},  // una mica més estricte
	"/registre": {rate: 2, burst: 5},   // molt estricte per prevenir abús
}

var defaultRouteLimit = routeLimitConfig{rate: 10, burst: 20}

func getRouteLimit(path string) routeLimitConfig {
	for prefix, cfg := range routeLimits {
		if strings.HasPrefix(path, prefix) {
			return cfg
		}
	}
	return defaultRouteLimit
}

// retorna una clau de limitació basada en ruta + sessió (si disponible) o IP
func getRequesterKey(r *http.Request, route string) string {
	if sid := getSessionID(r); sid != "" {
		return route + "::SID::" + sid
	}
	return route + "::IP::" + getIP(r)
}

// intenta obtenir algun identificador de sessió/cookie existent
func getSessionID(r *http.Request) string {
	// Cerca cookies comunes; si el projecte defineix una altra, s'afegeix aquí
	candidateNames := []string{"cg_session", "session_id", "sid", "SESSION"}
	for _, name := range candidateNames {
		if c, err := r.Cookie(name); err == nil && c != nil && c.Value != "" {
			return c.Value
		}
	}
	return ""
}

/*func applyMiddleware(fn http.HandlerFunc, middlewares ...func(http.HandlerFunc) http.HandlerFunc) http.HandlerFunc {
	for _, mw := range middlewares {
		fn = mw(fn)
	}
	return fn
}*/

// blockIPs – Bloqueja accés per IP o rang d'IPs
func BlockIPs(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ipStr := getIP(r)

		blockedIps := strings.Split(cnf.Config["BLOCKED_IPS"], ",")
		for _, b := range blockedIps {
			if ipStr == b {
				Errorf("Accés denegat per IP bloquejada: %s", ipStr)
				http.Error(w, "Accés denegat", http.StatusForbidden)
				return
			}
		}

		next(w, r)
	}
}

// rateLimit – Permet una petició cada 100ms (10 peticions/segon com a màxim)
func RateLimit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		cfg := getRouteLimit(path)
		key := getRequesterKey(r, path)

		bucketRegistry.mu.Lock()
		b, ok := bucketRegistry.buckets[key]
		if !ok {
			b = newTokenBucket(cfg.rate, cfg.burst)
			bucketRegistry.buckets[key] = b
		}
		bucketRegistry.mu.Unlock()

		if !b.allow(1) {
			ipStr := getIP(r)
			Errorf("Massa peticions (path=%s, key=%s, ip=%s)", path, key, ipStr)
			http.Error(w, "Massa peticions", http.StatusTooManyRequests)
			return
		}

		next(w, r)
	}
}

// serveStatic – Serveix només recursos autoritzats
func ServeStatic(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/static/"):]
	realPath := filepath.Join("static", path)

	// Bloqueja Path Traversal
	if strings.Contains(path, "..") || strings.HasPrefix(path, "/") {
		Errorf("Intent de path traversal: %s", realPath)
		http.Error(w, "Accés denegat", http.StatusForbidden)
		return
	}

	// No permet llistar carpetes
	info, err := os.Stat(realPath)
	if err == nil && info.IsDir() {
		ipStr := strings.Split(r.RemoteAddr, ":")[0]
		Errorf("Intent de llistar carpeta: %s - IP: %s", realPath, ipStr)
		http.Error(w, "Accés denegat", http.StatusForbidden)
		return
	}

	// Si no existeix
	if _, err := os.Stat(realPath); os.IsNotExist(err) {
		// Fallback: si l'executable s'executa fora de l'arrel, prova ruta relativa al fitxer actual
		if _, file, _, ok := runtime.Caller(0); ok {
			altBase := filepath.Join(filepath.Dir(file), "..")
			altPath := filepath.Join(altBase, "static", path)
			if _, err2 := os.Stat(altPath); err2 == nil {
				realPath = altPath
			} else {
				http.Error(w, "Fitxer no trobat", http.StatusNotFound)
				return
			}
		} else {
			http.Error(w, "Fitxer no trobat", http.StatusNotFound)
			return
		}
	}

	// Verifica si el camí està autoritzat
	allowed := allowedFiles[path] ||
		strings.HasPrefix(path, "css/") ||
		strings.HasPrefix(path, "js/") ||
		strings.HasPrefix(path, "img/") ||
		strings.HasPrefix(path, "fonts/")
	if !allowed {
		Errorf("Fitxer no autoritzat: %s", path)
		http.Error(w, "Aquest recurs no es pot servir", http.StatusForbidden)
		return
	}

	// Obtenir IP netament
	ipStr := getIP(r)

	// Aplica Content-Type manualment
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".js":
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	case ".css":
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	case ".png":
		w.Header().Set("Content-Type", "image/png")
	case ".jpg", ".jpeg":
		w.Header().Set("Content-Type", "image/jpeg")
	case ".gif":
		w.Header().Set("Content-Type", "image/gif")
	case ".svg":
		w.Header().Set("Content-Type", "image/svg+xml")
	case ".ico":
		w.Header().Set("Content-Type", "image/x-icon")
	default:
		// Deixa que Go dedueixi el Content-Type automàticament per altres tipus
		if ext != ".html" {
			Errorf("Extensió no permesa: %s", ext)
			http.Error(w, "Tipus de fitxer no suportat", http.StatusForbidden)
			return
		}
	}

	// Aplica referer check només per JS/CSS
	if strings.HasSuffix(path, ".js") || strings.HasSuffix(path, ".css") {
		referer := r.Header.Get("Referer")
		if referer != "" {
			if !strings.HasPrefix(referer, "http://localhost") && !strings.HasPrefix(referer, "https://genealogia.cat") {
				Errorf("Accés amb referer invàlid: %s - IP: %s", referer, ipStr)
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

		// MIME Sniffing - Només en producció per evitar problemes en desenvolupament
		if os.Getenv("ENVIRONMENT") != "development" {
			w.Header().Set("X-Content-Type-Options", "nosniff")
		}

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
				Errorf("Scraper bloquejat: %s - IP: %s", userAgent, ipStr)
				http.Error(w, "Accés denegat", http.StatusForbidden)
				return
			}
		}

		// Referer check només per JS/CSS
		referer := r.Header.Get("Referer")
		if referer != "" && !strings.HasPrefix(referer, "http://localhost") && !strings.HasPrefix(referer, "https://genealogia.cat ") {
			Errorf("Accés amb referer invàlid: %s - IP: %s", referer, ipStr)
			http.Error(w, "Accés denegat", http.StatusForbidden)
			return
		}

		// Força HTTPS en producció
		if r.URL.Scheme != "https" && os.Getenv("ENVIRONMENT") != "development" {
			Infof("Redirigint a HTTPS: %s", r.Host+r.URL.Path)
			http.Redirect(w, r, "https://"+r.Host+r.URL.Path, http.StatusMovedPermanently)
			return
		}

		next(w, r)
	}
}

// InitWebServer – Carrega configuració de seguretat i inicia el servidor
func InitWebServer(cfg map[string]string) {
	Infof("Iniciant servidor amb mesures de seguretat...")
	if cfg != nil {
		cnf.Config = cfg
	}
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
	// Útil per a punts sense *http.Request: aplicació d'un límit genèric per IP
	cfg := defaultRouteLimit
	key := "/generic" + "::IP::" + ip

	bucketRegistry.mu.Lock()
	b, ok := bucketRegistry.buckets[key]
	if !ok {
		b = newTokenBucket(cfg.rate, cfg.burst)
		bucketRegistry.buckets[key] = b
	}
	bucketRegistry.mu.Unlock()

	return b.allow(1)
}

func getIP(r *http.Request) string {
	Debugf("[getIP] RemoteAddr rebut: %v", r.RemoteAddr)
	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded != "" {
		Debugf("[getIP] X-Forwarded-For: %v", forwarded)
		ip := strings.Split(forwarded, ",")[0]
		Debugf("[getIP] IP parsejada de X-Forwarded-For: %v", ip)
		return ip
	}
	realIP := r.Header.Get("X-Real-IP")
	if realIP != "" {
		Debugf("[getIP] X-Real-IP: %v", realIP)
		return realIP
	}

	// Manejar IPv6 correctament
	ipPort := r.RemoteAddr
	var ip string
	if strings.Contains(ipPort, "[") {
		// IPv6 format: [::1]:port
		start := strings.Index(ipPort, "[")
		end := strings.Index(ipPort, "]")
		if start != -1 && end != -1 {
			ip = ipPort[start+1 : end]
		} else {
			ip = ipPort
		}
	} else {
		// IPv4 format: 127.0.0.1:port
		ip = strings.Split(ipPort, ":")[0]
	}
	Debugf("[getIP] IP parsejada de RemoteAddr: %v", ip)
	return ip
}
