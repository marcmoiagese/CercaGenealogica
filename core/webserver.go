// core/webserver.go
package core

import (
	"net"
	"net/http"
	"net/url"
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

type trustedOriginsCache struct {
	mu      sync.Mutex
	raw     string
	origins map[string]struct{}
}

type trustedProxyCache struct {
	mu   sync.Mutex
	raw  string
	cidr []*net.IPNet
}

var cachedTrustedOrigins trustedOriginsCache
var cachedTrustedProxies trustedProxyCache

func environmentName() string {
	env := strings.TrimSpace(cnf.Config["ENVIRONMENT"])
	if env == "" {
		env = strings.TrimSpace(os.Getenv("ENVIRONMENT"))
	}
	if env == "" {
		env = "development"
	}
	return strings.ToLower(env)
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

type clientIPInfo struct {
	ClientIP        string
	ConnectionIP    string
	UsedForwarded   bool
	ForwardedHeader string
}

func parseRemoteAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	if strings.HasPrefix(addr, "[") {
		if host, _, err := net.SplitHostPort(addr); err == nil {
			return host
		}
		return strings.Trim(addr, "[]")
	}
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	if ip := net.ParseIP(addr); ip != nil {
		return ip.String()
	}
	return ""
}

func parseIPToken(raw string) string {
	raw = strings.TrimSpace(strings.Trim(raw, "\""))
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "[") {
		raw = strings.TrimPrefix(raw, "[")
		raw = strings.TrimSuffix(raw, "]")
	}
	if host, _, err := net.SplitHostPort(raw); err == nil {
		raw = host
	}
	if ip := net.ParseIP(raw); ip != nil {
		return ip.String()
	}
	return ""
}

func normalizeOrigin(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host)
}

func isLoopbackOrigin(norm string) bool {
	if norm == "" {
		return false
	}
	u, err := url.Parse(norm)
	if err != nil || u.Host == "" {
		return false
	}
	host := u.Host
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = strings.Trim(host, "[]")
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

func getTrustedOrigins() map[string]struct{} {
	if cnf.Config == nil {
		return map[string]struct{}{}
	}
	raw := strings.TrimSpace(cnf.Config["TRUSTED_ORIGINS"])
	publicOrigin := PublicBaseOrigin(cnf.Config)
	if publicOrigin != "" {
		if raw == "" {
			raw = publicOrigin
		} else {
			raw = raw + "," + publicOrigin
		}
	}

	cachedTrustedOrigins.mu.Lock()
	defer cachedTrustedOrigins.mu.Unlock()
	if cachedTrustedOrigins.origins != nil && cachedTrustedOrigins.raw == raw {
		return cachedTrustedOrigins.origins
	}

	origins := make(map[string]struct{})
	for _, item := range strings.Split(raw, ",") {
		if norm := normalizeOrigin(item); norm != "" {
			origins[norm] = struct{}{}
		}
	}
	cachedTrustedOrigins.raw = raw
	cachedTrustedOrigins.origins = origins
	return origins
}

func isTrustedOrigin(r *http.Request, origin string) bool {
	norm := normalizeOrigin(origin)
	if norm == "" {
		return false
	}
	origins := getTrustedOrigins()
	if len(origins) == 0 {
		fallback := normalizeOrigin(requestScheme(r) + "://" + requestHost(r))
		return fallback != "" && fallback == norm
	}
	if _, ok := origins[norm]; ok {
		return true
	}
	if environmentName() == "development" {
		fallback := normalizeOrigin(requestScheme(r) + "://" + requestHost(r))
		if isLoopbackOrigin(norm) && isLoopbackOrigin(fallback) {
			return true
		}
	}
	return false
}

func getTrustedProxyCIDRs() []*net.IPNet {
	if cnf.Config == nil {
		return nil
	}
	raw := strings.TrimSpace(cnf.Config["TRUSTED_PROXY_CIDRS"])
	cachedTrustedProxies.mu.Lock()
	defer cachedTrustedProxies.mu.Unlock()
	if cachedTrustedProxies.cidr != nil && cachedTrustedProxies.raw == raw {
		return cachedTrustedProxies.cidr
	}

	var cidrs []*net.IPNet
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if strings.Contains(item, "/") {
			if _, netw, err := net.ParseCIDR(item); err == nil && netw != nil {
				cidrs = append(cidrs, netw)
			}
			continue
		}
		if ip := net.ParseIP(item); ip != nil {
			bits := 32
			if ip.To4() == nil {
				bits = 128
			}
			netw := &net.IPNet{
				IP:   ip,
				Mask: net.CIDRMask(bits, bits),
			}
			cidrs = append(cidrs, netw)
		}
	}

	cachedTrustedProxies.raw = raw
	cachedTrustedProxies.cidr = cidrs
	return cidrs
}

func isTrustedProxy(ip string) bool {
	parsed := net.ParseIP(strings.TrimSpace(ip))
	if parsed == nil {
		return false
	}
	for _, netw := range getTrustedProxyCIDRs() {
		if netw.Contains(parsed) {
			return true
		}
	}
	return false
}

func parseForwardedFor(header string) string {
	if header == "" {
		return ""
	}
	parts := strings.Split(header, ",")
	for _, part := range parts {
		seg := strings.TrimSpace(part)
		for _, token := range strings.Split(seg, ";") {
			token = strings.TrimSpace(token)
			if strings.HasPrefix(strings.ToLower(token), "for=") {
				val := strings.TrimSpace(token[4:])
				if ip := parseIPToken(val); ip != "" {
					return ip
				}
			}
		}
	}
	return ""
}

func parseForwardedProto(header string) string {
	if header == "" {
		return ""
	}
	parts := strings.Split(header, ",")
	for _, part := range parts {
		seg := strings.TrimSpace(part)
		for _, token := range strings.Split(seg, ";") {
			token = strings.TrimSpace(token)
			if strings.HasPrefix(strings.ToLower(token), "proto=") {
				val := strings.Trim(strings.TrimSpace(token[6:]), "\"")
				if val != "" {
					return strings.ToLower(val)
				}
			}
		}
	}
	return ""
}

func parseForwardedHost(header string) string {
	if header == "" {
		return ""
	}
	parts := strings.Split(header, ",")
	for _, part := range parts {
		seg := strings.TrimSpace(part)
		for _, token := range strings.Split(seg, ";") {
			token = strings.TrimSpace(token)
			if strings.HasPrefix(strings.ToLower(token), "host=") {
				val := strings.Trim(strings.TrimSpace(token[5:]), "\"")
				if val != "" {
					return val
				}
			}
		}
	}
	return ""
}

func resolveClientIP(r *http.Request) clientIPInfo {
	connIP := parseRemoteAddr(r.RemoteAddr)
	info := clientIPInfo{
		ClientIP:     connIP,
		ConnectionIP: connIP,
	}
	if connIP == "" || !isTrustedProxy(connIP) {
		return info
	}

	if ip := parseForwardedFor(r.Header.Get("Forwarded")); ip != "" {
		info.ClientIP = ip
		info.UsedForwarded = true
		info.ForwardedHeader = "Forwarded"
		return info
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if ip := parseIPToken(strings.Split(xff, ",")[0]); ip != "" {
			info.ClientIP = ip
			info.UsedForwarded = true
			info.ForwardedHeader = "X-Forwarded-For"
			return info
		}
	}
	if xr := parseIPToken(r.Header.Get("X-Real-IP")); xr != "" {
		info.ClientIP = xr
		info.UsedForwarded = true
		info.ForwardedHeader = "X-Real-IP"
		return info
	}

	return info
}

func requestHost(r *http.Request) string {
	connIP := parseRemoteAddr(r.RemoteAddr)
	if connIP != "" && isTrustedProxy(connIP) {
		if xfHost := strings.TrimSpace(r.Header.Get("X-Forwarded-Host")); xfHost != "" {
			return strings.TrimSpace(strings.Split(xfHost, ",")[0])
		}
		if fwdHost := parseForwardedHost(r.Header.Get("Forwarded")); fwdHost != "" {
			return fwdHost
		}
	}
	return strings.TrimSpace(r.Host)
}

func requestScheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	connIP := parseRemoteAddr(r.RemoteAddr)
	if connIP != "" && isTrustedProxy(connIP) {
		if proto := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")); proto != "" {
			return strings.ToLower(strings.Split(proto, ",")[0])
		}
		if proto := parseForwardedProto(r.Header.Get("Forwarded")); proto != "" {
			return proto
		}
	}
	return "http"
}

func isRequestHTTPS(r *http.Request) bool {
	return requestScheme(r) == "https"
}

func logSecurityBlock(r *http.Request, reason string, info clientIPInfo) {
	Errorf("[security] %s method=%s path=%s host=%s origin=%s referer=%s ua=%s ip=%s conn_ip=%s forwarded=%t forwarded_header=%s",
		reason,
		r.Method,
		r.URL.Path,
		requestHost(r),
		r.Header.Get("Origin"),
		r.Header.Get("Referer"),
		r.UserAgent(),
		info.ClientIP,
		info.ConnectionIP,
		info.UsedForwarded,
		info.ForwardedHeader,
	)
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
		if cnf.Config == nil {
			cnf.Config = map[string]string{}
		}
		info := resolveClientIP(r)
		ipStr := info.ClientIP

		blockedIps := strings.Split(cnf.Config["BLOCKED_IPS"], ",")
		for _, b := range blockedIps {
			b = strings.TrimSpace(b)
			if b == "" {
				continue
			}
			if ipStr == b {
				logSecurityBlock(r, "blocked_ip", info)
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
			info := resolveClientIP(r)
			logSecurityBlock(r, "rate_limited", info)
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
		strings.HasPrefix(path, "fonts/") ||
		strings.HasPrefix(path, "vendor/")
	if !allowed {
		Errorf("Fitxer no autoritzat: %s", path)
		http.Error(w, "Aquest recurs no es pot servir", http.StatusForbidden)
		return
	}

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

	// Serveix el fitxer
	http.ServeFile(w, r, realPath)
}

// SecureHeadersMiddleware – Aplica headers de seguretat a totes les rutes
func SecureHeaders(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Content Security Policy
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self' https://cdnjs.cloudflare.com ; style-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; style-src-elem 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; img-src 'self'; font-src 'self' https://cdnjs.cloudflare.com; connect-src 'self';")
		//w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")

		// HSTS - Força HTTPS
		if environmentName() != "development" {
			w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
		}

		// Protecció XSS
		w.Header().Set("X-XSS-Protection", "1; mode=block")

		// MIME Sniffing - Només en producció per evitar problemes en desenvolupament
		if environmentName() != "development" {
			w.Header().Set("X-Content-Type-Options", "nosniff")
		}

		// Evita que la web s'incrusti en altres webs
		w.Header().Set("X-Frame-Options", "DENY")

		// CORS (només per orígens de confiança)
		if origin := r.Header.Get("Origin"); origin != "" && isTrustedOrigin(r, origin) {
			if norm := normalizeOrigin(origin); norm != "" {
				w.Header().Set("Access-Control-Allow-Origin", norm)
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Authorization, X-CSRF-Token")
				w.Header().Set("Vary", "Origin")
			}
		}

		// Restringeix el Referer per seguretat: envia l'origen (domini) en peticions cross-origin, evitant filtrar URLs completes.
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")

		// Només quan volem que els cercadors no ens indexin
		w.Header().Set("X-Robots-Tag", "noindex, nofollow, nosnippet, noarchive")

		info := resolveClientIP(r)

		// Bloqueja User-Agent sospitosos
		userAgent := r.UserAgent()
		for _, agent := range blockedUserAgents {
			if strings.Contains(userAgent, agent) {
				logSecurityBlock(r, "blocked_user_agent", info)
				http.Error(w, "Accés denegat", http.StatusForbidden)
				return
			}
		}

		// Força HTTPS en producció
		if requestScheme(r) != "https" && environmentName() != "development" {
			host := requestHost(r)
			target := r.URL.Path
			if r.URL.RawQuery != "" {
				target += "?" + r.URL.RawQuery
			}
			Infof("Redirigint a HTTPS: %s%s", host, target)
			http.Redirect(w, r, "https://"+host+target, http.StatusMovedPermanently)
			return
		}

		next(w, r)
	}
}

func isStateChangingMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func isFetchMetadataAllowed(r *http.Request) (bool, string) {
	val := strings.ToLower(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")))
	if val == "" {
		return true, ""
	}
	switch val {
	case "same-origin", "same-site", "none":
		return true, ""
	default:
		return false, val
	}
}

// OriginGuard – valida Origin/Referer per peticions que canvien estat
func OriginGuard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/static/") && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
			next(w, r)
			return
		}
		if !isStateChangingMethod(r.Method) {
			next(w, r)
			return
		}

		if ok, meta := isFetchMetadataAllowed(r); !ok {
			info := resolveClientIP(r)
			logSecurityBlock(r, "fetch_metadata_block_"+meta, info)
			http.Error(w, "Accés denegat", http.StatusForbidden)
			return
		}

		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin != "" {
			if !isTrustedOrigin(r, origin) {
				info := resolveClientIP(r)
				logSecurityBlock(r, "origin_untrusted", info)
				http.Error(w, "Accés denegat", http.StatusForbidden)
				return
			}
		} else {
			referer := strings.TrimSpace(r.Header.Get("Referer"))
			if referer == "" {
				info := resolveClientIP(r)
				logSecurityBlock(r, "origin_missing", info)
				http.Error(w, "Accés denegat", http.StatusForbidden)
				return
			}
			if !isTrustedOrigin(r, referer) {
				info := resolveClientIP(r)
				logSecurityBlock(r, "referer_untrusted", info)
				http.Error(w, "Accés denegat", http.StatusForbidden)
				return
			}
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

func allowRouteLimit(r *http.Request, route string, rate, burst float64) bool {
	if r == nil || rate <= 0 || burst <= 0 {
		return false
	}
	key := getRequesterKey(r, route)

	bucketRegistry.mu.Lock()
	b, ok := bucketRegistry.buckets[key]
	if !ok {
		b = newTokenBucket(rate, burst)
		bucketRegistry.buckets[key] = b
	}
	bucketRegistry.mu.Unlock()

	return b.allow(1)
}

func getIP(r *http.Request) string {
	info := resolveClientIP(r)
	return info.ClientIP
}
