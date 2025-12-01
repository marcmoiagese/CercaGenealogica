package core

import (
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"os"
	"strings"
	"time"
)

const csrfCookieName = "cg_csrf"

// ensureCSRF retorna un token CSRF vàlid i garanteix que la cookie existeix.
// Fa servir el patró double-submit: el token viatja en cookie HttpOnly i al formulari.
func ensureCSRF(w http.ResponseWriter, r *http.Request) (string, error) {
	if c, err := r.Cookie(csrfCookieName); err == nil && c.Value != "" {
		return c.Value, nil
	}

	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", err
	}
	token := base64.RawURLEncoding.EncodeToString(tokenBytes)

	env := strings.ToLower(os.Getenv("ENVIRONMENT"))
	secure := true
	sameSite := http.SameSiteStrictMode
	if env == "development" {
		secure = r.TLS != nil
		sameSite = http.SameSiteLaxMode
	}

	http.SetCookie(w, &http.Cookie{
		Name:     csrfCookieName,
		Value:    token,
		Path:     "/",
		Expires:  time.Now().Add(24 * time.Hour),
		HttpOnly: true,
		SameSite: sameSite,
		Secure:   secure,
	})

	return token, nil
}

// validateCSRF compara el token enviat al formulari amb el de la cookie.
func validateCSRF(r *http.Request, formToken string) bool {
	if formToken == "" {
		return false
	}
	c, err := r.Cookie(csrfCookieName)
	if err != nil || c == nil || c.Value == "" {
		return false
	}
	return subtleConstantTimeCompare(c.Value, formToken)
}

func subtleConstantTimeCompare(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	// constant-time compare
	var res byte
	for i := 0; i < len(a); i++ {
		res |= a[i] ^ b[i]
	}
	return res == 0
}
