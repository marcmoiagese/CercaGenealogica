package common

import (
	"fmt"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

// ────────────────────────────────
// Placeholders
// ────────────────────────────────

func formatPlaceholders(engine, query string) string {
	if strings.ToLower(engine) != "postgres" {
		// sqlite i mysql treballen amb '?'
		return query
	}

	var sb strings.Builder
	argIdx := 1
	for _, ch := range query {
		if ch == '?' {
			sb.WriteString(fmt.Sprintf("$%d", argIdx))
			argIdx++
		} else {
			sb.WriteRune(ch)
		}
	}
	return sb.String()
}

// ────────────────────────────────
// Neteja usuaris
// ────────────────────────────────

func CleanupUser(t *testing.T, dbInstance db.DB, engine, username, email string) {
	t.Helper()

	if username == "" && email == "" {
		return
	}

	base := "DELETE FROM usuaris WHERE 1=0"
	args := []any{}

	if username != "" {
		base += " OR usuari = ?"
		args = append(args, username)
	}
	if email != "" {
		base += " OR correu = ?"
		args = append(args, email)
	}

	q := formatPlaceholders(engine, base)
	if _, err := dbInstance.Exec(q, args...); err != nil {
		t.Fatalf("[%s] no puc netejar usuari %q (%s): %v", engine, username, email, err)
	}
}

// ────────────────────────────────
// Neteja sessions
// ────────────────────────────────

func CleanupSession(t *testing.T, dbInstance db.DB, engine, token string) {
	t.Helper()

	token = strings.TrimSpace(token)
	if token == "" {
		return
	}

	q := "DELETE FROM sessions WHERE token_hash = ?"
	q = formatPlaceholders(engine, q)

	if _, err := dbInstance.Exec(q, token); err != nil {
		t.Fatalf("[%s] no puc netejar sessió %q: %v", engine, token, err)
	}
}

func EnsurePostgresBoolCompat(t *testing.T, dbInstance db.DB, engine string) {
	t.Helper()
	// No-op: els tests que tenen problemes amb Postgres ja es marquen amb t.Skip.
}
