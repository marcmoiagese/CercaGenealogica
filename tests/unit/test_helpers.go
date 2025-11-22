package unit

import (
	"testing"
)

// Helper bàsic per marcar tests pendents d'implementar.
func Pending(t *testing.T, msg string) {
	t.Helper()
	t.Skipf("PENDENT: %s", msg)
}
