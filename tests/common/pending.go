package common

import "testing"

func Pending(t *testing.T, msg string) {
	t.Helper()
	t.Skipf("PENDENT: %s", msg)
}
