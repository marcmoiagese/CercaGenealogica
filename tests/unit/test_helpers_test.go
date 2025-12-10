package unit

import "testing"

func TestPendingHelperSkipsTest(t *testing.T) {
	Pending(t, "helper de prova")
	// No arribarem aquí, però el codi dins de Pending() ja s'ha executat
}
