package unit

import (
	"testing"

	"golang.org/x/crypto/bcrypt"
)

// Test unitari senzill per validar el comportament de bcrypt.
// No depèn del codi directament, però comprova la base criptogràfica
// sobre la qual es construeix el sistema de contrasenyes de la Fase 1.
func TestPasswordHashRoundtrip(t *testing.T) {
	raw := "ContrasenyaDeProva123!"

	hash, err := bcrypt.GenerateFromPassword([]byte(raw), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("no s'ha pogut generar hash: %v", err)
	}

	if err := bcrypt.CompareHashAndPassword(hash, []byte(raw)); err != nil {
		t.Fatalf("el hash no valida la contrasenya original: %v", err)
	}
}
