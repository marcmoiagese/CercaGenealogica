package integration

import (
	"testing"
	. "tests/common"
)

// Test d'integració del canvi de contrasenya (Fase 2.1).
//
// IDEA:
//   - iniciar sessió per a un usuari de proves
//   - fer POST /perfil/password amb:
//   - contrasenya antiga incorrecta -> error
//   - contrasenya antiga correcta + nova vàlida -> èxit
//   - comprovar que només es pot iniciar sessió amb la nova contrasenya
func TestCanviContrasenya(t *testing.T) {
	Pending(t, "Implementar tests de canvi de contrasenya (Fase 2.1)")
}
