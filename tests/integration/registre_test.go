package integration

import (
	"testing"

	. "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

// Test d'integració del flux de registre.
//
// IDEA:
//   - inicialitzar una BD de proves (sqlite in-memory)
//   - iniciar un http.Server de proves amb els handlers reals
//   - enviar una petició POST /registre amb dades vàlides
//   - verificar resposta (codi HTTP, redirecció, canvis a BD)
//
// De moment es marca com a pendent per no trencar la suite.
func TestFluxRegistre(t *testing.T) {
	Pending(t, "Implementar flux complet de registre (Fase 1.2)")
}
