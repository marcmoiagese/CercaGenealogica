package unit

import (
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
	testcommon "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

// Aquest test força el camí "postgres" del format de placeholders intern
// (substitució de '?' per $1, $2, ...) fent servir ExistsUserByEmail sobre
// una DB configurada com a PostgreSQL. No ens interessa el resultat de
// la consulta, només exercitar el codi. Si no hi ha config de Postgres
// a tests/cnf/cnf.cfg, el test es marca com a Skip.
func TestFormatPlaceholders_PostgresPathIsExercised(t *testing.T) {
	configs := testcommon.LoadTestDBConfigs(t)

	var pgCfg *testcommon.TestDBConfig
	for i := range configs {
		if configs[i].Engine == "postgres" {
			pgCfg = &configs[i]
			break
		}
	}

	if pgCfg == nil {
		t.Skip("sense configuració Postgres a tests/cnf/cnf.cfg; salto test de placeholders postgres")
	}

	database, err := db.NewDB(pgCfg.Config)
	if err != nil {
		t.Fatalf("no s'ha pogut inicialitzar DB postgres de prova: %v", err)
	}
	defer database.Close()

	// Aquesta crida genera una consulta amb "?" que internament passa pel
	// format de placeholders de Postgres. No ens importa l'error retornat.
	_, _ = database.ExistsUserByEmail("dummy@example.com")
}
