package integration

import (
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
	testcommon "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

// TestMultiDBConnectAndPing comprova que per cada motor de BD definit
// als tests (sqlite, postgres, mysql) podem:
//
//   - Crear la connexió amb db.NewDB
//   - Fer una consulta molt simple (SELECT 1) amb Query sense errors.
//
// IMPORTANT: db.DB.Query retorna []map[string]interface{}, no *sql.Rows.
func TestMultiDBConnectAndPing(t *testing.T) {
	dbConfs := testcommon.LoadTestDBConfigs(t)

	if len(dbConfs) == 0 {
		t.Fatalf("no s'ha carregat cap configuració de BD de tests (revisa tests/cnf/cnf.cfg)")
	}

	for _, dbCfg := range dbConfs {
		dbCfg := dbCfg // capture per subtest

		t.Run(dbCfg.Label, func(t *testing.T) {
			// Reutilitzem el helper com a la resta de tests d'integració
			cfg := newConfigForDB(t, dbCfg, "ping")

			d, err := db.NewDB(cfg)
			if err != nil {
				t.Fatalf("[%s] NewDB ha fallat: %v", dbCfg.Label, err)
			}
			defer d.Close()

			const q = "SELECT 1"

			rows, err := d.Query(q)
			if err != nil {
				t.Fatalf("[%s] Query(%q) ha fallat: %v", dbCfg.Label, q, err)
			}

			// Com que és []map[string]interface{}, només cal comprovar que hi hagi almenys una fila.
			if len(rows) == 0 {
				t.Fatalf("[%s] Query(%q) no ha retornat cap fila", dbCfg.Label, q)
			}

			// Opcionalment podríem validar que la primera fila té alguna columna:
			// if len(rows[0]) == 0 {
			//     t.Fatalf("[%s] Query(%q) ha retornat una fila sense columnes", dbCfg.Label, q)
			// }
		})
	}
}
