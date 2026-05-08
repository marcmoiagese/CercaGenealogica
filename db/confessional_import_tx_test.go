package db

import (
	"os"
	"strings"
	"testing"
)

func TestConfessionalImportTxKeepsMotorSpecificSQL(t *testing.T) {
	txSource, err := os.ReadFile("confessional_import_tx.go")
	if err != nil {
		t.Fatalf("ReadFile confessional_import_tx.go: %v", err)
	}
	sqlCommonSource, err := os.ReadFile("sqlcommon.go")
	if err != nil {
		t.Fatalf("ReadFile sqlcommon.go: %v", err)
	}

	text := string(txSource)
	for _, want := range []string{
		"func sqliteApplyConfessionalImportPlanTx",
		"func postgresApplyConfessionalImportPlanTx",
		"func mysqlApplyConfessionalImportPlanTx",
		"BeginTx(context.Background(), nil)",
		"tx.Commit()",
		"tx.Rollback()",
		"datetime('now')",
		"NOW()",
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW())",
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))",
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("el fitxer transaccional F35-4U1 ha de contenir %q", want)
		}
	}
	if strings.Contains(text, "formatPlaceholders") {
		t.Fatalf("F35-4U1 no pot usar formatPlaceholders al flux transaccional")
	}
	if strings.Contains(string(sqlCommonSource), "ApplyConfessionalImportPlanTx") {
		t.Fatalf("F35-4U1 no ha d'afegir SQL ni flux transaccional a sqlcommon.go")
	}
}
