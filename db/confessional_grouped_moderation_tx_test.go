package db

import (
	"os"
	"strings"
	"testing"
)

func TestConfessionalGroupedModerationTxKeepsMotorSpecificSQL(t *testing.T) {
	txSource, err := os.ReadFile("confessional_grouped_moderation_tx.go")
	if err != nil {
		t.Fatalf("ReadFile confessional_grouped_moderation_tx.go: %v", err)
	}
	sqlCommonSource, err := os.ReadFile("sqlcommon.go")
	if err != nil {
		t.Fatalf("ReadFile sqlcommon.go: %v", err)
	}

	text := string(txSource)
	for _, want := range []string{
		"func sqliteApproveEntitatReligiosaWithInitialParentTx",
		"func sqliteRejectEntitatReligiosaWithInitialParentTx",
		"func postgresApproveEntitatReligiosaWithInitialParentTx",
		"func postgresRejectEntitatReligiosaWithInitialParentTx",
		"func mysqlApproveEntitatReligiosaWithInitialParentTx",
		"func mysqlRejectEntitatReligiosaWithInitialParentTx",
		"moderated_at=datetime('now')",
		"moderated_at=NOW()",
		"WHERE object_type = $1 AND object_id = $2",
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), $10, $11, NOW())",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("el fitxer transaccional ha de contenir %q", want)
		}
	}
	if strings.Contains(text, "formatPlaceholders") {
		t.Fatalf("F35-3Z15 no pot usar formatPlaceholders al flux transaccional")
	}
	if strings.Contains(string(sqlCommonSource), "ApproveEntitatReligiosaWithInitialParentTx") ||
		strings.Contains(string(sqlCommonSource), "RejectEntitatReligiosaWithInitialParentTx") {
		t.Fatalf("F35-3Z15 no ha d'afegir el flux transaccional a sqlcommon.go")
	}
}
