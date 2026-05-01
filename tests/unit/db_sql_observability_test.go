package unit

import (
	"bytes"
	"database/sql"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestWrapSQLError_IncludesDBContextAndLogs(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("no puc crear pipe per al logger: %v", err)
	}
	defer r.Close()
	defer w.Close()

	prev := log.Writer()
	defer log.SetOutput(prev)
	log.SetOutput(w)

	wrapped := db.WrapSQLError(db.SQLErrorContext{
		Engine:    "postgres",
		Component: "admin_municipis",
		Op:        "scan_update_municipi",
		Object:    "municipi",
		ObjectID:  308,
	}, errors.New("violates foreign key constraint"))
	if wrapped == nil {
		t.Fatalf("esperava error embolcallat")
	}
	if _, ok := wrapped.(*db.SQLOpError); !ok {
		t.Fatalf("esperava *db.SQLOpError, rebut %T", wrapped)
	}

	_ = w.Close()
	buf := make([]byte, 4096)
	n, err := r.Read(buf)
	if err != nil {
		t.Fatalf("error llegint de la pipe del logger: %v", err)
	}
	logged := string(buf[:n])

	for _, text := range []string{
		"[DB][ERROR] sql op failed",
		"engine=postgres",
		"component=admin_municipis",
		"op=scan_update_municipi",
		"object=municipi",
		"object_id=308",
		"violates foreign key constraint",
	} {
		if !strings.Contains(logged, text) {
			t.Fatalf("log no conte %q; sortida=%q", text, logged)
		}
	}

	errText := wrapped.Error()
	for _, text := range []string{
		"engine=postgres",
		"component=admin_municipis",
		"op=scan_update_municipi",
		"object=municipi",
		"object_id=308",
		"violates foreign key constraint",
	} {
		if !strings.Contains(errText, text) {
			t.Fatalf("error no conte %q; error=%q", text, errText)
		}
	}
}

func TestWrapSQLError_DoesNotDoubleWrap(t *testing.T) {
	base := errors.New("commit failed")
	first := db.WrapSQLError(db.SQLErrorContext{Engine: "sqlite", Component: "jobs", Op: "commit"}, base)
	second := db.WrapSQLError(db.SQLErrorContext{Engine: "sqlite", Component: "jobs", Op: "commit_again"}, first)
	if first != second {
		t.Fatalf("esperava no embolcallar dues vegades")
	}
}

func TestWrapSQLError_DoesNotGloballySuppressErrNoRows(t *testing.T) {
	var logged bytes.Buffer
	prev := log.Writer()
	defer log.SetOutput(prev)
	log.SetOutput(&logged)

	wrapped := db.WrapSQLError(db.SQLErrorContext{
		Engine:    "postgres",
		Component: "sql_helper",
		Op:        "unexpected_scan",
		Object:    "pg_extension",
	}, sql.ErrNoRows)
	if wrapped == nil {
		t.Fatalf("esperava que sql.ErrNoRows inesperat es continuï embolcallant")
	}
	if _, ok := wrapped.(*db.SQLOpError); !ok {
		t.Fatalf("esperava *db.SQLOpError, rebut %T", wrapped)
	}
	if !strings.Contains(logged.String(), "[DB][ERROR] sql op failed") {
		t.Fatalf("esperava log d'error SQL real; sortida=%q", logged.String())
	}
}

func TestSQLiteInitDoesNotLogRollbackWithoutActiveTransaction(t *testing.T) {
	t.Chdir(findRepoRootForF343(t))

	var logged bytes.Buffer
	prev := log.Writer()
	defer log.SetOutput(prev)
	log.SetOutput(&logged)

	database, err := db.NewDB(map[string]string{
		"DB_ENGINE": "sqlite",
		"DB_PATH":   filepath.Join(t.TempDir(), "f34_3.sqlite"),
		"RECREADB":  "true",
	})
	if err != nil {
		t.Fatalf("db.NewDB ha fallat: %v", err)
	}
	database.Close()

	out := logged.String()
	if strings.Contains(out, "[DB][ERROR]") && strings.Contains(out, "cannot rollback - no transaction is active") {
		t.Fatalf("la inicialització SQLite no hauria de loguejar rollback esperat; sortida=%q", out)
	}
}

func findRepoRootForF343(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no he trobat go.mod pujant des de %s", dir)
		}
		dir = parent
	}
}
