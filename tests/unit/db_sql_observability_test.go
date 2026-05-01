package unit

import (
	"errors"
	"log"
	"os"
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
