package unit

import (
	"errors"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
)

// Com a mínim, assegurar-nos que SetLogLevel accepta diversos valors
// sense fer pànic i que les funcions de log es poden cridar després.
func TestSetLogLevel_AcceptsVariousValues(t *testing.T) {
	levels := []string{"debug", "info", "error", "unknown"}

	for _, lvl := range levels {
		t.Run(lvl, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("SetLogLevel(%q) ha fet pànic: %v", lvl, r)
				}
			}()

			core.SetLogLevel(lvl)

			// No comprovem el filtrat per nivell (no cal per cobertura),
			// només que les crides no petin.
			core.Debugf("debug level=%s", lvl)
			core.Infof("info level=%s", lvl)
			core.Errorf("error level=%s", lvl)
		})
	}
}

func TestLogDBOperationError_IncludesSafeContext(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("no puc crear pipe per al logger: %v", err)
	}
	defer r.Close()
	defer w.Close()

	prev := log.Writer()
	defer log.SetOutput(prev)

	core.SetLogLevel("error")
	log.SetOutput(w)
	core.LogDBOperationError(core.DBOperationLog{
		Component: "admin_municipis",
		Op:        "create_wiki_change",
		Object:    "municipi",
		ObjectID:  308,
		UserID:    1,
		Engine:    "postgres",
		Err:       errors.New("violates foreign key constraint"),
	})

	_ = w.Close()

	buf := make([]byte, 4096)
	n, err := r.Read(buf)
	if err != nil {
		t.Fatalf("error llegint de la pipe del logger: %v", err)
	}

	out := string(buf[:n])
	for _, want := range []string{
		"[ERROR] db operation failed",
		"component=admin_municipis",
		"op=create_wiki_change",
		"object=municipi",
		"object_id=308",
		"user_id=1",
		"engine=postgres",
		"violates foreign key constraint",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("log no contÃ© %q; sortida=%q", want, out)
		}
	}
}

// Verifiquem que AttachLoggerOutput realment redirigeix la sortida de log
// cap al descriptor que li passem.
func TestAttachLoggerOutput_ChangesDestination(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("no puc crear pipe per al logger: %v", err)
	}
	defer r.Close()
	defer w.Close()

	core.AttachLoggerOutput(w)

	msg := "prova de logger"
	core.Infof("MISSATGE: %s", msg)

	// Tanquem el writer per forçar que el reader pugui llegir-ho tot
	_ = w.Close()

	buf := make([]byte, 4096)
	n, err := r.Read(buf)
	if err != nil {
		t.Fatalf("error llegint de la pipe del logger: %v", err)
	}
	if n == 0 {
		t.Fatalf("esperava alguna sortida de log, però n=0")
	}
}
