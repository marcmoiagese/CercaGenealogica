package unit

import (
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
)

func TestParseDateEmptyString(t *testing.T) {
	d := core.ParseDate("")
	if !d.IsZero() {
		t.Fatalf("s'esperava time.Time zero per cadena buida, rebut: %v", d)
	}
}

func TestParseDateISOFormat(t *testing.T) {
	d := core.ParseDate("1980-05-04")
	if d.IsZero() {
		t.Fatalf("s'esperava time.Time no zero per data ISO, rebut: %v", d)
	}

	if d.Year() != 1980 || d.Month() != time.May || d.Day() != 4 {
		t.Errorf("data incorrecta, s'esperava 1980-05-04, rebut: %v", d)
	}
}

func TestParseDateInvalidFormats(t *testing.T) {
	cases := []string{
		"any-estrany",
		"04/05/1980",
		"04-05-1980",
		"1980/05/04",
		"31-02-2020",
	}

	for _, input := range cases {
		t.Run(input, func(t *testing.T) {
			d := core.ParseDate(input)
			if !d.IsZero() {
				t.Fatalf("per %q s'esperava time.Time zero (format invàlid segons implementació actual), rebut: %v", input, d)
			}
		})
	}
}
