package unit

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
)

// TestAvailableLangsContéIdiomesBàsics comprova que els idiomes suportats
// inclouen com a mínim cat, en i oc.
func TestAvailableLangsConteIdiomesBasics(t *testing.T) {
	langs := core.AvailableLangs()

	// Si la versió de Go no té slices.Contains, es pot fer un bucle manual.
	for _, required := range []string{"cat", "en", "oc"} {
		if !slices.Contains(langs, required) {
			t.Errorf("AvailableLangs no conté %q: %v", required, langs)
		}
	}
}

// TestTCarregaTraduccionsBàsiques comprova que T retorna valors coherents
// per a claus conegudes en idiomes diferents.
func TestTCarregaTraduccionsBasiques(t *testing.T) {
	// Aquesta clau existeix a cat.json i en.json
	key := "index.title"

	catVal := core.T("cat", key)
	enVal := core.T("en", key)

	if catVal == "" || enVal == "" {
		t.Fatalf("T ha retornat cadenes buides: cat=%q en=%q", catVal, enVal)
	}
	if catVal == enVal {
		t.Errorf("Esperava traduccions diferents per %q entre cat i en, però tinc %q", key, catVal)
	}

	// Per un idioma no suportat (p.ex. 'it'), hauria de caure al default (cat)
	itVal := core.T("it", key)
	if itVal != catVal {
		t.Errorf("Per idioma no suportat, esperava fallback a cat %q, però tinc %q", catVal, itVal)
	}

	// Per una clau inexistent, ha de retornar la clau literal.
	unknownKey := "aquesta.clau.no.existeix"
	if got := core.T("cat", unknownKey); got != unknownKey {
		t.Errorf("Per clau inexistent, esperava %q però tinc %q", unknownKey, got)
	}
}

// TestResolveLangCookie comprova que la cookie `lang` té prioritat.
func TestResolveLangCookie(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	// Cookie amb valor suportat
	req.AddCookie(&http.Cookie{
		Name:  "lang",
		Value: "en",
	})

	if got := core.ResolveLang(req); got != "en" {
		t.Errorf("ResolveLang amb cookie en = %q, vull 'en'", got)
	}

	// Cookie amb variant que s'hauria de normalitzar
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.AddCookie(&http.Cookie{
		Name:  "lang",
		Value: "CA", // hauria de normalitzar a 'cat'
	})

	if got := core.ResolveLang(req2); got != "cat" {
		t.Errorf("ResolveLang amb cookie CA = %q, vull 'cat'", got)
	}
}

// TestResolveLangAcceptLanguage comprova el fallback a Accept-Language
// quan no hi ha cookie lang.
func TestResolveLangAcceptLanguage(t *testing.T) {
	// Sense cookie, Accept-Language amb en-US primer
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Language", "en-US,en;q=0.9,ca;q=0.8")

	if got := core.ResolveLang(req); got != "en" {
		t.Errorf("ResolveLang amb Accept-Language en-US... = %q, vull 'en'", got)
	}

	// Accept-Language només amb idiomes no suportats -> default 'cat'
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("Accept-Language", "it-IT,it;q=0.9")

	if got := core.ResolveLang(req2); got != "cat" {
		t.Errorf("ResolveLang amb Accept-Language it-IT... = %q, vull 'cat'", got)
	}

	// Si la request és nil, també ha de tornar el default.
	if got := core.ResolveLang(nil); got != "cat" {
		t.Errorf("ResolveLang(nil) = %q, vull 'cat'", got)
	}
}
