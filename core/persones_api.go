package core

import (
	"net/http"
	"strings"
)

func (a *App) PersonesAPI(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/external-links") {
		a.PersonesExternalLinksAPI(w, r)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/arbre") {
		a.PersonaArbreAPI(w, r)
		return
	}
	http.NotFound(w, r)
}
