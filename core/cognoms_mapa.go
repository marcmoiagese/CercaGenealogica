package core

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (a *App) CognomMapa(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	id := extractID(r.URL.Path)
	if id <= 0 {
		http.NotFound(w, r)
		return
	}
	cognom, err := a.DB.GetCognom(id)
	if err != nil || cognom == nil {
		http.NotFound(w, r)
		return
	}
	maxYear := time.Now().Year()
	y0 := parseYearParam(r.URL.Query().Get("y0"), 1500)
	y1 := parseYearParam(r.URL.Query().Get("y1"), maxYear)
	if y0 > y1 {
		y0, y1 = y1, y0
	}
	RenderPrivateTemplate(w, r, "cognom-mapa.html", map[string]interface{}{
		"Cognom":  cognom,
		"Y0":      y0,
		"Y1":      y1,
		"MaxYear": maxYear,
	})
}

func parseYearParam(raw string, fallback int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	val, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return val
}
