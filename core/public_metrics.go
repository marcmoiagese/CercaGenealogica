package core

import (
	"encoding/json"
	"net/http"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type publicMetrics struct {
	IndexedRecords int `json:"indexed_records"`
	Countries      int `json:"countries"`
	Regions        int `json:"regions"`
	Ecclesiastical int `json:"ecclesiastical"`
}

func (a *App) PublicMetricsAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	indexedRecords, err := a.DB.CountIndexedRegistres("publicat")
	if err != nil {
		http.Error(w, "No s'han pogut carregar les mètriques", http.StatusInternalServerError)
		return
	}
	countries, err := a.DB.CountPaisos()
	if err != nil {
		http.Error(w, "No s'han pogut carregar les mètriques", http.StatusInternalServerError)
		return
	}
	regions, err := a.DB.CountNivells(db.NivellAdminFilter{Status: "publicat"})
	if err != nil {
		http.Error(w, "No s'han pogut carregar les mètriques", http.StatusInternalServerError)
		return
	}
	ecclesiastical, err := a.DB.CountArquebisbats(db.ArquebisbatFilter{Status: "publicat"})
	if err != nil {
		http.Error(w, "No s'han pogut carregar les mètriques", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(publicMetrics{
		IndexedRecords: indexedRecords,
		Countries:      countries,
		Regions:        regions,
		Ecclesiastical: ecclesiastical,
	})
}
