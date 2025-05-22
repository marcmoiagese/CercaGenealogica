package handlers

import (
	"database/sql"
	"net/http"
	"text/template"

	"github.com/julienschmidt/httprouter"
)

// MonPageHandler serveix la pàgina municipis.html
func MonPageHandler(db *sql.DB) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		tmpl, err := template.ParseFiles("web/templates/mon/municipis.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var municipis []string
		rows, err := db.Query("SELECT DISTINCT municipi FROM usuaris WHERE municipi IS NOT NULL AND municipi != '' ORDER BY municipi ASC")
		if err != nil {
			http.Error(w, "Error al carregar els municipis", http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var m string
			if err := rows.Scan(&m); err != nil {
				http.Error(w, "Error llegint municipi", http.StatusInternalServerError)
				return
			}
			municipis = append(municipis, m)
		}
		if err := tmpl.Execute(w, municipis); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}
