package handlers

import (
	"database/sql"
	"html/template"
	"net/http"

	arquevisbats "github.com/marcmoiagese/CercaGenealogica/modules/Importacio/Arquevisbats"
)

func UploadPageHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tmpl, err := template.ParseFiles("web/templates/upload.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		tmpl.Execute(w, nil)
	}
}

func ImportHandler(db *sql.DB) http.HandlerFunc {
	return arquevisbats.HandleImport(db)
}
