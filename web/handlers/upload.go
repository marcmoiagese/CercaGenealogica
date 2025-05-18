package handlers

import (
	"net/http"
	"text/template"

	"github.com/marcmoiagese/CercaGenealogica/db"
	arquevisbats "github.com/marcmoiagese/CercaGenealogica/modules/Importacio/Arquevisbats"
)

func UploadPageHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tmpl, err := template.ParseFiles("web/templates/upload.html")
		if err != nil {
			http.Error(w, "No es pot llegir la plantilla", http.StatusInternalServerError)
			return
		}
		tmpl.Execute(w, nil)
	}
}

func ImportHandler(dbManager db.DBManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		arquevisbats.HandleImport(dbManager)(w, r, nil)
	}
}
