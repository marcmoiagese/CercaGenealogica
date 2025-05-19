package handlers

import (
	"log"
	"net/http"
	"strconv"
	"text/template"

	db "github.com/marcmoiagese/CercaGenealogica/db"
)

func PendentsHandler(dbManager db.DBManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dups, err := dbManager.GetPossibleDuplicates()
		if err != nil {
			log.Println("❌ Error llegint duplicats:", err)
			http.Error(w, "No es poden obtenir duplicats", http.StatusInternalServerError)
			return
		}

		tmpl, err := template.ParseFiles("web/templates/revisio_duplicats.html")
		if err != nil {
			log.Println("❌ Error llegint plantilla:", err)
			http.Error(w, "No es pot llegir la plantilla", http.StatusInternalServerError)
			return
		}
		tmpl.Execute(w, dups)
	}
}

func ImportarDuplicatsSeleccionatsHandler(dbManager db.DBManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Println("🚀 Entro a ImportarDuplicatsSeleccionatsHandler")
		if r.Method == "POST" {
			r.ParseForm()

			selectedIDs := r.Form["ids"]
			log.Printf("📥 IDs rebuts: %v", selectedIDs)

			if len(selectedIDs) == 0 {
				log.Println("❌ Cap ID seleccionat")
				http.Redirect(w, r, "/pendents", http.StatusSeeOther)
				return
			}

			var ids []int
			for _, v := range selectedIDs {
				id, err := strconv.Atoi(v)
				if err != nil {
					log.Printf("⚠️ ID invàlid: %s", v)
					continue
				}
				ids = append(ids, id)
			}

			if len(ids) == 0 {
				log.Println("❌ No s'han pogut convertir cap dels IDs")
				http.Redirect(w, r, "/pendents", http.StatusSeeOther)
				return
			}

			log.Printf("✅ Processant els següents IDs: %v", ids)

			// Movem els seleccionats a taula principal
			err := dbManager.ImportSelectedDuplicates(ids)
			if err != nil {
				log.Printf("🚫 Error al moure duplicats seleccionats: %v", err)
				http.Error(w, "Error al processar duplicats", http.StatusInternalServerError)
				return
			}

			// Eliminar després d'inserir
			err = dbManager.DeleteDuplicates(ids)
			if err != nil {
				log.Printf("🗑️ Error al eliminar duplicats seleccionats: %v", err)
			} else {
				log.Printf("✔️ S'han eliminat els duplicats seleccionats")
			}

			log.Printf("S'han processat %d duplicats seleccionats", len(ids))
		}

		http.Redirect(w, r, "/pendents", http.StatusSeeOther)
	}
}

func EliminarDuplicatsSeleccionatsHandler(dbManager db.DBManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Println("🗑️ Entro a EliminarDuplicatsSeleccionatsHandler")
		if r.Method == "POST" {
			r.ParseForm()
			selectedIDs := r.Form["ids"]

			log.Printf("📥 IDs rebuts per eliminar: %v", selectedIDs)

			if len(selectedIDs) == 0 {
				log.Println("❌ Cap ID seleccionat")
				http.Redirect(w, r, "/pendents", http.StatusSeeOther)
				return
			}

			var ids []int
			for _, v := range selectedIDs {
				id, err := strconv.Atoi(v)
				if err != nil {
					log.Printf("⚠️ ID invàlid: %s", v)
					continue
				}
				ids = append(ids, id)
			}

			if len(ids) == 0 {
				log.Println("❌ Cap ID vàlid per eliminar")
				http.Redirect(w, r, "/pendents", http.StatusSeeOther)
				return
			}

			err := dbManager.DeleteDuplicates(ids)
			if err != nil {
				log.Printf("🚫 Error eliminant duplicats: %v", err)
				http.Error(w, "Error al processar les eliminacions", http.StatusInternalServerError)
				return
			}

			log.Printf("✔️ S'han eliminat %d registres seleccionats", len(ids))
		}

		http.Redirect(w, r, "/pendents", http.StatusSeeOther)
	}
}
