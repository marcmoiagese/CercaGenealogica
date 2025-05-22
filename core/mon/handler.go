package mon

import (
	"html/template"
	"net/http"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func MonHandler(dbManager db.DBManager) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		country := r.FormValue("pais")
		if country == "" {
			http.Redirect(w, r, "/?error=no_country", http.StatusSeeOther)
			return
		}

		fields := GetCountryFields(country)

		// Recollir filtres
		filters := make(map[string]string)
		for _, f := range fields {
			filters[f] = r.FormValue(strings.ToLower(f))
		}

		// Paginació
		page := r.FormValue("page")
		if page == "" {
			page = "1"
		}
		pageNum, _ := strconv.Atoi(page)
		pageSize := 10
		offset := (pageNum - 1) * pageSize

		// Buscar resultats
		results, err := SearchRecords(dbManager, country, filters)
		if err != nil || len(results) == 0 {
			results = []map[string]interface{}{}
		}

		// Paginació
		start := offset
		end := offset + pageSize
		if start > len(results) {
			start = len(results)
		}
		if end > len(results) {
			end = len(results)
		}
		paginatedResults := results[start:end]

		// Renderitzar HTML
		tmpl, err := template.ParseFiles("web/templates/mon/index.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data := struct {
			Pais     string
			Filters  map[string]string
			Headers  []string
			Results  []map[string]interface{}
			Page     int
			PageSize int
			Total    int
			PrevPage int
			NextPage int
		}{
			Pais:     country,
			Filters:  filters,
			Headers:  append([]string{"Municipi", "Llibre", "Any"}, fields...),
			Results:  paginatedResults,
			Page:     pageNum,
			PageSize: pageSize,
			Total:    len(results),
			PrevPage: pageNum - 1,
			NextPage: pageNum + 1,
		}

		err = tmpl.Execute(w, data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}
