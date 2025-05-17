package cerca

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
)

type Usuari struct {
	Nom     string `json:"nom"`
	Cognom1 string `json:"cognom1"`
	Cognom2 string `json:"cognom2"`
}

func CercaHandler(db *sql.DB) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		query := r.URL.Query().Get("q")
		if query == "" {
			json.NewEncoder(w).Encode([]Usuari{})
			return
		}

		words := strings.Fields(query)
		if len(words) == 0 {
			json.NewEncoder(w).Encode([]Usuari{})
			return
		}

		var clauses []string
		var params []interface{}
		for _, word := range words {
			clauses = append(clauses, "(nom LIKE ? OR cognom1 LIKE ? OR cognom2 LIKE ?)")
			params = append(params, "%"+word+"%", "%"+word+"%", "%"+word+"%")
		}

		sqlQuery := fmt.Sprintf("SELECT nom, cognom1, cognom2 FROM usuaris WHERE %s LIMIT 10", strings.Join(clauses, " AND "))
		rows, err := db.Query(sqlQuery, params...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var resultats []Usuari
		for rows.Next() {
			var u Usuari
			err := rows.Scan(&u.Nom, &u.Cognom1, &u.Cognom2)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			resultats = append(resultats, u)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resultats)
	}
}
