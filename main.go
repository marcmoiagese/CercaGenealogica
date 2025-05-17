package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Usuari struct {
	Nom     string `json:"nom"`
	Cognom1 string `json:"cognom1"`
	Cognom2 string `json:"cognom2"`
}

func initDB() *sql.DB {
	db, err := sql.Open("sqlite3", "./database.db")
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name='usuaris';")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		_, err = db.Exec(`
            CREATE TABLE usuaris (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nom TEXT NOT NULL,
                cognom1 TEXT NOT NULL,
                cognom2 TEXT NOT NULL,
                municipi TEXT,
                arquevisbat TEXT,
                nom_complet TEXT,
                pagina TEXT,
                llibre TEXT,
                any TEXT
            );
        `)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Crear taula temporal per a possibles duplicats
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS usuaris_possibles_duplicats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom TEXT NOT NULL,
            cognom1 TEXT NOT NULL,
            cognom2 TEXT NOT NULL,
            municipi TEXT,
            arquevisbat TEXT,
            nom_complet TEXT,
            pagina TEXT,
            llibre TEXT,
            any TEXT
        );
    `)
	if err != nil {
		log.Fatal(err)
	}

	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(5 * time.Minute)

	return db
}

func contains(slice []string, target string) bool {
	for _, s := range slice {
		if s == target {
			return true
		}
	}
	return false
}

func parseCognoms(cognomStr string) (string, string, string, string) {
	cognomStr = strings.TrimSpace(cognomStr)

	if strings.ContainsAny(cognomStr, "¿?") || strings.HasPrefix(cognomStr, "??") || strings.HasSuffix(cognomStr, "??") {
		return "", "", "", ""
	}

	var result []string
	words := strings.Fields(cognomStr)
	compostos := map[string]bool{
		"de": true, "del": true, "dela": true, "dels": true, "la": true, "lo": true, "los": true, "las": true,
		"san": true, "santa": true, "sant": true, "sa": true, "st": true, "ste": true,
		"stra": true, "stma": true, "i": true, "y": true,
	}

	for i := 0; i < len(words); i++ {
		word := words[i]
		if _, ok := compostos[strings.ToLower(word)]; ok && i+1 < len(words) {
			result = append(result, word+" "+words[i+1])
			i++
		} else {
			result = append(result, word)
		}
	}

	var c1, c2, nom string
	if len(result) >= 1 {
		c1 = result[0]
	}
	if len(result) >= 2 {
		c2 = result[1]
	}
	if len(result) > 2 {
		nom = strings.Join(result[2:], " ")
	}

	return c1, c2, nom, cognomStr
}

func readCSVFile(reader *csv.Reader) ([][]string, error) {
	var records [][]string
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func handleImport(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.ParseMultipartForm(10 << 20) // Límit 10MB

		file, handler, err := r.FormFile("csvFile")
		if err != nil {
			http.Error(w, "No s'ha pogut llegir el fitxer", http.StatusBadRequest)
			return
		}
		defer file.Close()

		if filepath.Ext(handler.Filename) != ".csv" {
			http.Error(w, "El fitxer ha de ser .csv", http.StatusBadRequest)
			return
		}

		reader := csv.NewReader(file)
		reader.Comma = ';'
		reader.FieldsPerRecord = -1 // Permetre nombre variable de camps

		records, err := readCSVFile(reader)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error llegint CSV: %v", err), http.StatusInternalServerError)
			return
		}

		log.Printf("Nombre total de línies llegides: %d", len(records))

		municipi := r.FormValue("municipi")
		arquevisbat := r.FormValue("arquevisbat")

		stmtInsert, err := db.Prepare("INSERT INTO usuaris(nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, any) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer stmtInsert.Close()

		stmtDuplicats, err := db.Prepare("INSERT INTO usuaris_possibles_duplicats(nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, any) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer stmtDuplicats.Close()

		totalProcessed := 0
		totalPossibleDuplicates := 0

		for i, record := range records {
			if i == 0 {
				continue // Ometre capçalera
			}

			if len(record) < 17 {
				log.Printf("Línia %d: Menys de 17 camps (%d). Saltada.", i, len(record))
				continue
			}

			nomCSV := record[0]
			nomCSV = strings.TrimSpace(nomCSV)

			if nomCSV == "" {
				log.Printf("Línia %d: Cognoms buits. Saltada.", i)
				continue
			}

			cognom1, cognom2, nom, nom_complet := parseCognoms(nomCSV)

			pagina := record[1] // Pagina real
			llibre := record[3] // Llibre
			any := record[4]    // Any

			exists := 0

			if cognom1 != "" || cognom2 != "" {
				if nom != "" {
					err := db.QueryRow(`
                        SELECT COUNT(*) FROM usuaris 
                        WHERE cognom1 = ? AND cognom2 = ? AND nom = ? AND pagina = ? AND llibre = ? AND any = ?
                    `, cognom1, cognom2, nom, pagina, llibre, any).Scan(&exists)

					if err != nil {
						log.Println("Error comprovant duplicat:", err)
						continue
					}
				} else {
					err := db.QueryRow(`
                        SELECT COUNT(*) FROM usuaris 
                        WHERE cognom1 = ? AND cognom2 = ? AND pagina = ? AND llibre = ? AND any = ?
                    `, cognom1, cognom2, pagina, llibre, any).Scan(&exists)

					if err != nil {
						log.Println("Error comprovant duplicat:", err)
						continue
					}
				}

				if exists > 0 {
					dup := fmt.Sprintf("%s %s | Pàgina: %s | Llibre: %s | Any: %s", cognom1, cognom2, pagina, llibre, any)
					log.Printf("Línia %d: Duplicat trobat: %s", i, dup)

					_, err = stmtDuplicats.Exec(nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, any)
					if err != nil {
						log.Printf("Error inserint a possibles duplicats: %v", err)
					}

					totalPossibleDuplicates++
					continue
				}
			} else {
				log.Printf("Línia %d: Cognoms buits, evitem comprovació de duplicat", i)
			}

			_, err = stmtInsert.Exec(nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, any)
			if err != nil {
				log.Printf("Error inserint línia %d: %v\n", i, err)
				continue
			}

			totalProcessed++
		}

		// Mostrar missatge final amb llista de possibles duplicats
		fmt.Fprintf(w, `
<!DOCTYPE html>
<html lang="ca">
<head>
    <meta charset="UTF-8">
    <title>Resultat Importació</title>
    <style>
        body { font-family: Arial; max-width: 600px; margin: auto }
        h1 { text-align: center }
        ul { list-style-type: none; padding: 0 }
        li { padding: 5px 0 }
    </style>
</head>
<body>
    <h1>Importació completada</h1>
    <p><strong>Registres processats:</strong> %d</p>
    <p><strong>Possibles duplicats detectats:</strong> %d</p>

    <form action="/import-seleccionats" method="POST">
        <button type="submit">Inserir possibles duplicats seleccionats</button>
        <ul>
`, totalProcessed, totalPossibleDuplicates)

		rows, err := db.Query("SELECT id, cognom1, cognom2, pagina, llibre, any FROM usuaris_possibles_duplicats")
		if err != nil {
			log.Println("Error llegint possibles duplicats:", err)
		} else {
			for rows.Next() {
				var id int
				var c1, c2, pag, lb, y string
				err := rows.Scan(&id, &c1, &c2, &pag, &lb, &y)
				if err != nil {
					log.Println("Error llegint registre:", err)
					continue
				}
				defer rows.Close()
				fmt.Fprintf(w, "<li><input type=\"checkbox\" name=\"ids\" value=\"%d\"> %s %s | Pàgina: %s | Llibre: %s | Any: %s</li>\n", id, c1, c2, pag, lb, y)
			}
		}

		fmt.Fprintf(w, `
        </ul>
    </form>
</body>
</html>
`)
	}
}

func handleSeleccionats(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			r.ParseForm()
			selectedIDs := r.Form["ids"]

			if len(selectedIDs) > 0 {
				placeholders := make([]string, len(selectedIDs))
				args := make([]interface{}, len(selectedIDs))
				for i, v := range selectedIDs {
					id, _ := strconv.Atoi(v)
					placeholders[i] = "?"
					args[i] = id
				}
				whereClause := strings.Join(placeholders, ",")

				// Obtenir registres seleccionats
				rows, err := db.Query(fmt.Sprintf("SELECT nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, any FROM usuaris_possibles_duplicats WHERE id IN (%s)", whereClause), args...)
				if err != nil {
					http.Error(w, "Error llegint registres seleccionats", http.StatusInternalServerError)
					return
				}

				// Inserció definitiva
				stmt, _ := db.Prepare("INSERT INTO usuaris(nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, any) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
				defer stmt.Close()

				for rows.Next() {
					var nom, c1, c2, muni, arq, nc, pag, lb, y string
					err := rows.Scan(&nom, &c1, &c2, &muni, &arq, &nc, &pag, &lb, &y)
					if err != nil {
						log.Println("Error llegint registre:", err)
						continue
					}

					_, err = stmt.Exec(nom, c1, c2, muni, arq, nc, pag, lb, y)
					if err != nil {
						log.Println("Error inserint registre:", err)
					}
				}

				// Eliminar després d'inserir
				db.Exec(fmt.Sprintf("DELETE FROM usuaris_possibles_duplicats WHERE id IN (%s)", whereClause), args...)
			}
		}

		http.Redirect(w, r, "/upload", http.StatusSeeOther)
	}
}

func main() {
	db := initDB()
	defer db.Close()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		tmpl, err := template.ParseFiles("templates/index.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		tmpl.Execute(w, nil)
	})

	http.HandleFunc("/cerca", func(w http.ResponseWriter, r *http.Request) {
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
	})

	http.HandleFunc("/pendents", func(w http.ResponseWriter, r *http.Request) {
		rows, err := db.Query("SELECT id, cognom1, cognom2, pagina, llibre, any FROM usuaris_possibles_duplicats")
		if err != nil {
			http.Error(w, "Error llegint possibles duplicats", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var pendentList []map[string]string
		for rows.Next() {
			var id int
			var c1, c2, pag, lb, y string
			err := rows.Scan(&id, &c1, &c2, &pag, &lb, &y)
			if err != nil {
				log.Println("Error llegint registre:", err)
				continue
			}

			pendentList = append(pendentList, map[string]string{
				"id":      strconv.Itoa(id),
				"cognoms": c1 + " " + c2,
				"pagina":  pag,
				"llibre":  lb,
				"any":     y,
			})
		}

		tmpl, err := template.ParseFiles("templates/pendents.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		tmpl.Execute(w, pendentList)
	})

	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))
	http.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		tmpl, err := template.ParseFiles("templates/upload.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		tmpl.Execute(w, nil)
	})
	http.HandleFunc("/import", handleImport(db))
	http.HandleFunc("/import-seleccionats", handleSeleccionats(db))

	fmt.Println("Servidor executant-se a http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
