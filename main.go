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
	"strings"

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

	// Crear nova taula temporal per a possibles duplicats
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

	return db
}

type Baptisme struct {
	Cognoms         string `csv:"Cognoms"`
	PaginaReal      string `csv:"Pagina real"`
	PaginaLlibre    string `csv:"Pagina llibre"`
	Llibre          string `csv:"Llibre"`
	Any             string `csv:"Any"`
	Pare            string `csv:"Pare"`
	Mare            string `csv:"Mare"`
	AvisPaterns     string `csv:"Avis Paterns"`
	AvisMaterns     string `csv:"Avis Materns"`
	Casat           string `csv:"Casat"`
	Nascut          string `csv:"Nascut"`
	PadriBateig     string `csv:"Padri de bateig"`
	PadrinetaBateig string `csv:"Padrineta de bateig"`
	Bateig          string `csv:"Bateig"`
	Ofici           string `csv:"Ofici"`
	Defuncio        string `csv:"Defunció"`
	MatrimoniPares  string `csv:"Matrimoni Pares"`
	Historia        string `csv:"Historia"`
}

// Funció auxiliar per veure si una paraula està en un slice
func contains(slice []string, target string) bool {
	for _, s := range slice {
		if s == target {
			return true
		}
	}
	return false
}

// Funció per processar cognoms
func parseCognoms(cognomStr string) (string, string, string, string) {
	cognomStr = strings.TrimSpace(cognomStr)

	// Valors amb marques d’incomplet
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

// Funció per llegir totes les línies del CSV
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
		r.ParseMultipartForm(10 << 20) // Limit 10MB

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
		reader.FieldsPerRecord = -1 // Allow variable number of fields

		records, err := readCSVFile(reader)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error llegint CSV: %v", err), http.StatusInternalServerError)
			return
		}

		log.Printf("Nombre total de línies llegides: %d", len(records))

		municipi := r.FormValue("municipi")
		arquevisbat := r.FormValue("arquevisbat")

		stmt, err := db.Prepare("INSERT INTO usuaris(nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, any) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer stmt.Close()

		totalProcessed := 0
		totalPossibleDuplicates := 0

		for i, record := range records {
			if i == 0 {
				continue // Skip header
			}

			if len(record) < 17 {
				continue
			}

			nomCSV := record[0]
			nomCSV = strings.TrimSpace(nomCSV)

			if nomCSV == "" {
				continue
			}

			cognom1, cognom2, nom, nom_complet := parseCognoms(nomCSV)

			pagina := record[1] // Pagina real
			llibre := record[3] // Llibre
			any := record[4]    // Any

			exists := 0

			// Si tenim cognoms vàlids, comprovem duplicats
			if cognom1 != "" || cognom2 != "" {
				var err error
				if nom != "" {
					err = db.QueryRow(`
                        SELECT COUNT(*) FROM usuaris 
                        WHERE cognom1 = ? AND cognom2 = ? AND nom = ? AND pagina = ? AND llibre = ? AND any = ?
                    `, cognom1, cognom2, nom, pagina, llibre, any).Scan(&exists)
				} else {
					err = db.QueryRow(`
                        SELECT COUNT(*) FROM usuaris 
                        WHERE cognom1 = ? AND cognom2 = ? AND pagina = ? AND llibre = ? AND any = ?
                    `, cognom1, cognom2, pagina, llibre, any).Scan(&exists)
				}

				if err != nil {
					log.Println("Error comprovant duplicat:", err)
					continue
				}

				if exists > 0 {
					// Afegir a la taula de possibles duplicats
					_, err = db.Exec(`
                        INSERT INTO usuaris_possibles_duplicats(nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, any) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    `, nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, any)

					if err != nil {
						log.Printf("Error inserint a possibles duplicats: %v", err)
					}

					totalPossibleDuplicates++
					continue
				}
			}

			// Inserim registre
			_, err = stmt.Exec(nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, any)
			if err != nil {
				log.Printf("Error inserint (%s %s): %v\n", cognom1, cognom2, err)
				continue
			}

			totalProcessed++
		}

		// Mostrar resum final
		fmt.Fprintf(w, `
<!DOCTYPE html>
<html lang="ca">
<head>
    <meta charset="UTF-8">
    <title>Resultat Importació</title>
    <style>
        body { font-family: Arial; max-width: 600px; margin: auto; }
        h1 { text-align: center; }
        ul { list-style-type: none; padding: 0; }
        li { padding: 5px 0; }
    </style>
</head>
<body>
    <h1>Importació completada</h1>
    <p><strong>Registres processats:</strong> %d</p>
    <p><strong>Possibles duplicats detectats:</strong> %d</p>
    <form action="/import-seleccionats" method="POST">
        <button type="submit">Inserir possibles duplicats seleccionats</button>
    </form>
    <br>
    <h2>Possibles duplicats</h2>
    <ul>
`, totalProcessed, totalPossibleDuplicates)

		// Llegir possibles duplicats per mostrar-los
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
				fmt.Fprintf(w, "<li><input type=\"checkbox\" name=\"ids\" value=\"%d\"> %s %s | Pàgina: %s | Llibre: %s | Any: %s</li>\n", id, c1, c2, pag, lb, y)
			}
		}

		fmt.Fprintf(w, `
    </ul>
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
				placeholders := strings.Join(strings.Repeat("?", len(selectedIDs)), ",")
				query := fmt.Sprintf("SELECT nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, any FROM usuaris_possibles_duplicats WHERE id IN (%s)", placeholders)

				rows, err := db.Query(query, selectedIDs...)
				if err != nil {
					http.Error(w, "Error llegint registres seleccionats", http.StatusInternalServerError)
					return
				}
				defer rows.Close()

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
				idsStr := strings.Join(selectedIDs, ", ")
				db.Exec("DELETE FROM usuaris_possibles_duplicats WHERE id IN (" + idsStr + ")")
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

		whereClause := strings.Join(clauses, " AND ")
		sqlQuery := fmt.Sprintf("SELECT nom, cognom1, cognom2 FROM usuaris WHERE %s LIMIT 10", whereClause)

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
