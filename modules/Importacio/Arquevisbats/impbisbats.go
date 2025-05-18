package arquevisbats

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"

	"github.com/julienschmidt/httprouter"
)

// normalizeSpaces redueix múltiples espais a un sol espai
func normalizeSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// parseCognoms separa els cognoms i el nom
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

// readCSVFile llegeix totes les linies del CSV
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

// HandleImport processa l'upload del CSV
func HandleImport(dbManager db.DBManager) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		r.ParseMultipartForm(10 << 20)
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
		reader.FieldsPerRecord = -1
		records, err := readCSVFile(reader)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error llegint CSV: %v", err), http.StatusInternalServerError)
			return
		}

		log.Printf("Nombre total de línies llegides: %d", len(records))
		municipi := r.FormValue("municipi")
		arquevisbat := r.FormValue("arquevisbat")

		totalProcessed := 0
		totalDuplicates := 0
		var duplicats []string // Per mostrar al final

		log.Printf("Iniciant importació del CSV")
		log.Printf("Nombre total de línies llegides: %d", len(records))

		for i, record := range records {
			if i == 0 {
				continue // Ometre capçalera
			}
			if len(record) < 17 {
				log.Printf("Línia %d: Menys de 17 camps (%d). Saltada.", i, len(record))
				continue
			}

			nomCSV := record[0]
			nomCSV = normalizeSpaces(strings.TrimSpace(nomCSV))
			//nomCSV = strings.TrimSpace(nomCSV)
			if nomCSV == "" {
				log.Printf("Línia %d: Cognoms buits. Saltada.", i)
				continue
			}

			cognom1, cognom2, nom, nom_complet := parseCognoms(nomCSV)
			pagina := record[1]
			llibre := record[3]
			any := record[4]

			log.Printf("Processant línia %d: %s %s | Pàgina: %s | Llibre: %s | Any: %s", i, cognom1, cognom2, pagina, llibre, any)

			exists := false

			// Només comprova duplicats si hi ha cognoms vàlids
			if cognom1 != "" || cognom2 != "" {
				var isDup bool
				var err error

				// Si tenim nom, fem servir també per comprovar duplicats
				if nom != "" {
					isDup, err = dbManager.CheckDuplicate(cognom1, cognom2, nom, pagina, llibre, any)
				} else {
					isDup, err = dbManager.CheckDuplicate(cognom1, cognom2, "", pagina, llibre, any)
				}

				if err != nil {
					log.Println("Error comprovant duplicat:", err)
					continue
				}

				exists = isDup
			} else {
				log.Printf("Línia %d: Cognoms buits, evitem comprovació de duplicat", i)
			}

			if exists {
				dup := fmt.Sprintf("%s %s (%s) - Pàgina: %s, Llibre: %s, Any: %s", nom, cognom1, cognom2, pagina, llibre, any)
				duplicats = append(duplicats, dup)
				err := dbManager.InsertUsuariAPossiblesDuplicats(nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, any)
				if err != nil {
					log.Printf("Error afegint a duplicats línia %d: %v\n", i, err)
					continue
				}
				totalDuplicates++
				log.Printf("Línia %d: Duplicat trobat: %s", i, dup)
				continue
			}

			err = dbManager.InsertUsuari(nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, any)
			if err != nil {
				log.Printf("Error inserint línia %d: %v\n", i, err)
				continue
			}

			totalProcessed++
		}

		// Missatge final amb tots els duplicats
		missatgeDuplicats := "<strong>Llista de duplicats:</strong><ul>"
		for _, dup := range duplicats {
			missatgeDuplicats += "<li>" + dup + "</li>"
		}
		missatgeDuplicats += "</ul>"

		log.Printf("Registres processats: %d | Duplicats: %d", totalProcessed, totalDuplicates)

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
    <p><strong>Duplicats trobats:</strong> %d</p>
    %s
    <p><a href="/upload">Tornar</a></p>
</body>
</html>
`, totalProcessed, totalDuplicates, missatgeDuplicats)
	}
}
