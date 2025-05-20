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

// parsePersona extreu nom, c1, c2 i municipi d'una cadena com "Nom Cognom1 Cognom2 (Municipi)"
func parsePersona(input string) (string, string, string, string) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", "", "", ""
	}

	var nom, c1, c2, municipi string

	// Municipi entre parèntesis
	if strings.Contains(input, "(") && strings.Contains(input, ")") {
		parts := strings.SplitN(input, "(", 2)
		namePart := strings.TrimSpace(parts[0])
		municipi = strings.TrimSuffix(strings.TrimSpace(parts[1]), ")")

		// Separa nom i cognoms
		names := strings.Fields(namePart)
		if len(names) >= 1 {
			nom = names[0]
		}
		if len(names) >= 2 {
			c1 = names[1]
		}
		if len(names) >= 3 {
			c2 = strings.Join(names[2:], " ")
		}
	} else {
		// Sense municipi
		names := strings.Fields(input)
		if len(names) >= 1 {
			nom = names[0]
		}
		if len(names) >= 2 {
			c1 = names[1]
		}
		if len(names) >= 3 {
			c2 = strings.Join(names[2:], " ")
		}
	}

	return nom, c1, c2, municipi
}

// processRelacions processa avis paterns o materns
func processRelacions(dbManager db.DBManager, usuariID int, relStr, tipus, municipiDefault string) {
	if relStr == "" {
		return
	}
	for _, part := range strings.Split(relStr, "i") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		nom, c1, c2, municipi := parsePersona(part)
		if municipi == "" {
			municipi = municipiDefault
		}
		dbManager.InsertRelacio(usuariID, tipus, nom, c1, c2, municipi, "", "")

	}
}

// processCasats processa matrimonis
func processCasats(dbManager db.DBManager, usuariID int, casatStr, municipiDefault string) {
	if casatStr == "" {
		return
	}
	matrimonis := strings.Split(casatStr, "-")
	for _, m := range matrimonis {
		m = strings.TrimSpace(m)
		if m == "" {
			continue
		}
		// Ex: "1er Nom cognom1 cognom2 dd/mm/yyyy"
		parts := strings.Fields(m)
		var data string
		var lloc string

		// Busca data i lloc
		for i, p := range parts {
			if len(p) == 10 && p[2] == '/' && p[5] == '/' {
				data = p
				if i+1 < len(parts) && !strings.Contains(parts[i+1], "/") {
					lloc = parts[i+1]
				}
				break
			}
		}

		// Nom del conjuge
		nomConjuge := strings.TrimPrefix(strings.Split(m, data)[0], "1er ")
		nomConjuge = strings.TrimPrefix(nomConjuge, "2on ")
		nomConjuge = strings.TrimSpace(nomConjuge)

		// Separa nom i cognoms
		names := strings.Fields(nomConjuge)
		var nom, c1, c2 string
		if len(names) >= 1 {
			nom = names[0]
		}
		if len(names) >= 2 {
			c1 = names[1]
		}
		if len(names) >= 3 {
			c2 = strings.Join(names[2:], " ")
		}

		dbManager.InsertRelacio(usuariID, "casat", nom, c1, c2, lloc, "", data)
	}
}

// updateUsuari actualitza camps addicionals com naixement, bateig, defuncio, ofici
func updateUsuari(dbManager db.DBManager, id int, dataNaixement, dataBateig, dataDefuncio, ofici, estatCivil string) error {
	stmt, err := dbManager.DB().Prepare(`
        UPDATE usuaris 
        SET data_naixement = ?, 
            data_bateig = ?, 
            data_defuncio = ?, 
            ofici = ?,
			estat_civil = ? 
        WHERE id = ?
    `)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(dataNaixement, dataBateig, dataDefuncio, ofici, estatCivil, id)
	return err
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

			// Nous camps
			pareStr := record[5]            // Pare
			mareStr := record[6]            // Mare
			avisPaternsStr := record[7]     // Avis Paterns
			avisMaternsStr := record[8]     // Avis Materns
			casatStr := record[9]           // Casat
			dataNaixement := record[10]     // Nascut
			padriStr := record[11]          // Padri de bateig
			padrinaStr := record[12]        // Padrineta de bateig
			dataBateig := record[13]        // Bateig
			oficiPare := record[14]         // Ofici
			dataDefuncio := record[15]      // Defunció
			matrimoniParesStr := record[16] // Matrimoni Pares

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

			// Obtenir últim ID inserit
			var lastID int64
			err = dbManager.DB().QueryRow("SELECT last_insert_rowid()").Scan(&lastID)
			if err != nil {
				log.Printf("⚠️ Error obtenint lastID: %v", err)
				continue
			}

			log.Printf("✅ Últim ID inserit: %d", lastID)

			if lastID <= 0 {
				log.Printf("⚠️ lastID invàlid: %d", lastID)
				continue
			}

			if err != nil {
				log.Printf("Error obtenint últim ID: %v", err)
			} else {
				// Validem si hem trovat algun matrimoni
				estatCivil := getEstatCivil(casatStr)

				// Actualitzar camps extra
				err = updateUsuari(dbManager, int(lastID), dataNaixement, dataBateig, dataDefuncio, oficiPare, estatCivil)
				if err != nil {
					log.Printf("Error actualitzant camps addicionals: %v", err)
				}
			}

			// Processa pare i mare
			if pareStr != "" {
				nomPare, c1Pare, c2Pare, municipiPare := parsePersona(pareStr)
				if municipiPare == "" {
					municipiPare = municipi
				}
				dbManager.InsertRelacio(int(lastID), "pare", nomPare, c1Pare, c2Pare, municipiPare, "", "")
			}

			if mareStr != "" {
				nomMare, c1Mare, c2Mare, municipiMare := parsePersona(mareStr)
				if municipiMare == "" {
					municipiMare = municipi
				}
				dbManager.InsertRelacio(int(lastID), "mare", nomMare, c1Mare, c2Mare, municipiMare, "", "")
			}

			// Processa avis
			processRelacions(dbManager, int(lastID), avisPaternsStr, "avi_patern", municipi)
			processRelacions(dbManager, int(lastID), avisMaternsStr, "avi_matern", municipi)

			// Processa padrins
			if padriStr != "" {
				nom, c1, c2, municipi := parsePersona(padriStr)
				log.Printf("Padri: %s %s %s - Municipi: %s", nom, c1, c2, municipi)
				dbManager.InsertRelacio(int(lastID), "padri", nom, c1, c2, municipi, "", "")
			}
			if padrinaStr != "" {
				nom, c1, c2, municipi := parsePersona(padrinaStr)
				log.Printf("Padrina: %s %s %s - Municipi: %s", nom, c1, c2, municipi)
				err = dbManager.InsertRelacio(int(lastID), "padrina", nom, c1, c2, municipi, "", "")
				if err != nil {
					log.Printf("❌ Error inserint padrina: %v", err)
				} else {
					log.Printf("⚠️ Padrina no processada: %q", padrinaStr)
				}
			}

			// Processa casat
			processCasats(dbManager, int(lastID), casatStr, municipi)

			// Processa matrimoni pares
			if matrimoniParesStr != "" {
				parts := strings.Fields(matrimoniParesStr)
				var data string
				var lloc string

				for i, p := range parts {
					if len(p) == 10 && p[2] == '/' && p[5] == '/' {
						data = p
						if i+1 < len(parts) && !strings.Contains(parts[i+1], "/") {
							lloc = parts[i+1]
						}
						break
					}
				}

				dbManager.InsertRelacio(int(lastID), "matrimoni_pare", "", "", "", lloc, "", data)
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

func getEstatCivil(casatStr string) string {
	casatStr = strings.ToLower(casatStr)

	if strings.Contains(casatStr, "1er") || strings.Contains(casatStr, "casat") ||
		strings.Contains(casatStr, "matrimoni") || strings.Contains(casatStr, "/") {
		return "Casat/a"
	}

	return "NoSe"
}
