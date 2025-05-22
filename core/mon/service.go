package mon

import (
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func GetCountryFields(country string) []string {
	if fields, ok := CountryStructure[country]; ok {
		return fields
	}
	return CountryStructure["Pais desconegut"]
}

// SearchRecords busca usuaris segons filtres aplicats
func SearchRecords(dbManager db.DBManager, country string, filters map[string]string) ([]map[string]interface{}, error) {
	var conditions []string
	var args []interface{}
	for field, value := range filters {
		if value != "" {
			switch field {
			case "Municipi":
				conditions = append(conditions, "municipi LIKE ?")
				args = append(args, "%"+value+"%")
			case "Provincia":
				conditions = append(conditions, "llibre LIKE ?")
				args = append(args, "%"+value+"%")
			case "Comunitat Autònoma", "Regió", "Estat", "Comtat":
				// Aquí pots afegir altres camps si s'emmagatzemen diferents
				conditions = append(conditions, "arquevisbat LIKE ?")
				args = append(args, "%"+value+"%")
			default:
				continue
			}
		}
	}

	query := "SELECT id, cognom1, cognom2, pagina, llibre, any FROM usuaris"
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	rows, err := dbManager.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var id int
		var c1, c2, pagina, llibre, any string
		err := rows.Scan(&id, &c1, &c2, &pagina, &llibre, &any)
		if err != nil {
			continue
		}
		results = append(results, map[string]interface{}{
			"id":      id,
			"cognoms": c1 + " " + c2,
			"pagina":  pagina,
			"llibre":  llibre,
			"any":     any,
		})
	}
	return results, nil
}
