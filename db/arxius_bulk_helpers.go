package db

import "strings"

const (
	bulkArxiusBatchSize = 250
)

func buildBulkInsertArxius(style, nowFun string, rows []Arxiu) (string, []interface{}) {
	if len(rows) == 0 {
		return "", nil
	}
	cols := []string{
		"nom",
		"tipus",
		"municipi_id",
		"entitat_eclesiastica_id",
		"adreca",
		"ubicacio",
		"what3words",
		"web",
		"acces",
		"notes",
		"accepta_donacions",
		"donacions_url",
		"created_by",
		"moderation_status",
		"moderated_by",
		"moderated_at",
		"moderation_notes",
		"created_at",
		"updated_at",
	}
	values := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*17)
	for _, a := range rows {
		values = append(values, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "+nowFun+", "+nowFun+")")
		args = append(args,
			a.Nom,
			a.Tipus,
			a.MunicipiID,
			a.EntitatEclesiasticaID,
			a.Adreca,
			a.Ubicacio,
			a.What3Words,
			a.Web,
			a.Acces,
			a.Notes,
			a.AcceptaDonacions,
			a.DonacionsURL,
			a.CreatedBy,
			a.ModeracioEstat,
			a.ModeratedBy,
			a.ModeratedAt,
			a.ModeracioMotiu,
		)
	}
	query := "INSERT INTO arxius (" + strings.Join(cols, ", ") + ") VALUES " + strings.Join(values, ", ")
	query = formatPlaceholders(style, query)
	return query, args
}
