package db

import "strings"

const (
	bulkActivityBatchSize = 250
)

func buildBulkInsertUserActivities(style, nowFun string, rows []UserActivity) (string, []interface{}) {
	if len(rows) == 0 {
		return "", nil
	}
	cols := []string{
		"usuari_id",
		"regla_id",
		"accio",
		"objecte_tipus",
		"objecte_id",
		"punts",
		"estat",
		"moderat_per",
		"detalls",
		"data_creacio",
	}
	values := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*9)
	for _, a := range rows {
		values = append(values, "(?, ?, ?, ?, ?, ?, ?, ?, ?, "+nowFun+")")
		var ruleID interface{}
		if a.RuleID.Valid {
			ruleID = a.RuleID.Int64
		}
		var objID interface{}
		if a.ObjectID.Valid {
			objID = a.ObjectID.Int64
		}
		var modID interface{}
		if a.ModeratedBy.Valid {
			modID = a.ModeratedBy.Int64
		}
		args = append(args,
			a.UserID,
			ruleID,
			a.Action,
			a.ObjectType,
			objID,
			a.Points,
			a.Status,
			modID,
			a.Details,
		)
	}
	query := "INSERT INTO usuaris_activitat (" + strings.Join(cols, ", ") + ") VALUES " + strings.Join(values, ", ")
	query = formatPlaceholders(style, query)
	return query, args
}
