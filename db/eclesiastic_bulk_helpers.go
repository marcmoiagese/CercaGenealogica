package db

import "strings"

const (
	bulkEclesiasticBatchSize = 250
)

func buildBulkInsertArquebisbats(style, nowFun string, rows []Arquebisbat) (string, []interface{}) {
	if len(rows) == 0 {
		return "", nil
	}
	cols := []string{
		"nom",
		"tipus_entitat",
		"pais_id",
		"nivell",
		"parent_id",
		"any_inici",
		"any_fi",
		"web",
		"web_arxiu",
		"web_wikipedia",
		"territori",
		"observacions",
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
			a.TipusEntitat,
			a.PaisID,
			a.Nivell,
			a.ParentID,
			a.AnyInici,
			a.AnyFi,
			a.Web,
			a.WebArxiu,
			a.WebWikipedia,
			a.Territori,
			a.Observacions,
			a.CreatedBy,
			a.ModeracioEstat,
			a.ModeratedBy,
			a.ModeratedAt,
			a.ModeracioMotiu,
		)
	}
	query := "INSERT INTO arquebisbats (" + strings.Join(cols, ", ") + ") VALUES " + strings.Join(values, ", ")
	query = formatPlaceholders(style, query)
	return query, args
}

func buildBulkInsertArquebisbatMunicipis(style, nowFun string, rows []ArquebisbatMunicipi) (string, []interface{}) {
	if len(rows) == 0 {
		return "", nil
	}
	cols := []string{
		"id_municipi",
		"id_arquevisbat",
		"any_inici",
		"any_fi",
		"motiu",
		"font",
		"created_at",
	}
	values := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*6)
	for _, r := range rows {
		values = append(values, "(?, ?, ?, ?, ?, ?, "+nowFun+")")
		args = append(args,
			r.MunicipiID,
			r.ArquebisbatID,
			r.AnyInici,
			r.AnyFi,
			r.Motiu,
			r.Font,
		)
	}
	query := "INSERT INTO arquebisbats_municipi (" + strings.Join(cols, ", ") + ") VALUES " + strings.Join(values, ", ")
	query = formatPlaceholders(style, query)
	return query, args
}
