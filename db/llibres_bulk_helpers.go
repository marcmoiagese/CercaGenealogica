package db

import (
	"database/sql"
	"strings"
)

const (
	bulkLlibresBatchSize = 200
)

func buildBulkInsertLlibres(style, nowFun string, rows []Llibre) (string, []interface{}) {
	if len(rows) == 0 {
		return "", nil
	}
	cols := []string{
		"arquevisbat_id",
		"municipi_id",
		"nom_esglesia",
		"codi_digital",
		"codi_fisic",
		"titol",
		"tipus_llibre",
		"cronologia",
		"volum",
		"abat",
		"contingut",
		"llengua",
		"requeriments_tecnics",
		"unitat_catalogacio",
		"unitat_instalacio",
		"pagines",
		"url_base",
		"url_imatge_prefix",
		"pagina",
		"indexacio_completa",
		"created_by",
		"moderation_status",
		"moderated_by",
		"moderated_at",
		"moderation_notes",
		"created_at",
		"updated_at",
	}
	values := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*25)
	for _, l := range rows {
		values = append(values, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "+nowFun+", "+nowFun+")")
		arquebisbatArg := interface{}(l.ArquebisbatID)
		if l.ArquebisbatID == 0 {
			arquebisbatArg = nil
		}
		pagines := l.Pagines
		if !pagines.Valid {
			pagines = sql.NullInt64{}
		}
		args = append(args,
			arquebisbatArg,
			l.MunicipiID,
			l.NomEsglesia,
			l.CodiDigital,
			l.CodiFisic,
			l.Titol,
			l.TipusLlibre,
			l.Cronologia,
			l.Volum,
			l.Abat,
			l.Contingut,
			l.Llengua,
			l.Requeriments,
			l.UnitatCatalogacio,
			l.UnitatInstalacio,
			pagines,
			l.URLBase,
			l.URLImatgePrefix,
			l.Pagina,
			l.IndexacioCompleta,
			l.CreatedBy,
			l.ModeracioEstat,
			l.ModeratedBy,
			l.ModeratedAt,
			l.ModeracioMotiu,
		)
	}
	query := "INSERT INTO llibres (" + strings.Join(cols, ", ") + ") VALUES " + strings.Join(values, ", ")
	query = formatPlaceholders(style, query)
	return query, args
}

func buildBulkInsertArxiuLlibres(style string, rows []ArxiuLlibreLink) (string, []interface{}) {
	if len(rows) == 0 {
		return "", nil
	}
	cols := []string{
		"arxiu_id",
		"llibre_id",
		"signatura",
		"url_override",
	}
	values := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*4)
	for _, link := range rows {
		values = append(values, "(?, ?, ?, ?)")
		args = append(args,
			link.ArxiuID,
			link.LlibreID,
			link.Signatura,
			link.URLOverride,
		)
	}
	query := "INSERT INTO arxius_llibres (" + strings.Join(cols, ", ") + ") VALUES " + strings.Join(values, ", ")
	query = formatPlaceholders(style, query)
	return query, args
}

func buildBulkInsertLlibreURLs(style, nowFun string, rows []LlibreURL) (string, []interface{}) {
	if len(rows) == 0 {
		return "", nil
	}
	cols := []string{
		"llibre_id",
		"arxiu_id",
		"llibre_ref_id",
		"url",
		"tipus",
		"descripcio",
		"created_by",
		"created_at",
	}
	values := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*7)
	for _, link := range rows {
		values = append(values, "(?, ?, ?, ?, ?, ?, ?, "+nowFun+")")
		args = append(args,
			link.LlibreID,
			link.ArxiuID,
			link.LlibreRefID,
			link.URL,
			link.Tipus,
			link.Descripcio,
			link.CreatedBy,
		)
	}
	query := "INSERT INTO llibres_urls (" + strings.Join(cols, ", ") + ") VALUES " + strings.Join(values, ", ")
	query = formatPlaceholders(style, query)
	return query, args
}
