package db

import (
	"strings"
)

const (
	bulkTerritoriBatchSize = 250
)

func buildBulkInsertNivells(style, nowFun string, rows []NivellAdministratiu) (string, []interface{}) {
	if len(rows) == 0 {
		return "", nil
	}
	cols := []string{
		"pais_id",
		"nivel",
		"nom_nivell",
		"tipus_nivell",
		"codi_oficial",
		"altres",
		"parent_id",
		"any_inici",
		"any_fi",
		"estat",
		"created_by",
		"moderation_status",
		"moderated_by",
		"moderated_at",
		"moderation_notes",
		"created_at",
		"updated_at",
	}
	values := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*15)
	for _, n := range rows {
		values = append(values, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "+nowFun+", "+nowFun+")")
		args = append(args,
			n.PaisID,
			n.Nivel,
			n.NomNivell,
			n.TipusNivell,
			n.CodiOficial,
			n.Altres,
			n.ParentID,
			n.AnyInici,
			n.AnyFi,
			n.Estat,
			n.CreatedBy,
			n.ModeracioEstat,
			n.ModeratedBy,
			n.ModeratedAt,
			n.ModeracioMotiu,
		)
	}
	query := "INSERT INTO nivells_administratius (" + strings.Join(cols, ", ") + ") VALUES " + strings.Join(values, ", ")
	query = formatPlaceholders(style, query)
	return query, args
}

func buildBulkInsertMunicipis(style, nowFun string, rows []Municipi) (string, []interface{}) {
	if len(rows) == 0 {
		return "", nil
	}
	cols := []string{
		"nom",
		"municipi_id",
		"tipus",
		"nivell_administratiu_id_1",
		"nivell_administratiu_id_2",
		"nivell_administratiu_id_3",
		"nivell_administratiu_id_4",
		"nivell_administratiu_id_5",
		"nivell_administratiu_id_6",
		"nivell_administratiu_id_7",
		"codi_postal",
		"latitud",
		"longitud",
		"what3words",
		"web",
		"wikipedia",
		"altres",
		"estat",
		"created_by",
		"moderation_status",
		"moderated_by",
		"moderated_at",
		"moderation_notes",
		"data_creacio",
		"ultima_modificacio",
	}
	values := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*23)
	for _, m := range rows {
		values = append(values, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "+nowFun+", "+nowFun+")")
		args = append(args,
			m.Nom,
			m.MunicipiID,
			m.Tipus,
			m.NivellAdministratiuID[0],
			m.NivellAdministratiuID[1],
			m.NivellAdministratiuID[2],
			m.NivellAdministratiuID[3],
			m.NivellAdministratiuID[4],
			m.NivellAdministratiuID[5],
			m.NivellAdministratiuID[6],
			m.CodiPostal,
			m.Latitud,
			m.Longitud,
			m.What3Words,
			m.Web,
			m.Wikipedia,
			m.Altres,
			m.Estat,
			m.CreatedBy,
			m.ModeracioEstat,
			m.ModeratedBy,
			m.ModeratedAt,
			m.ModeracioMotiu,
		)
	}
	query := "INSERT INTO municipis (" + strings.Join(cols, ", ") + ") VALUES " + strings.Join(values, ", ")
	query = formatPlaceholders(style, query)
	return query, args
}

func buildBulkUpdateMunicipiParents(style string, updates []MunicipiParentUpdate) (string, []interface{}) {
	if len(updates) == 0 {
		return "", nil
	}
	var sb strings.Builder
	args := make([]interface{}, 0, len(updates)*3)
	sb.WriteString("UPDATE municipis SET municipi_id = CASE id ")
	for _, upd := range updates {
		sb.WriteString("WHEN ? THEN ? ")
		args = append(args, upd.ID, upd.ParentID)
	}
	sb.WriteString("END WHERE id IN (")
	for i, upd := range updates {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("?")
		args = append(args, upd.ID)
	}
	sb.WriteString(")")
	query := formatPlaceholders(style, sb.String())
	return query, args
}
