package db

func scanLlibreDocumentaryContexts(rows rowScanner) ([]LlibreDocumentaryContext, error) {
	res := []LlibreDocumentaryContext{}
	for rows.Next() {
		var row LlibreDocumentaryContext
		if err := rows.Scan(
			&row.ArxiuID,
			&row.ArxiuCode,
			&row.ArxiuNom,
			&row.Signatura,
			&row.URLOverride,
			&row.ReligiousEntityID,
			&row.ReligiousEntityCode,
			&row.ReligiousEntityName,
			&row.ReligionCode,
			&row.LevelCode,
			&row.RelationType,
			&row.RelationState,
			&row.RelationModerationStatus,
		); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, rows.Err()
}

type rowScanner interface {
	Next() bool
	Scan(dest ...interface{}) error
	Err() error
}

func sqliteListLlibreDocumentaryContexts(d *SQLite, llibreID int) ([]LlibreDocumentaryContext, error) {
	rows, err := d.Conn.Query(`
        SELECT al.arxiu_id,
               ax.codi,
               ax.nom,
               al.signatura,
               al.url_override,
               aer.entitat_religiosa_id,
               er.codi,
               er.nom,
               er.religio_confessio_codi,
               er.nivell_confessional_codi,
               aer.tipus_relacio,
               aer.estat,
               aer.moderation_status
        FROM arxius_llibres al
        LEFT JOIN arxius ax ON ax.id = al.arxiu_id
        LEFT JOIN arxiu_entitat_religiosa aer ON aer.arxiu_id = al.arxiu_id
        LEFT JOIN entitat_religiosa er ON er.id = aer.entitat_religiosa_id
        WHERE al.llibre_id = ?
        ORDER BY al.arxiu_id, aer.id`, llibreID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLlibreDocumentaryContexts(rows)
}

func postgresListLlibreDocumentaryContexts(d *PostgreSQL, llibreID int) ([]LlibreDocumentaryContext, error) {
	rows, err := d.Conn.Query(`
        SELECT al.arxiu_id,
               ax.codi,
               ax.nom,
               al.signatura,
               al.url_override,
               aer.entitat_religiosa_id,
               er.codi,
               er.nom,
               er.religio_confessio_codi,
               er.nivell_confessional_codi,
               aer.tipus_relacio,
               aer.estat,
               aer.moderation_status
        FROM arxius_llibres al
        LEFT JOIN arxius ax ON ax.id = al.arxiu_id
        LEFT JOIN arxiu_entitat_religiosa aer ON aer.arxiu_id = al.arxiu_id
        LEFT JOIN entitat_religiosa er ON er.id = aer.entitat_religiosa_id
        WHERE al.llibre_id = $1
        ORDER BY al.arxiu_id, aer.id`, llibreID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLlibreDocumentaryContexts(rows)
}

func mysqlListLlibreDocumentaryContexts(d *MySQL, llibreID int) ([]LlibreDocumentaryContext, error) {
	rows, err := d.Conn.Query(`
        SELECT al.arxiu_id,
               ax.codi,
               ax.nom,
               al.signatura,
               al.url_override,
               aer.entitat_religiosa_id,
               er.codi,
               er.nom,
               er.religio_confessio_codi,
               er.nivell_confessional_codi,
               aer.tipus_relacio,
               aer.estat,
               aer.moderation_status
        FROM arxius_llibres al
        LEFT JOIN arxius ax ON ax.id = al.arxiu_id
        LEFT JOIN arxiu_entitat_religiosa aer ON aer.arxiu_id = al.arxiu_id
        LEFT JOIN entitat_religiosa er ON er.id = aer.entitat_religiosa_id
        WHERE al.llibre_id = ?
        ORDER BY al.arxiu_id, aer.id`, llibreID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLlibreDocumentaryContexts(rows)
}
