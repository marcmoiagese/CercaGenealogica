package db

import (
	"database/sql"
	"fmt"
)

type confessionalQueries struct {
	engine string

	listReligions  string
	getReligion    string
	insertReligion string
	updateReligion string
	deleteReligion string

	listModels  string
	getModel    string
	insertModel string
	updateModel string
	deleteModel string

	listNivells  string
	getNivell    string
	insertNivell string
	updateNivell string
	deleteNivell string

	listEntitats  string
	getEntitat    string
	insertEntitat string
	updateEntitat string
	deleteEntitat string

	listRelacionsAll        string
	listRelacionsByMunicipi string
	getRelacio              string
	insertRelacio           string
	updateRelacio           string
	deleteRelacio           string
	returningID             bool
}

func sqliteConfessionalQueries() confessionalQueries {
	return confessionalQueries{
		engine:                  "sqlite",
		listReligions:           `SELECT id, nom, pare_id, COALESCE(descripcio, ''), estat, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM religio_confessio ORDER BY nom`,
		getReligion:             `SELECT id, nom, pare_id, COALESCE(descripcio, ''), estat, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM religio_confessio WHERE id = ?`,
		insertReligion:          `INSERT INTO religio_confessio (nom, pare_id, descripcio, estat, observacions, moderation_status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`,
		updateReligion:          `UPDATE religio_confessio SET nom=?, pare_id=?, descripcio=?, estat=?, observacions=?, moderation_status=?, updated_at=datetime('now') WHERE id=?`,
		deleteReligion:          `DELETE FROM religio_confessio WHERE id = ?`,
		listModels:              `SELECT id, nom, religio_confessio_id, pais_id, COALESCE(descripcio, ''), any_inici, any_fi, estat, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM model_confessional ORDER BY nom`,
		getModel:                `SELECT id, nom, religio_confessio_id, pais_id, COALESCE(descripcio, ''), any_inici, any_fi, estat, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM model_confessional WHERE id = ?`,
		insertModel:             `INSERT INTO model_confessional (nom, religio_confessio_id, pais_id, descripcio, any_inici, any_fi, estat, observacions, moderation_status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`,
		updateModel:             `UPDATE model_confessional SET nom=?, religio_confessio_id=?, pais_id=?, descripcio=?, any_inici=?, any_fi=?, estat=?, observacions=?, moderation_status=?, updated_at=datetime('now') WHERE id=?`,
		deleteModel:             `DELETE FROM model_confessional WHERE id = ?`,
		listNivells:             `SELECT id, model_confessional_id, ordre, nom_nivell, COALESCE(nom_plural, ''), COALESCE(tipus_nivell, ''), COALESCE(codi_oficial, ''), parent_id, any_inici, any_fi, estat, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM nivell_confessional ORDER BY model_confessional_id, ordre, nom_nivell`,
		getNivell:               `SELECT id, model_confessional_id, ordre, nom_nivell, COALESCE(nom_plural, ''), COALESCE(tipus_nivell, ''), COALESCE(codi_oficial, ''), parent_id, any_inici, any_fi, estat, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM nivell_confessional WHERE id = ?`,
		insertNivell:            `INSERT INTO nivell_confessional (model_confessional_id, ordre, nom_nivell, nom_plural, tipus_nivell, codi_oficial, parent_id, any_inici, any_fi, estat, observacions, moderation_status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`,
		updateNivell:            `UPDATE nivell_confessional SET model_confessional_id=?, ordre=?, nom_nivell=?, nom_plural=?, tipus_nivell=?, codi_oficial=?, parent_id=?, any_inici=?, any_fi=?, estat=?, observacions=?, moderation_status=?, updated_at=datetime('now') WHERE id=?`,
		deleteNivell:            `DELETE FROM nivell_confessional WHERE id = ?`,
		listEntitats:            `SELECT id, nom, religio_confessio_id, model_confessional_id, nivell_confessional_id, pais_id, parent_id, COALESCE(tipus_entitat, ''), COALESCE(tipus_especific, ''), any_inici, any_fi, estat, COALESCE(web, ''), COALESCE(web_wikipedia, ''), COALESCE(territori, ''), COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM entitat_religiosa ORDER BY nom`,
		getEntitat:              `SELECT id, nom, religio_confessio_id, model_confessional_id, nivell_confessional_id, pais_id, parent_id, COALESCE(tipus_entitat, ''), COALESCE(tipus_especific, ''), any_inici, any_fi, estat, COALESCE(web, ''), COALESCE(web_wikipedia, ''), COALESCE(territori, ''), COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM entitat_religiosa WHERE id = ?`,
		insertEntitat:           `INSERT INTO entitat_religiosa (nom, religio_confessio_id, model_confessional_id, nivell_confessional_id, pais_id, parent_id, tipus_entitat, tipus_especific, any_inici, any_fi, estat, web, web_wikipedia, territori, observacions, moderation_status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`,
		updateEntitat:           `UPDATE entitat_religiosa SET nom=?, religio_confessio_id=?, model_confessional_id=?, nivell_confessional_id=?, pais_id=?, parent_id=?, tipus_entitat=?, tipus_especific=?, any_inici=?, any_fi=?, estat=?, web=?, web_wikipedia=?, territori=?, observacions=?, moderation_status=?, updated_at=datetime('now') WHERE id=?`,
		deleteEntitat:           `DELETE FROM entitat_religiosa WHERE id = ?`,
		listRelacionsAll:        `SELECT id, municipi_id, nucli_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM municipi_entitat_religiosa ORDER BY municipi_id, nucli_id, any_inici, id`,
		listRelacionsByMunicipi: `SELECT id, municipi_id, nucli_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM municipi_entitat_religiosa WHERE municipi_id = ? ORDER BY municipi_id, nucli_id, any_inici, id`,
		getRelacio:              `SELECT id, municipi_id, nucli_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM municipi_entitat_religiosa WHERE id = ?`,
		insertRelacio:           `INSERT INTO municipi_entitat_religiosa (municipi_id, nucli_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, observacions, moderation_status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`,
		updateRelacio:           `UPDATE municipi_entitat_religiosa SET municipi_id=?, nucli_id=?, entitat_religiosa_id=?, tipus_relacio=?, any_inici=?, any_fi=?, observacions=?, moderation_status=?, updated_at=datetime('now') WHERE id=?`,
		deleteRelacio:           `DELETE FROM municipi_entitat_religiosa WHERE id = ?`,
	}
}

func mysqlConfessionalQueries() confessionalQueries {
	q := sqliteConfessionalQueries()
	q.engine = "mysql"
	q.insertReligion = `INSERT INTO religio_confessio (nom, pare_id, descripcio, estat, observacions, moderation_status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, NOW(), NOW())`
	q.updateReligion = `UPDATE religio_confessio SET nom=?, pare_id=?, descripcio=?, estat=?, observacions=?, moderation_status=?, updated_at=NOW() WHERE id=?`
	q.insertModel = `INSERT INTO model_confessional (nom, religio_confessio_id, pais_id, descripcio, any_inici, any_fi, estat, observacions, moderation_status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`
	q.updateModel = `UPDATE model_confessional SET nom=?, religio_confessio_id=?, pais_id=?, descripcio=?, any_inici=?, any_fi=?, estat=?, observacions=?, moderation_status=?, updated_at=NOW() WHERE id=?`
	q.insertNivell = `INSERT INTO nivell_confessional (model_confessional_id, ordre, nom_nivell, nom_plural, tipus_nivell, codi_oficial, parent_id, any_inici, any_fi, estat, observacions, moderation_status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`
	q.updateNivell = `UPDATE nivell_confessional SET model_confessional_id=?, ordre=?, nom_nivell=?, nom_plural=?, tipus_nivell=?, codi_oficial=?, parent_id=?, any_inici=?, any_fi=?, estat=?, observacions=?, moderation_status=?, updated_at=NOW() WHERE id=?`
	q.insertEntitat = `INSERT INTO entitat_religiosa (nom, religio_confessio_id, model_confessional_id, nivell_confessional_id, pais_id, parent_id, tipus_entitat, tipus_especific, any_inici, any_fi, estat, web, web_wikipedia, territori, observacions, moderation_status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`
	q.updateEntitat = `UPDATE entitat_religiosa SET nom=?, religio_confessio_id=?, model_confessional_id=?, nivell_confessional_id=?, pais_id=?, parent_id=?, tipus_entitat=?, tipus_especific=?, any_inici=?, any_fi=?, estat=?, web=?, web_wikipedia=?, territori=?, observacions=?, moderation_status=?, updated_at=NOW() WHERE id=?`
	q.insertRelacio = `INSERT INTO municipi_entitat_religiosa (municipi_id, nucli_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, observacions, moderation_status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`
	q.updateRelacio = `UPDATE municipi_entitat_religiosa SET municipi_id=?, nucli_id=?, entitat_religiosa_id=?, tipus_relacio=?, any_inici=?, any_fi=?, observacions=?, moderation_status=?, updated_at=NOW() WHERE id=?`
	return q
}

func postgresConfessionalQueries() confessionalQueries {
	q := confessionalQueries{
		engine: "postgres", returningID: true,
		listReligions:           `SELECT id, nom, pare_id, COALESCE(descripcio, ''), estat, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM religio_confessio ORDER BY nom`,
		getReligion:             `SELECT id, nom, pare_id, COALESCE(descripcio, ''), estat, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM religio_confessio WHERE id = $1`,
		insertReligion:          `INSERT INTO religio_confessio (nom, pare_id, descripcio, estat, observacions, moderation_status, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW()) RETURNING id`,
		updateReligion:          `UPDATE religio_confessio SET nom=$1, pare_id=$2, descripcio=$3, estat=$4, observacions=$5, moderation_status=$6, updated_at=NOW() WHERE id=$7`,
		deleteReligion:          `DELETE FROM religio_confessio WHERE id = $1`,
		listModels:              `SELECT id, nom, religio_confessio_id, pais_id, COALESCE(descripcio, ''), any_inici, any_fi, estat, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM model_confessional ORDER BY nom`,
		getModel:                `SELECT id, nom, religio_confessio_id, pais_id, COALESCE(descripcio, ''), any_inici, any_fi, estat, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM model_confessional WHERE id = $1`,
		insertModel:             `INSERT INTO model_confessional (nom, religio_confessio_id, pais_id, descripcio, any_inici, any_fi, estat, observacions, moderation_status, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW()) RETURNING id`,
		updateModel:             `UPDATE model_confessional SET nom=$1, religio_confessio_id=$2, pais_id=$3, descripcio=$4, any_inici=$5, any_fi=$6, estat=$7, observacions=$8, moderation_status=$9, updated_at=NOW() WHERE id=$10`,
		deleteModel:             `DELETE FROM model_confessional WHERE id = $1`,
		listNivells:             `SELECT id, model_confessional_id, ordre, nom_nivell, COALESCE(nom_plural, ''), COALESCE(tipus_nivell, ''), COALESCE(codi_oficial, ''), parent_id, any_inici, any_fi, estat, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM nivell_confessional ORDER BY model_confessional_id, ordre, nom_nivell`,
		getNivell:               `SELECT id, model_confessional_id, ordre, nom_nivell, COALESCE(nom_plural, ''), COALESCE(tipus_nivell, ''), COALESCE(codi_oficial, ''), parent_id, any_inici, any_fi, estat, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM nivell_confessional WHERE id = $1`,
		insertNivell:            `INSERT INTO nivell_confessional (model_confessional_id, ordre, nom_nivell, nom_plural, tipus_nivell, codi_oficial, parent_id, any_inici, any_fi, estat, observacions, moderation_status, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW(), NOW()) RETURNING id`,
		updateNivell:            `UPDATE nivell_confessional SET model_confessional_id=$1, ordre=$2, nom_nivell=$3, nom_plural=$4, tipus_nivell=$5, codi_oficial=$6, parent_id=$7, any_inici=$8, any_fi=$9, estat=$10, observacions=$11, moderation_status=$12, updated_at=NOW() WHERE id=$13`,
		deleteNivell:            `DELETE FROM nivell_confessional WHERE id = $1`,
		listEntitats:            `SELECT id, nom, religio_confessio_id, model_confessional_id, nivell_confessional_id, pais_id, parent_id, COALESCE(tipus_entitat, ''), COALESCE(tipus_especific, ''), any_inici, any_fi, estat, COALESCE(web, ''), COALESCE(web_wikipedia, ''), COALESCE(territori, ''), COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM entitat_religiosa ORDER BY nom`,
		getEntitat:              `SELECT id, nom, religio_confessio_id, model_confessional_id, nivell_confessional_id, pais_id, parent_id, COALESCE(tipus_entitat, ''), COALESCE(tipus_especific, ''), any_inici, any_fi, estat, COALESCE(web, ''), COALESCE(web_wikipedia, ''), COALESCE(territori, ''), COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM entitat_religiosa WHERE id = $1`,
		insertEntitat:           `INSERT INTO entitat_religiosa (nom, religio_confessio_id, model_confessional_id, nivell_confessional_id, pais_id, parent_id, tipus_entitat, tipus_especific, any_inici, any_fi, estat, web, web_wikipedia, territori, observacions, moderation_status, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, NOW(), NOW()) RETURNING id`,
		updateEntitat:           `UPDATE entitat_religiosa SET nom=$1, religio_confessio_id=$2, model_confessional_id=$3, nivell_confessional_id=$4, pais_id=$5, parent_id=$6, tipus_entitat=$7, tipus_especific=$8, any_inici=$9, any_fi=$10, estat=$11, web=$12, web_wikipedia=$13, territori=$14, observacions=$15, moderation_status=$16, updated_at=NOW() WHERE id=$17`,
		deleteEntitat:           `DELETE FROM entitat_religiosa WHERE id = $1`,
		listRelacionsAll:        `SELECT id, municipi_id, nucli_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM municipi_entitat_religiosa ORDER BY municipi_id, nucli_id, any_inici, id`,
		listRelacionsByMunicipi: `SELECT id, municipi_id, nucli_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM municipi_entitat_religiosa WHERE municipi_id = $1 ORDER BY municipi_id, nucli_id, any_inici, id`,
		getRelacio:              `SELECT id, municipi_id, nucli_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), moderation_status, created_at, updated_at FROM municipi_entitat_religiosa WHERE id = $1`,
		insertRelacio:           `INSERT INTO municipi_entitat_religiosa (municipi_id, nucli_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, observacions, moderation_status, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW()) RETURNING id`,
		updateRelacio:           `UPDATE municipi_entitat_religiosa SET municipi_id=$1, nucli_id=$2, entitat_religiosa_id=$3, tipus_relacio=$4, any_inici=$5, any_fi=$6, observacions=$7, moderation_status=$8, updated_at=NOW() WHERE id=$9`,
		deleteRelacio:           `DELETE FROM municipi_entitat_religiosa WHERE id = $1`,
	}
	return q
}

func sqliteListReligioConfessions(d *SQLite) ([]ReligioConfessio, error) {
	return listReligioConfessions(d.Conn, sqliteConfessionalQueries())
}
func postgresListReligioConfessions(d *PostgreSQL) ([]ReligioConfessio, error) {
	return listReligioConfessions(d.Conn, postgresConfessionalQueries())
}
func mysqlListReligioConfessions(d *MySQL) ([]ReligioConfessio, error) {
	return listReligioConfessions(d.Conn, mysqlConfessionalQueries())
}
func sqliteGetReligioConfessio(d *SQLite, id int) (*ReligioConfessio, error) {
	return getReligioConfessio(d.Conn, sqliteConfessionalQueries(), id)
}
func postgresGetReligioConfessio(d *PostgreSQL, id int) (*ReligioConfessio, error) {
	return getReligioConfessio(d.Conn, postgresConfessionalQueries(), id)
}
func mysqlGetReligioConfessio(d *MySQL, id int) (*ReligioConfessio, error) {
	return getReligioConfessio(d.Conn, mysqlConfessionalQueries(), id)
}
func sqliteSaveReligioConfessio(d *SQLite, r *ReligioConfessio) (int, error) {
	return saveReligioConfessio(d.Conn, sqliteConfessionalQueries(), r)
}
func postgresSaveReligioConfessio(d *PostgreSQL, r *ReligioConfessio) (int, error) {
	return saveReligioConfessio(d.Conn, postgresConfessionalQueries(), r)
}
func mysqlSaveReligioConfessio(d *MySQL, r *ReligioConfessio) (int, error) {
	return saveReligioConfessio(d.Conn, mysqlConfessionalQueries(), r)
}
func sqliteDeleteReligioConfessio(d *SQLite, id int) error {
	return deleteReligioConfessio(d.Conn, sqliteConfessionalQueries(), id)
}
func postgresDeleteReligioConfessio(d *PostgreSQL, id int) error {
	return deleteReligioConfessio(d.Conn, postgresConfessionalQueries(), id)
}
func mysqlDeleteReligioConfessio(d *MySQL, id int) error {
	return deleteReligioConfessio(d.Conn, mysqlConfessionalQueries(), id)
}

func sqliteListModelsConfessionals(d *SQLite) ([]ModelConfessional, error) {
	return listModelsConfessionals(d.Conn, sqliteConfessionalQueries())
}
func postgresListModelsConfessionals(d *PostgreSQL) ([]ModelConfessional, error) {
	return listModelsConfessionals(d.Conn, postgresConfessionalQueries())
}
func mysqlListModelsConfessionals(d *MySQL) ([]ModelConfessional, error) {
	return listModelsConfessionals(d.Conn, mysqlConfessionalQueries())
}
func sqliteGetModelConfessional(d *SQLite, id int) (*ModelConfessional, error) {
	return getModelConfessional(d.Conn, sqliteConfessionalQueries(), id)
}
func postgresGetModelConfessional(d *PostgreSQL, id int) (*ModelConfessional, error) {
	return getModelConfessional(d.Conn, postgresConfessionalQueries(), id)
}
func mysqlGetModelConfessional(d *MySQL, id int) (*ModelConfessional, error) {
	return getModelConfessional(d.Conn, mysqlConfessionalQueries(), id)
}
func sqliteSaveModelConfessional(d *SQLite, m *ModelConfessional) (int, error) {
	return saveModelConfessional(d.Conn, sqliteConfessionalQueries(), m)
}
func postgresSaveModelConfessional(d *PostgreSQL, m *ModelConfessional) (int, error) {
	return saveModelConfessional(d.Conn, postgresConfessionalQueries(), m)
}
func mysqlSaveModelConfessional(d *MySQL, m *ModelConfessional) (int, error) {
	return saveModelConfessional(d.Conn, mysqlConfessionalQueries(), m)
}
func sqliteDeleteModelConfessional(d *SQLite, id int) error {
	return deleteModelConfessional(d.Conn, sqliteConfessionalQueries(), id)
}
func postgresDeleteModelConfessional(d *PostgreSQL, id int) error {
	return deleteModelConfessional(d.Conn, postgresConfessionalQueries(), id)
}
func mysqlDeleteModelConfessional(d *MySQL, id int) error {
	return deleteModelConfessional(d.Conn, mysqlConfessionalQueries(), id)
}

func sqliteListNivellsConfessionals(d *SQLite) ([]NivellConfessional, error) {
	return listNivellsConfessionals(d.Conn, sqliteConfessionalQueries())
}
func postgresListNivellsConfessionals(d *PostgreSQL) ([]NivellConfessional, error) {
	return listNivellsConfessionals(d.Conn, postgresConfessionalQueries())
}
func mysqlListNivellsConfessionals(d *MySQL) ([]NivellConfessional, error) {
	return listNivellsConfessionals(d.Conn, mysqlConfessionalQueries())
}
func sqliteGetNivellConfessional(d *SQLite, id int) (*NivellConfessional, error) {
	return getNivellConfessional(d.Conn, sqliteConfessionalQueries(), id)
}
func postgresGetNivellConfessional(d *PostgreSQL, id int) (*NivellConfessional, error) {
	return getNivellConfessional(d.Conn, postgresConfessionalQueries(), id)
}
func mysqlGetNivellConfessional(d *MySQL, id int) (*NivellConfessional, error) {
	return getNivellConfessional(d.Conn, mysqlConfessionalQueries(), id)
}
func sqliteSaveNivellConfessional(d *SQLite, n *NivellConfessional) (int, error) {
	return saveNivellConfessional(d.Conn, sqliteConfessionalQueries(), n)
}
func postgresSaveNivellConfessional(d *PostgreSQL, n *NivellConfessional) (int, error) {
	return saveNivellConfessional(d.Conn, postgresConfessionalQueries(), n)
}
func mysqlSaveNivellConfessional(d *MySQL, n *NivellConfessional) (int, error) {
	return saveNivellConfessional(d.Conn, mysqlConfessionalQueries(), n)
}
func sqliteDeleteNivellConfessional(d *SQLite, id int) error {
	return deleteNivellConfessional(d.Conn, sqliteConfessionalQueries(), id)
}
func postgresDeleteNivellConfessional(d *PostgreSQL, id int) error {
	return deleteNivellConfessional(d.Conn, postgresConfessionalQueries(), id)
}
func mysqlDeleteNivellConfessional(d *MySQL, id int) error {
	return deleteNivellConfessional(d.Conn, mysqlConfessionalQueries(), id)
}

func sqliteListEntitatsReligioses(d *SQLite) ([]EntitatReligiosa, error) {
	return listEntitatsReligioses(d.Conn, sqliteConfessionalQueries())
}
func postgresListEntitatsReligioses(d *PostgreSQL) ([]EntitatReligiosa, error) {
	return listEntitatsReligioses(d.Conn, postgresConfessionalQueries())
}
func mysqlListEntitatsReligioses(d *MySQL) ([]EntitatReligiosa, error) {
	return listEntitatsReligioses(d.Conn, mysqlConfessionalQueries())
}
func sqliteGetEntitatReligiosa(d *SQLite, id int) (*EntitatReligiosa, error) {
	return getEntitatReligiosa(d.Conn, sqliteConfessionalQueries(), id)
}
func postgresGetEntitatReligiosa(d *PostgreSQL, id int) (*EntitatReligiosa, error) {
	return getEntitatReligiosa(d.Conn, postgresConfessionalQueries(), id)
}
func mysqlGetEntitatReligiosa(d *MySQL, id int) (*EntitatReligiosa, error) {
	return getEntitatReligiosa(d.Conn, mysqlConfessionalQueries(), id)
}
func sqliteSaveEntitatReligiosa(d *SQLite, e *EntitatReligiosa) (int, error) {
	return saveEntitatReligiosa(d.Conn, sqliteConfessionalQueries(), e)
}
func postgresSaveEntitatReligiosa(d *PostgreSQL, e *EntitatReligiosa) (int, error) {
	return saveEntitatReligiosa(d.Conn, postgresConfessionalQueries(), e)
}
func mysqlSaveEntitatReligiosa(d *MySQL, e *EntitatReligiosa) (int, error) {
	return saveEntitatReligiosa(d.Conn, mysqlConfessionalQueries(), e)
}
func sqliteDeleteEntitatReligiosa(d *SQLite, id int) error {
	return deleteEntitatReligiosa(d.Conn, sqliteConfessionalQueries(), id)
}
func postgresDeleteEntitatReligiosa(d *PostgreSQL, id int) error {
	return deleteEntitatReligiosa(d.Conn, postgresConfessionalQueries(), id)
}
func mysqlDeleteEntitatReligiosa(d *MySQL, id int) error {
	return deleteEntitatReligiosa(d.Conn, mysqlConfessionalQueries(), id)
}

func sqliteListMunicipiEntitatsReligioses(d *SQLite, municipiID int) ([]MunicipiEntitatReligiosa, error) {
	return listMunicipiEntitatsReligioses(d.Conn, sqliteConfessionalQueries(), municipiID)
}
func postgresListMunicipiEntitatsReligioses(d *PostgreSQL, municipiID int) ([]MunicipiEntitatReligiosa, error) {
	return listMunicipiEntitatsReligioses(d.Conn, postgresConfessionalQueries(), municipiID)
}
func mysqlListMunicipiEntitatsReligioses(d *MySQL, municipiID int) ([]MunicipiEntitatReligiosa, error) {
	return listMunicipiEntitatsReligioses(d.Conn, mysqlConfessionalQueries(), municipiID)
}
func sqliteGetMunicipiEntitatReligiosa(d *SQLite, id int) (*MunicipiEntitatReligiosa, error) {
	return getMunicipiEntitatReligiosa(d.Conn, sqliteConfessionalQueries(), id)
}
func postgresGetMunicipiEntitatReligiosa(d *PostgreSQL, id int) (*MunicipiEntitatReligiosa, error) {
	return getMunicipiEntitatReligiosa(d.Conn, postgresConfessionalQueries(), id)
}
func mysqlGetMunicipiEntitatReligiosa(d *MySQL, id int) (*MunicipiEntitatReligiosa, error) {
	return getMunicipiEntitatReligiosa(d.Conn, mysqlConfessionalQueries(), id)
}
func sqliteSaveMunicipiEntitatReligiosa(d *SQLite, rel *MunicipiEntitatReligiosa) (int, error) {
	return saveMunicipiEntitatReligiosa(d.Conn, sqliteConfessionalQueries(), rel)
}
func postgresSaveMunicipiEntitatReligiosa(d *PostgreSQL, rel *MunicipiEntitatReligiosa) (int, error) {
	return saveMunicipiEntitatReligiosa(d.Conn, postgresConfessionalQueries(), rel)
}
func mysqlSaveMunicipiEntitatReligiosa(d *MySQL, rel *MunicipiEntitatReligiosa) (int, error) {
	return saveMunicipiEntitatReligiosa(d.Conn, mysqlConfessionalQueries(), rel)
}
func sqliteDeleteMunicipiEntitatReligiosa(d *SQLite, id int) error {
	return execDelete(d.Conn, sqliteConfessionalQueries(), "delete_municipi_entitat_religiosa", "municipi_entitat_religiosa", id, sqliteConfessionalQueries().deleteRelacio)
}
func postgresDeleteMunicipiEntitatReligiosa(d *PostgreSQL, id int) error {
	return execDelete(d.Conn, postgresConfessionalQueries(), "delete_municipi_entitat_religiosa", "municipi_entitat_religiosa", id, postgresConfessionalQueries().deleteRelacio)
}
func mysqlDeleteMunicipiEntitatReligiosa(d *MySQL, id int) error {
	return execDelete(d.Conn, mysqlConfessionalQueries(), "delete_municipi_entitat_religiosa", "municipi_entitat_religiosa", id, mysqlConfessionalQueries().deleteRelacio)
}

func confessionalWrap(q confessionalQueries, op, object string, id int, err error) error {
	return WrapSQLError(SQLErrorContext{Engine: q.engine, Component: "confessional", Op: op, Object: object, ObjectID: id}, err)
}

func scanReligion(row interface{ Scan(...interface{}) error }) (*ReligioConfessio, error) {
	var r ReligioConfessio
	if err := row.Scan(&r.ID, &r.Nom, &r.PareID, &r.Descripcio, &r.Estat, &r.Observacions, &r.ModeracioEstat, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return nil, err
	}
	return &r, nil
}

func listReligioConfessions(conn *sql.DB, q confessionalQueries) ([]ReligioConfessio, error) {
	rows, err := conn.Query(q.listReligions)
	if err != nil {
		return nil, confessionalWrap(q, "list_religio_confessio", "religio_confessio", 0, err)
	}
	defer rows.Close()
	var out []ReligioConfessio
	for rows.Next() {
		item, err := scanReligion(rows)
		if err != nil {
			return nil, confessionalWrap(q, "scan_religio_confessio", "religio_confessio", 0, err)
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, confessionalWrap(q, "rows_religio_confessio", "religio_confessio", 0, err)
	}
	return out, nil
}

func getReligioConfessio(conn *sql.DB, q confessionalQueries, id int) (*ReligioConfessio, error) {
	item, err := scanReligion(conn.QueryRow(q.getReligion, id))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, err
		}
		return nil, confessionalWrap(q, "get_religio_confessio", "religio_confessio", id, err)
	}
	return item, nil
}

func saveReligioConfessio(conn *sql.DB, q confessionalQueries, r *ReligioConfessio) (int, error) {
	args := []interface{}{r.Nom, r.PareID, r.Descripcio, r.Estat, r.Observacions, r.ModeracioEstat}
	if r.ID == 0 {
		return execInsert(conn, q, "create_religio_confessio", "religio_confessio", q.insertReligion, args, &r.ID)
	}
	args = append(args, r.ID)
	if _, err := conn.Exec(q.updateReligion, args...); err != nil {
		return 0, confessionalWrap(q, "update_religio_confessio", "religio_confessio", r.ID, err)
	}
	return r.ID, nil
}

func deleteReligioConfessio(conn *sql.DB, q confessionalQueries, id int) error {
	if err := ensureNoRefs(conn, q, "religio_confessio", id, [][2]string{{"religio_confessio", "pare_id"}, {"model_confessional", "religio_confessio_id"}, {"entitat_religiosa", "religio_confessio_id"}}); err != nil {
		return err
	}
	return execDelete(conn, q, "delete_religio_confessio", "religio_confessio", id, q.deleteReligion)
}

func scanModel(row interface{ Scan(...interface{}) error }) (*ModelConfessional, error) {
	var m ModelConfessional
	if err := row.Scan(&m.ID, &m.Nom, &m.ReligioConfessioID, &m.PaisID, &m.Descripcio, &m.AnyInici, &m.AnyFi, &m.Estat, &m.Observacions, &m.ModeracioEstat, &m.CreatedAt, &m.UpdatedAt); err != nil {
		return nil, err
	}
	return &m, nil
}

func listModelsConfessionals(conn *sql.DB, q confessionalQueries) ([]ModelConfessional, error) {
	rows, err := conn.Query(q.listModels)
	if err != nil {
		return nil, confessionalWrap(q, "list_model_confessional", "model_confessional", 0, err)
	}
	defer rows.Close()
	var out []ModelConfessional
	for rows.Next() {
		item, err := scanModel(rows)
		if err != nil {
			return nil, confessionalWrap(q, "scan_model_confessional", "model_confessional", 0, err)
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, confessionalWrap(q, "rows_model_confessional", "model_confessional", 0, err)
	}
	return out, nil
}

func getModelConfessional(conn *sql.DB, q confessionalQueries, id int) (*ModelConfessional, error) {
	item, err := scanModel(conn.QueryRow(q.getModel, id))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, err
		}
		return nil, confessionalWrap(q, "get_model_confessional", "model_confessional", id, err)
	}
	return item, nil
}

func saveModelConfessional(conn *sql.DB, q confessionalQueries, m *ModelConfessional) (int, error) {
	args := []interface{}{m.Nom, m.ReligioConfessioID, m.PaisID, m.Descripcio, m.AnyInici, m.AnyFi, m.Estat, m.Observacions, m.ModeracioEstat}
	if m.ID == 0 {
		return execInsert(conn, q, "create_model_confessional", "model_confessional", q.insertModel, args, &m.ID)
	}
	args = append(args, m.ID)
	if _, err := conn.Exec(q.updateModel, args...); err != nil {
		return 0, confessionalWrap(q, "update_model_confessional", "model_confessional", m.ID, err)
	}
	return m.ID, nil
}

func deleteModelConfessional(conn *sql.DB, q confessionalQueries, id int) error {
	if err := ensureNoRefs(conn, q, "model_confessional", id, [][2]string{{"nivell_confessional", "model_confessional_id"}, {"entitat_religiosa", "model_confessional_id"}}); err != nil {
		return err
	}
	return execDelete(conn, q, "delete_model_confessional", "model_confessional", id, q.deleteModel)
}

func scanNivell(row interface{ Scan(...interface{}) error }) (*NivellConfessional, error) {
	var n NivellConfessional
	if err := row.Scan(&n.ID, &n.ModelConfessionalID, &n.Ordre, &n.NomNivell, &n.NomPlural, &n.TipusNivell, &n.CodiOficial, &n.ParentID, &n.AnyInici, &n.AnyFi, &n.Estat, &n.Observacions, &n.ModeracioEstat, &n.CreatedAt, &n.UpdatedAt); err != nil {
		return nil, err
	}
	return &n, nil
}

func listNivellsConfessionals(conn *sql.DB, q confessionalQueries) ([]NivellConfessional, error) {
	rows, err := conn.Query(q.listNivells)
	if err != nil {
		return nil, confessionalWrap(q, "list_nivell_confessional", "nivell_confessional", 0, err)
	}
	defer rows.Close()
	var out []NivellConfessional
	for rows.Next() {
		item, err := scanNivell(rows)
		if err != nil {
			return nil, confessionalWrap(q, "scan_nivell_confessional", "nivell_confessional", 0, err)
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, confessionalWrap(q, "rows_nivell_confessional", "nivell_confessional", 0, err)
	}
	return out, nil
}

func getNivellConfessional(conn *sql.DB, q confessionalQueries, id int) (*NivellConfessional, error) {
	item, err := scanNivell(conn.QueryRow(q.getNivell, id))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, err
		}
		return nil, confessionalWrap(q, "get_nivell_confessional", "nivell_confessional", id, err)
	}
	return item, nil
}

func saveNivellConfessional(conn *sql.DB, q confessionalQueries, n *NivellConfessional) (int, error) {
	args := []interface{}{n.ModelConfessionalID, n.Ordre, n.NomNivell, n.NomPlural, n.TipusNivell, n.CodiOficial, n.ParentID, n.AnyInici, n.AnyFi, n.Estat, n.Observacions, n.ModeracioEstat}
	if n.ID == 0 {
		return execInsert(conn, q, "create_nivell_confessional", "nivell_confessional", q.insertNivell, args, &n.ID)
	}
	args = append(args, n.ID)
	if _, err := conn.Exec(q.updateNivell, args...); err != nil {
		return 0, confessionalWrap(q, "update_nivell_confessional", "nivell_confessional", n.ID, err)
	}
	return n.ID, nil
}

func deleteNivellConfessional(conn *sql.DB, q confessionalQueries, id int) error {
	if err := ensureNoRefs(conn, q, "nivell_confessional", id, [][2]string{{"nivell_confessional", "parent_id"}, {"entitat_religiosa", "nivell_confessional_id"}}); err != nil {
		return err
	}
	return execDelete(conn, q, "delete_nivell_confessional", "nivell_confessional", id, q.deleteNivell)
}

func scanEntitat(row interface{ Scan(...interface{}) error }) (*EntitatReligiosa, error) {
	var e EntitatReligiosa
	if err := row.Scan(&e.ID, &e.Nom, &e.ReligioConfessioID, &e.ModelConfessionalID, &e.NivellConfessionalID, &e.PaisID, &e.ParentID, &e.TipusEntitat, &e.TipusEspecific, &e.AnyInici, &e.AnyFi, &e.Estat, &e.Web, &e.WebWikipedia, &e.Territori, &e.Observacions, &e.ModeracioEstat, &e.CreatedAt, &e.UpdatedAt); err != nil {
		return nil, err
	}
	return &e, nil
}

func listEntitatsReligioses(conn *sql.DB, q confessionalQueries) ([]EntitatReligiosa, error) {
	rows, err := conn.Query(q.listEntitats)
	if err != nil {
		return nil, confessionalWrap(q, "list_entitat_religiosa", "entitat_religiosa", 0, err)
	}
	defer rows.Close()
	var out []EntitatReligiosa
	for rows.Next() {
		item, err := scanEntitat(rows)
		if err != nil {
			return nil, confessionalWrap(q, "scan_entitat_religiosa", "entitat_religiosa", 0, err)
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, confessionalWrap(q, "rows_entitat_religiosa", "entitat_religiosa", 0, err)
	}
	return out, nil
}

func getEntitatReligiosa(conn *sql.DB, q confessionalQueries, id int) (*EntitatReligiosa, error) {
	item, err := scanEntitat(conn.QueryRow(q.getEntitat, id))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, err
		}
		return nil, confessionalWrap(q, "get_entitat_religiosa", "entitat_religiosa", id, err)
	}
	return item, nil
}

func saveEntitatReligiosa(conn *sql.DB, q confessionalQueries, e *EntitatReligiosa) (int, error) {
	args := []interface{}{e.Nom, e.ReligioConfessioID, e.ModelConfessionalID, e.NivellConfessionalID, e.PaisID, e.ParentID, e.TipusEntitat, e.TipusEspecific, e.AnyInici, e.AnyFi, e.Estat, e.Web, e.WebWikipedia, e.Territori, e.Observacions, e.ModeracioEstat}
	if e.ID == 0 {
		return execInsert(conn, q, "create_entitat_religiosa", "entitat_religiosa", q.insertEntitat, args, &e.ID)
	}
	args = append(args, e.ID)
	if _, err := conn.Exec(q.updateEntitat, args...); err != nil {
		return 0, confessionalWrap(q, "update_entitat_religiosa", "entitat_religiosa", e.ID, err)
	}
	return e.ID, nil
}

func deleteEntitatReligiosa(conn *sql.DB, q confessionalQueries, id int) error {
	if err := ensureNoRefs(conn, q, "entitat_religiosa", id, [][2]string{{"entitat_religiosa", "parent_id"}, {"municipi_entitat_religiosa", "entitat_religiosa_id"}, {"entitat_religiosa_relacio", "entitat_origen_id"}, {"entitat_religiosa_relacio", "entitat_desti_id"}}); err != nil {
		return err
	}
	return execDelete(conn, q, "delete_entitat_religiosa", "entitat_religiosa", id, q.deleteEntitat)
}

func scanRelacio(row interface{ Scan(...interface{}) error }) (*MunicipiEntitatReligiosa, error) {
	var rel MunicipiEntitatReligiosa
	if err := row.Scan(&rel.ID, &rel.MunicipiID, &rel.NucliID, &rel.EntitatReligiosaID, &rel.TipusRelacio, &rel.AnyInici, &rel.AnyFi, &rel.Observacions, &rel.ModeracioEstat, &rel.CreatedAt, &rel.UpdatedAt); err != nil {
		return nil, err
	}
	return &rel, nil
}

func listMunicipiEntitatsReligioses(conn *sql.DB, q confessionalQueries, municipiID int) ([]MunicipiEntitatReligiosa, error) {
	query := q.listRelacionsAll
	args := []interface{}{}
	if municipiID > 0 {
		query = q.listRelacionsByMunicipi
		args = append(args, municipiID)
	}
	rows, err := conn.Query(query, args...)
	if err != nil {
		return nil, confessionalWrap(q, "list_municipi_entitat_religiosa", "municipi_entitat_religiosa", municipiID, err)
	}
	defer rows.Close()
	var out []MunicipiEntitatReligiosa
	for rows.Next() {
		item, err := scanRelacio(rows)
		if err != nil {
			return nil, confessionalWrap(q, "scan_municipi_entitat_religiosa", "municipi_entitat_religiosa", municipiID, err)
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, confessionalWrap(q, "rows_municipi_entitat_religiosa", "municipi_entitat_religiosa", municipiID, err)
	}
	return out, nil
}

func getMunicipiEntitatReligiosa(conn *sql.DB, q confessionalQueries, id int) (*MunicipiEntitatReligiosa, error) {
	item, err := scanRelacio(conn.QueryRow(q.getRelacio, id))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, err
		}
		return nil, confessionalWrap(q, "get_municipi_entitat_religiosa", "municipi_entitat_religiosa", id, err)
	}
	return item, nil
}

func saveMunicipiEntitatReligiosa(conn *sql.DB, q confessionalQueries, rel *MunicipiEntitatReligiosa) (int, error) {
	args := []interface{}{rel.MunicipiID, rel.NucliID, rel.EntitatReligiosaID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.Observacions, rel.ModeracioEstat}
	if rel.ID == 0 {
		return execInsert(conn, q, "create_municipi_entitat_religiosa", "municipi_entitat_religiosa", q.insertRelacio, args, &rel.ID)
	}
	args = append(args, rel.ID)
	if _, err := conn.Exec(q.updateRelacio, args...); err != nil {
		return 0, confessionalWrap(q, "update_municipi_entitat_religiosa", "municipi_entitat_religiosa", rel.ID, err)
	}
	return rel.ID, nil
}

func execInsert(conn *sql.DB, q confessionalQueries, op, object, stmt string, args []interface{}, dest *int) (int, error) {
	if q.returningID {
		if err := conn.QueryRow(stmt, args...).Scan(dest); err != nil {
			return 0, confessionalWrap(q, op, object, 0, err)
		}
		return *dest, nil
	}
	res, err := conn.Exec(stmt, args...)
	if err != nil {
		return 0, confessionalWrap(q, op, object, 0, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, confessionalWrap(q, op, object, 0, err)
	}
	*dest = int(id)
	return *dest, nil
}

func execDelete(conn *sql.DB, q confessionalQueries, op, object string, id int, stmt string) error {
	if _, err := conn.Exec(stmt, id); err != nil {
		return confessionalWrap(q, op, object, id, err)
	}
	return nil
}

func ensureNoRefs(conn *sql.DB, q confessionalQueries, object string, id int, refs [][2]string) error {
	for _, ref := range refs {
		stmt := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = ?", ref[0], ref[1])
		if q.engine == "postgres" {
			stmt = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = $1", ref[0], ref[1])
		}
		var total int
		if err := conn.QueryRow(stmt, id).Scan(&total); err != nil {
			return confessionalWrap(q, "count_refs_"+object, ref[0], id, err)
		}
		if total > 0 {
			return fmt.Errorf("%w: %s %d referenced by %s", ErrUnsafeDelete, object, id, ref[0])
		}
	}
	return nil
}
