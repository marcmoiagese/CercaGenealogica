package db

import (
	"context"
	"database/sql"
	"errors"
)

type entitatReligiosaInitialRelationsTxOps struct {
	engine string
	q      confessionalQueries
	begin  func() (*sql.Tx, error)
}

func sqliteSaveEntitatReligiosaWithInitialRelationsTx(d *SQLite, plan *EntitatReligiosaInitialRelationsTxPlan) (*EntitatReligiosaInitialRelationsTxResult, error) {
	return runEntitatReligiosaWithInitialRelationsTx(entitatReligiosaInitialRelationsTxOps{
		engine: "sqlite",
		q:      sqliteConfessionalQueries(),
		begin:  func() (*sql.Tx, error) { return d.Conn.BeginTx(context.Background(), nil) },
	}, plan)
}

func postgresSaveEntitatReligiosaWithInitialRelationsTx(d *PostgreSQL, plan *EntitatReligiosaInitialRelationsTxPlan) (*EntitatReligiosaInitialRelationsTxResult, error) {
	return runEntitatReligiosaWithInitialRelationsTx(entitatReligiosaInitialRelationsTxOps{
		engine: "postgres",
		q:      postgresConfessionalQueries(),
		begin:  func() (*sql.Tx, error) { return d.Conn.BeginTx(context.Background(), nil) },
	}, plan)
}

func mysqlSaveEntitatReligiosaWithInitialRelationsTx(d *MySQL, plan *EntitatReligiosaInitialRelationsTxPlan) (*EntitatReligiosaInitialRelationsTxResult, error) {
	return runEntitatReligiosaWithInitialRelationsTx(entitatReligiosaInitialRelationsTxOps{
		engine: "mysql",
		q:      mysqlConfessionalQueries(),
		begin:  func() (*sql.Tx, error) { return d.Conn.BeginTx(context.Background(), nil) },
	}, plan)
}

func runEntitatReligiosaWithInitialRelationsTx(ops entitatReligiosaInitialRelationsTxOps, plan *EntitatReligiosaInitialRelationsTxPlan) (_ *EntitatReligiosaInitialRelationsTxResult, err error) {
	if plan == nil || plan.Entitat == nil {
		return nil, confessionalInitialRelationsTxWrap(ops.engine, "invalid_plan", "entitat_religiosa", 0, errors.New("pla d'entitat religiosa buit"))
	}
	tx, err := ops.begin()
	if err != nil {
		return nil, confessionalInitialRelationsTxWrap(ops.engine, "begin_tx", "entitat_religiosa", 0, err)
	}
	defer func() {
		if err == nil {
			return
		}
		_ = tx.Rollback()
	}()

	entityID, err := saveEntitatReligiosaTx(tx, ops.q, plan.Entitat)
	if err != nil {
		return nil, err
	}
	result := &EntitatReligiosaInitialRelationsTxResult{EntitatID: entityID}

	if plan.ParentRelation != nil {
		rel := *plan.ParentRelation
		if rel.EntitatDestiID == 0 {
			rel.EntitatDestiID = entityID
		}
		result.ParentRelationID, err = insertEntitatReligiosaRelacioTx(tx, ops.q, &rel)
		if err != nil {
			return nil, err
		}
	}

	if plan.TerritoryRelation != nil {
		rel := *plan.TerritoryRelation
		if rel.EntitatReligiosaID == 0 {
			rel.EntitatReligiosaID = entityID
		}
		result.TerritoryRelationID, err = insertMunicipiEntitatReligiosaTx(tx, ops.q, &rel)
		if err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, confessionalInitialRelationsTxWrap(ops.engine, "commit_tx", "entitat_religiosa", entityID, err)
	}
	return result, nil
}

func saveEntitatReligiosaTx(tx *sql.Tx, q confessionalQueries, e *EntitatReligiosa) (int, error) {
	args := []interface{}{nullableStringArg(e.Codi), e.Nom, nullableStringArg(e.ReligioConfessioCodi), nullableStringArg(e.NivellConfessionalCodi), e.ReligioConfessioID, e.ModelConfessionalID, e.NivellConfessionalID, e.PaisID, e.ParentID, e.TipusEntitat, e.TipusEspecific, e.AnyInici, e.AnyFi, e.Estat, e.Web, e.WebWikipedia, e.Territori, e.Descripcio, e.Observacions, e.ModeracioEstat, e.ModeracioMotiu, e.CreatedBy, e.UpdatedBy, e.ModeratedBy, e.ModeratedAt}
	if e.ID == 0 {
		return execInsertTx(tx, q, "create_entitat_religiosa_tx", "entitat_religiosa", q.insertEntitat, args, &e.ID)
	}
	args = append(args, e.ID)
	if _, err := tx.Exec(q.updateEntitat, args...); err != nil {
		return 0, confessionalInitialRelationsTxWrap(q.engine, "update_entitat_religiosa_tx", "entitat_religiosa", e.ID, err)
	}
	return e.ID, nil
}

func insertEntitatReligiosaRelacioTx(tx *sql.Tx, q confessionalQueries, rel *EntitatReligiosaRelacio) (int, error) {
	args := []interface{}{rel.EntitatOrigenID, rel.EntitatDestiID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.FontID, rel.Observacions, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt}
	return execInsertTx(tx, q, "create_entitat_religiosa_relacio_tx", "entitat_religiosa_relacio", q.insertEntitatRelacio, args, &rel.ID)
}

func insertMunicipiEntitatReligiosaTx(tx *sql.Tx, q confessionalQueries, rel *MunicipiEntitatReligiosa) (int, error) {
	args := []interface{}{rel.MunicipiID, rel.NucliID, rel.EntitatReligiosaID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.Observacions, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt}
	return execInsertTx(tx, q, "create_municipi_entitat_religiosa_tx", "municipi_entitat_religiosa", q.insertRelacio, args, &rel.ID)
}

func execInsertTx(tx *sql.Tx, q confessionalQueries, op, object, stmt string, args []interface{}, dest *int) (int, error) {
	if q.returningID {
		if err := tx.QueryRow(stmt, args...).Scan(dest); err != nil {
			return 0, confessionalInitialRelationsTxWrap(q.engine, op, object, 0, err)
		}
		return *dest, nil
	}
	res, err := tx.Exec(stmt, args...)
	if err != nil {
		return 0, confessionalInitialRelationsTxWrap(q.engine, op, object, 0, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, confessionalInitialRelationsTxWrap(q.engine, op, object, 0, err)
	}
	*dest = int(id)
	return *dest, nil
}

func confessionalInitialRelationsTxWrap(engine, op, object string, id int, err error) error {
	return WrapSQLError(SQLErrorContext{Engine: engine, Component: "confessional_initial_relations_tx", Op: op, Object: object, ObjectID: id}, err)
}
