package db

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteDB implementa DBManager per SQLite
type SQLiteDB struct {
	db *sql.DB
}

// Init inicialitza la connexió i crea les taules si no existeixen
func (s *SQLiteDB) Init() error {
	db, err := sql.Open("sqlite3", "./database.db")
	if err != nil {
		return err
	}
	s.db = db

	// Crear taula principal si no existeix
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS usuaris (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom TEXT NOT NULL,
            cognom1 TEXT NOT NULL,
            cognom2 TEXT NOT NULL,
            municipi TEXT,
            arquevisbat TEXT,
            nom_complet TEXT,
            pagina TEXT,
            llibre TEXT,
            any TEXT
        );
    `)
	if err != nil {
		return err
	}

	// Crear taula duplicats si no existeix
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS usuaris_possibles_duplicats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom TEXT NOT NULL,
            cognom1 TEXT NOT NULL,
            cognom2 TEXT NOT NULL,
            municipi TEXT,
            arquevisbat TEXT,
            nom_complet TEXT,
            pagina TEXT,
            llibre TEXT,
            any TEXT
        );
    `)
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(5 * time.Minute)
	return nil
}

// Close tanca la connexió activa
func (s *SQLiteDB) Close() {
	if s.db != nil {
		s.db.Close()
	}
}

// DB retorna la connexió SQL neta
func (s *SQLiteDB) DB() *sql.DB {
	return s.db
}

// InsertUsuari insereix un usuari a la taula principal
func (s *SQLiteDB) InsertUsuari(nom, c1, c2, muni, arq, nc, pag, lb, y string) error {
	stmt, err := s.db.Prepare("INSERT INTO usuaris(nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, any) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(nom, c1, c2, muni, arq, nc, pag, lb, y)
	return err
}

// CheckDuplicate comprova duplicats
func (s *SQLiteDB) CheckDuplicate(c1, c2, nom, pag, lb, y string) (bool, error) {
	exists := 0
	var err error
	if nom != "" {
		err = s.db.QueryRow(`
            SELECT COUNT(*) FROM usuaris 
            WHERE cognom1 = ? AND cognom2 = ? AND nom = ? AND pagina = ? AND llibre = ? AND any = ?
        `, c1, c2, nom, pag, lb, y).Scan(&exists)
	} else {
		err = s.db.QueryRow(`
            SELECT COUNT(*) FROM usuaris 
            WHERE cognom1 = ? AND cognom2 = ? AND pagina = ? AND llibre = ? AND any = ?
        `, c1, c2, pag, lb, y).Scan(&exists)
	}
	return exists > 0, err
}

// GetPossibleDuplicates retorna tots els possibles duplicats
func (s *SQLiteDB) GetPossibleDuplicates() ([]map[string]string, error) {
	rows, err := s.db.Query("SELECT id, cognom1, cognom2, pagina, llibre, any FROM usuaris_possibles_duplicats")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pendentList []map[string]string
	for rows.Next() {
		var id int
		var c1, c2, pag, lb, y string
		err := rows.Scan(&id, &c1, &c2, &pag, &lb, &y)
		if err != nil {
			log.Println("Error llegint registre:", err)
			continue
		}
		pendentList = append(pendentList, map[string]string{
			"id":      strconv.Itoa(id),
			"cognoms": c1 + " " + c2,
			"pagina":  pag,
			"llibre":  lb,
			"any":     y,
		})
	}
	return pendentList, nil
}

// DeleteDuplicates esborra els duplicats seleccionats
func (s *SQLiteDB) DeleteDuplicates(ids []int) error {
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, v := range ids {
		placeholders[i] = "?"
		args[i] = v
	}
	whereClause := strings.Join(placeholders, ",")
	_, err := s.db.Exec(fmt.Sprintf("DELETE FROM usuaris_possibles_duplicats WHERE id IN (%s)", whereClause), args...)
	return err
}

// ImportSelectedDuplicates mou els duplicats seleccionats a la taula principal
func (s *SQLiteDB) ImportSelectedDuplicates(ids []int) error {
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, v := range ids {
		placeholders[i] = "?"
		args[i] = v
	}
	whereClause := strings.Join(placeholders, ",")

	// Obtenir registres seleccionats
	rows, err := s.db.Query(fmt.Sprintf("SELECT nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, any FROM usuaris_possibles_duplicats WHERE id IN (%s)", whereClause), args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Inserció definitiva
	stmt, _ := s.db.Prepare("INSERT INTO usuaris(nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, any) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
	defer stmt.Close()

	for rows.Next() {
		var nom, c1, c2, muni, arq, nc, pag, lb, y string
		err := rows.Scan(&nom, &c1, &c2, &muni, &arq, &nc, &pag, &lb, &y)
		if err != nil {
			log.Println("Error llegint registre:", err)
			continue
		}
		_, err = stmt.Exec(nom, c1, c2, muni, arq, nc, pag, lb, y)
		if err != nil {
			log.Println("Error inserint registre:", err)
		}
	}
	return nil
}
