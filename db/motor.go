package db

import (
	"fmt"
)

// DBManager defineix la interfície comuna per tots els motors de BD
type DBManager interface {
	Init() error
	Close()
	InsertUsuari(nom, c1, c2, muni, arq, nc, pag, lb, y string) error
	CheckDuplicate(c1, c2, nom, pag, lb, y string) (bool, error)
	GetPossibleDuplicates() ([]map[string]string, error)
	DeleteDuplicates(ids []int) error
	ImportSelectedDuplicates(ids []int) error
}

var defaultManager DBManager

// GetDBManager retorna una instància del motor segons DB_ENGINE
func GetDBManager(engine string) (DBManager, error) {
	switch engine {
	case "sqlite":
		defaultManager = &SQLiteDB{}
	default:
		return nil, fmt.Errorf("motor de base de dades no suportat: %s", engine)
	}
	return defaultManager, nil
}

// Close tanca la connexió activa
func Close() {
	if defaultManager != nil {
		defaultManager.Close()
	}
}
