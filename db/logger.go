package db

import (
	"log"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
)

func logLevel() string {
	if cnf.Config == nil {
		return "info"
	}
	l := strings.ToLower(strings.TrimSpace(cnf.Config["LOG_LEVEL"]))
	if l == "" {
		return "info"
	}
	return l
}

func logInfof(format string, v ...interface{}) {
	l := logLevel()
	if l == "silent" || l == "error" {
		return
	}
	log.Printf("[DB] "+format, v...)
}

func logErrorf(format string, v ...interface{}) {
	log.Printf("[DB][ERROR] "+format, v...)
}
