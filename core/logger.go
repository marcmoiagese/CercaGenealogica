package core

import (
	"log"
	"os"
	"strings"
)

type logLevel int

const (
	logSilent logLevel = iota
	logError
	logInfo
	logDebug
)

var currentLevel = logError

func SetLogLevel(levelStr string) {
	switch strings.ToLower(strings.TrimSpace(levelStr)) {
	case "silent", "error":
		currentLevel = logError
	case "info", "":
		currentLevel = logInfo
	case "debug":
		currentLevel = logDebug
	default:
		currentLevel = logInfo
	}
	log.Printf("[log] nivell configurat: %s", strings.ToLower(strings.TrimSpace(levelStr)))
}

func Debugf(format string, v ...interface{}) {
	if currentLevel >= logDebug {
		log.Printf("[DEBUG] "+format, v...)
	}
}

func IsDebugEnabled() bool {
	return currentLevel >= logDebug
}

func IsImportProfileEnabled() bool {
	return strings.TrimSpace(os.Getenv("CG_IMPORT_PROFILE")) == "1"
}

func IsPostgresStagingProfileEnabled() bool {
	return strings.TrimSpace(os.Getenv("CG_POSTGRES_STAGING_PROFILE")) == "1"
}

func IsPostgresStagingWholeImportEnabled() bool {
	return strings.TrimSpace(os.Getenv("CG_POSTGRES_STAGING_WHOLE_IMPORT")) == "1"
}

func IsPostgresDirectChildCopyEnabled() bool {
	return strings.TrimSpace(os.Getenv("CG_POSTGRES_DIRECT_CHILD_COPY")) == "1"
}

func Infof(format string, v ...interface{}) {
	if currentLevel >= logInfo {
		log.Printf("[INFO] "+format, v...)
	}
}

func PostgresStagingProfilef(format string, v ...interface{}) {
	log.Printf("[POSTGRES_STAGING_PROFILE] "+format, v...)
}

func Errorf(format string, v ...interface{}) {
	if currentLevel >= logError {
		log.Printf("[ERROR] "+format, v...)
	}
}

// AttachLogger allow redirecting output if needed in the future.
func AttachLoggerOutput(file *os.File) {
	log.SetOutput(file)
}
