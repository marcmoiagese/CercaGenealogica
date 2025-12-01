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

func Infof(format string, v ...interface{}) {
	if currentLevel >= logInfo {
		log.Printf("[INFO] "+format, v...)
	}
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
