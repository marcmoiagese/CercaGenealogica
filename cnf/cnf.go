package cnf

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config – Variable pública amb les opcions de configuració
var Config map[string]string

// AppConfig – Configuració tipada per facilitar l'ús
type AppConfig struct {
	DBEngine     string
	DBPath       string
	RecreaDB     bool
	RegisterD    bool
	LogLevel     string
	Env          string
	DBHost       string
	DBUser       string
	DBPass       string
	DBPort       string
	DBName       string
	MailEnabled  bool
	MailFrom     string
	MailSMTPHost string
	MailSMTPPort string
}

// LoadConfig carrega el fitxer en format clau=valor, ignorant línies buides o comentaris.
func LoadConfig(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("no s'ha pogut obrir el fitxer de configuració: %w", err)
	}
	defer file.Close()

	config := make(map[string]string)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.Contains(line, "=") {
			parts := strings.SplitN(line, "=", 2)
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			if value != "" {
				commentIdx := -1
				for _, marker := range []string{" #", "\t#", " ;", "\t;"} {
					if idx := strings.Index(value, marker); idx >= 0 && (commentIdx == -1 || idx < commentIdx) {
						commentIdx = idx
					}
				}
				if commentIdx >= 0 {
					value = strings.TrimSpace(value[:commentIdx])
				}
			}
			config[key] = value
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error llegint config: %w", err)
	}

	Config = config
	return config, nil
}

// ParseConfig converteix map[string]string en AppConfig amb valors per defecte.
func ParseConfig(cfg map[string]string) (AppConfig, error) {
	ac := AppConfig{
		DBEngine: strings.TrimSpace(cfg["DB_ENGINE"]),
		DBPath:   cfg["DB_PATH"],
		LogLevel: strings.TrimSpace(cfg["LOG_LEVEL"]),
		Env:      strings.TrimSpace(cfg["ENVIRONMENT"]),
		DBHost:   cfg["DB_HOST"],
		DBUser:   cfg["DB_USR"],
		DBPass:   cfg["DB_PASS"],
		DBPort:   cfg["DB_PORT"],
		DBName:   cfg["DB_NAME"],
	}

	if ac.DBEngine == "" {
		ac.DBEngine = "sqlite"
	}
	if ac.DBPath == "" {
		ac.DBPath = "./database.db"
	}
	if ac.LogLevel == "" {
		ac.LogLevel = "info"
	}
	if ac.Env == "" {
		ac.Env = os.Getenv("ENVIRONMENT")
		if ac.Env == "" {
			ac.Env = "development"
		}
	}

	if v, ok := cfg["RECREADB"]; ok {
		ac.RecreaDB, _ = strconv.ParseBool(strings.ToLower(strings.TrimSpace(v)))
	}
	if v, ok := cfg["REGISTERD"]; ok {
		ac.RegisterD, _ = strconv.ParseBool(strings.ToLower(strings.TrimSpace(v)))
	}
	if v, ok := cfg["MAIL_ENABLED"]; ok {
		ac.MailEnabled, _ = strconv.ParseBool(strings.ToLower(strings.TrimSpace(v)))
	}

	ac.MailFrom = strings.TrimSpace(cfg["MAIL_FROM"])
	if ac.MailFrom == "" {
		ac.MailFrom = "no-reply@localhost"
	}

	ac.MailSMTPHost = strings.TrimSpace(cfg["MAIL_SMTP_HOST"])
	if ac.MailSMTPHost == "" {
		ac.MailSMTPHost = "localhost"
	}

	ac.MailSMTPPort = strings.TrimSpace(cfg["MAIL_SMTP_PORT"])
	if ac.MailSMTPPort == "" {
		ac.MailSMTPPort = "25"
	}

	return ac, nil
}
