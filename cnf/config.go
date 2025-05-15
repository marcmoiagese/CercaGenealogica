package cnf

import (
	"fmt"
	"os"

	"github.com/marcmoiagese/CercaGenealogica/db"
	"gopkg.in/yaml.v3"
)

type YamlConfig struct {
	Database struct {
		Type     string `yaml:"type"`
		Postgres struct {
			Host     string `yaml:"host"`
			Port     int    `yaml:"port"`
			User     string `yaml:"user"`
			Password string `yaml:"password"`
			DBName   string `yaml:"dbname"`
			SSLMode  string `yaml:"sslmode"`
		} `yaml:"postgresql"`
		SQLite struct {
			Path string `yaml:"path"`
		} `yaml:"sqlite"`
	} `yaml:"database"`
}

func LoadConfig() (*db.Config, error) {
	config := &YamlConfig{}

	file, err := os.Open("cnf/config.yaml")
	if err != nil {
		return nil, fmt.Errorf("error obrint fitxer de configuració: %v", err)
	}
	defer file.Close()

	if err := yaml.NewDecoder(file).Decode(config); err != nil {
		return nil, fmt.Errorf("error decodificant YAML: %v", err)
	}

	// Converteix a l'estructura que espera el paquet db
	dbConfig := db.Config{
		Type: config.Database.Type,
		Postgres: db.PostgreSQLConfig{
			Host:     config.Database.Postgres.Host,
			Port:     config.Database.Postgres.Port,
			User:     config.Database.Postgres.User,
			Password: config.Database.Postgres.Password,
			DBName:   config.Database.Postgres.DBName,
			SSLMode:  config.Database.Postgres.SSLMode,
		},
		SQLite: db.SQLiteConfig{
			Path: config.Database.SQLite.Path,
		},
	}

	return &dbConfig, nil
}
