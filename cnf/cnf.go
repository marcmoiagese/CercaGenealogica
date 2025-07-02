package cnf

import (
	"bufio"
	"log"
	"os"
	"strings"
)

// Config – Variable pública amb les opcions de configuració
var Config map[string]string

func LoadConfig(path string) map[string]string {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("No s'ha pogut obrir el fitxer de configuració: %v", err)
	}
	defer file.Close()

	config := make(map[string]string)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "=") {
			parts := strings.SplitN(line, "=", 2)
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			config[key] = value
		}
	}

	return config
}
