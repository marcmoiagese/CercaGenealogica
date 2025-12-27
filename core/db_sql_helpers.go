package core

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func dbStyleFromDB(database db.DB) string {
	switch database.(type) {
	case *db.PostgreSQL:
		return "postgres"
	case *db.MySQL:
		return "mysql"
	case *db.SQLite:
		return "sqlite"
	default:
		return ""
	}
}

func formatSQLPlaceholders(style, query string) string {
	if style != "postgres" {
		return query
	}
	var out strings.Builder
	out.Grow(len(query))
	idx := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			out.WriteString("$")
			out.WriteString(strconv.Itoa(idx))
			idx++
			continue
		}
		out.WriteByte(query[i])
	}
	return out.String()
}

func formatSQLForDB(database db.DB, query string) string {
	return formatSQLPlaceholders(dbStyleFromDB(database), query)
}

func rowString(row map[string]interface{}, key string) string {
	val, ok := row[key]
	if !ok || val == nil {
		return ""
	}
	switch v := val.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprint(v)
	}
}

func rowInt(row map[string]interface{}, key string) int {
	val, ok := row[key]
	if !ok || val == nil {
		return 0
	}
	switch v := val.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case int32:
		return int(v)
	case float64:
		return int(v)
	case []byte:
		if n, err := strconv.Atoi(string(v)); err == nil {
			return n
		}
	case string:
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return 0
}
