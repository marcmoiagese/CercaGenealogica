package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
)

// ComputeTemplateSignature returns a stable hash of the template JSON model.
func ComputeTemplateSignature(modelJSON string) string {
	normalized := normalizeTemplateJSON(modelJSON)
	sum := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(sum[:])
}

func normalizeTemplateJSON(modelJSON string) string {
	payload := strings.TrimSpace(modelJSON)
	if payload == "" {
		return ""
	}
	var data interface{}
	if err := json.Unmarshal([]byte(payload), &data); err != nil {
		return payload
	}
	var buf bytes.Buffer
	writeCanonicalJSON(&buf, data)
	return buf.String()
}

func writeCanonicalJSON(buf *bytes.Buffer, value interface{}) {
	switch v := value.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			keyJSON, _ := json.Marshal(key)
			buf.Write(keyJSON)
			buf.WriteByte(':')
			writeCanonicalJSON(buf, v[key])
		}
		buf.WriteByte('}')
	case []interface{}:
		buf.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				buf.WriteByte(',')
			}
			writeCanonicalJSON(buf, item)
		}
		buf.WriteByte(']')
	default:
		dataJSON, _ := json.Marshal(v)
		buf.Write(dataJSON)
	}
}
