package core

import "strings"

func normalizeNivellKindKey(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	raw = strings.ReplaceAll(raw, "_", "")
	raw = strings.ReplaceAll(raw, " ", "")
	return raw
}

func nivellKindAliasesForTargetKind(targetKind string) []string {
	normalized := normalizeNivellKindKey(targetKind)
	switch normalized {
	case "", "nivelladministratiu":
		return nil
	case "comarca":
		return []string{"comarca"}
	case "provincia":
		return []string{"provincia"}
	case "comunitatautonoma":
		return []string{"comunitatautonoma", "autonomia", "regioautonoma"}
	case "estat":
		return []string{"estat", "pais"}
	default:
		return []string{normalized}
	}
}

func nivellKindMatchesAliases(levelType string, aliases []string) bool {
	if len(aliases) == 0 {
		return true
	}
	current := normalizeNivellKindKey(levelType)
	for _, alias := range aliases {
		if current == alias {
			return true
		}
	}
	return false
}

func nivellKindMatchesTargetKind(targetKind, levelType string) bool {
	if normalizeNivellKindKey(targetKind) == "nivelladministratiu" {
		return strings.TrimSpace(levelType) != ""
	}
	return nivellKindMatchesAliases(levelType, nivellKindAliasesForTargetKind(targetKind))
}
