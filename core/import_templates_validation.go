package core

import (
	"fmt"
	"strings"
)

const (
	templateMaxColumns           = 200
	templateMaxTargetsPerColumn  = 8
	templateMaxTransformsPerLink = 12
)

var allowedTemplateTransforms = map[string]bool{
	"trim":                                       true,
	"lower":                                      true,
	"strip_diacritics":                           true,
	"normalize_cronologia":                       true,
	"parse_ddmmyyyy_to_iso":                       true,
	"parse_date_flexible_to_base_data_acte":      true,
	"parse_date_flexible_to_date_or_text_with_quality": true,
	"split_couple_i":                             true,
	"set_default":                                true,
	"map_values":                                 true,
	"regex_extract":                              true,
	"default_quality_if_present":                 true,
	"parse_person_from_cognoms":                  true,
	"parse_person_from_nom":                      true,
	"parse_person_from_cognoms_marcmoia_v2":      true,
	"parse_person_from_nom_marcmoia_v2":          true,
	"parse_person_from_cognoms_marcmoia_v2_maternal_first": true,
	"parse_person_from_nom_marcmoia_v2_maternal_first":     true,
	"parse_int_nullable":                         true,
	"parse_marriage_order_int_nullable":          true,
	"strip_marriage_order_text":                  true,
	"extract_parenthetical_last":                 true,
	"extract_parenthetical_all":                  true,
	"strip_parentheticals":                       true,
}

var allowedBaseTargets = map[string]bool{
	"llibre_id":                 true,
	"pagina_id":                 true,
	"num_pagina_text":           true,
	"posicio_pagina":            true,
	"tipus_acte":                true,
	"any_doc":                   true,
	"data_acte_text":            true,
	"data_acte_iso":             true,
	"data_acte_estat":           true,
	"data_acte_iso_text_estat":  true,
	"transcripcio_literal":      true,
	"notes_marginals":           true,
	"observacions_paleografiques": true,
	"moderation_status":         true,
}

var templateTargetCatalog = map[string][]string{
	"common": {
		"base.tipus_acte",
		"base.num_pagina_text",
		"attr.pagina_digital.text",
		"base.any_doc",
		"base.data_acte_text",
		"base.data_acte_iso",
		"base.data_acte_estat",
		"base.data_acte_iso_text_estat",
		"base.transcripcio_literal",
		"base.notes_marginals",
		"base.observacions_paleografiques",
	},
	"baptisme": {
		"person.batejat",
		"person.pare",
		"person.mare",
		"person.mare.cognom_soltera",
		"person.pare.ofici",
		"person.avi_patern",
		"person.avia_paterna",
		"person.avia_paterna.cognom_soltera",
		"person.avi_matern",
		"person.avia_materna",
		"person.avia_materna.cognom_soltera",
		"person.padri",
		"person.padrina",
		"attr.data_bateig.date",
		"attr.data_bateig.date_or_text_with_quality",
		"attr.data_naixement.date",
		"attr.data_naixement.date_or_text_with_quality",
		"attr.data_defuncio.date",
		"attr.data_defuncio.date_or_text_with_quality",
	},
	"obit": {
		"person.difunt",
		"person.pare",
		"person.mare",
		"person.mare.cognom_soltera",
		"person.parella",
		"person.difunt.estat_civil",
		"attr.data_defuncio.date",
		"attr.data_defuncio.date_or_text_with_quality",
		"attr.data_enterrament.date",
		"attr.data_enterrament.date_or_text_with_quality",
		"attr.edat.int",
		"attr.edat.int_nullable",
		"attr.causa.text",
		"attr.classe_enterrament.text",
	},
	"matrimoni": {
		"person.nuvi",
		"person.nuvi.ofici",
		"person.nuvi.edat",
		"person.pare_nuvi",
		"person.mare_nuvi",
		"person.mare_nuvi.cognom_soltera",
		"person.novia",
		"person.novia.edat",
		"person.novia.ofici",
		"person.pare_novia",
		"person.mare_novia",
		"person.mare_novia.cognom_soltera",
		"person.testimoni1",
		"person.testimoni2",
		"attr.data_matrimoni.date",
		"attr.data_matrimoni.date_or_text_with_quality",
	},
	"padro": {
		"person.cap_familia",
		"person.cap_familia.sexe",
		"person.cap_familia.edat",
		"person.cap_familia.estat_civil",
		"person.cap_familia.ofici",
		"person.cap_familia.casa",
		"attr.data_naixement.date",
		"attr.data_naixement.date_or_text_with_quality",
		"attr.carrer.text",
		"attr.numero_casa.text",
		"attr.adreca.text",
		"attr.localitat.text",
		"attr.procedencia.text",
		"attr.alfabetitzat.bool",
		"attr.sap_llegir.bool",
		"attr.sap_escriure.bool",
		"attr.condicio_padro.text",
	},
}

func normalizeTemplateRecordType(recordType string) string {
	recordType = strings.TrimSpace(strings.ToLower(recordType))
	switch recordType {
	case "baptismes", "baptisme", "bateig":
		return "baptisme"
	case "obits", "obit", "defuncio", "defuncions":
		return "obit"
	case "matrimonis", "matrimoni":
		return "matrimoni"
	case "padro", "padrons", "cens", "censos":
		return "padro"
	case "generic":
		return "generic"
	default:
		return ""
	}
}

func allowedTemplateTargetsForRecordType(recordType string) map[string]bool {
	recordType = normalizeTemplateRecordType(recordType)
	if recordType == "" {
		return nil
	}
	allow := map[string]bool{}
	for _, target := range templateTargetCatalog["common"] {
		allow[target] = true
	}
	switch recordType {
	case "baptisme", "obit", "matrimoni", "padro":
		for _, target := range templateTargetCatalog[recordType] {
			allow[target] = true
		}
	case "generic":
		return nil
	}
	return allow
}

func validateTemplateImportModel(model *templateImportModel) error {
	if model == nil {
		return fmt.Errorf("plantilla buida")
	}
	allowedTargets := allowedTemplateTargetsForRecordType(model.RecordType)
	if len(model.Mapping) > templateMaxColumns {
		return fmt.Errorf("massa columnes a la plantilla")
	}
	for idx, col := range model.Mapping {
		if strings.TrimSpace(col.Header) == "" {
			return fmt.Errorf("capçalera buida a la columna %d", idx+1)
		}
		if len(col.MapTo) > templateMaxTargetsPerColumn {
			return fmt.Errorf("massa map_to a la columna %q", col.Header)
		}
		if err := validateTemplateMapTo(col.MapTo, allowedTargets); err != nil {
			return err
		}
		if col.Condition != nil {
			if err := validateTemplateConditionExpr(col.Condition.Expr); err != nil {
				return err
			}
			if len(col.Condition.Then.MapTo) > templateMaxTargetsPerColumn {
				return fmt.Errorf("massa map_to a la condició THEN de %q", col.Header)
			}
			if err := validateTemplateMapTo(col.Condition.Then.MapTo, allowedTargets); err != nil {
				return err
			}
			if err := validateTemplateTransforms(col.Condition.Then.Transforms); err != nil {
				return err
			}
			if col.Condition.Else != nil {
				if len(col.Condition.Else.MapTo) > templateMaxTargetsPerColumn {
					return fmt.Errorf("massa map_to a la condició ELSE de %q", col.Header)
				}
				if err := validateTemplateMapTo(col.Condition.Else.MapTo, allowedTargets); err != nil {
					return err
				}
				if err := validateTemplateTransforms(col.Condition.Else.Transforms); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateTemplateConditionExpr(expr string) error {
	expr = strings.TrimSpace(expr)
	if expr == "" || strings.ToLower(expr) == "not_empty" {
		return nil
	}
	if strings.Contains(expr, "==") || strings.Contains(expr, "!=") {
		op := "=="
		if strings.Contains(expr, "!=") {
			op = "!="
		}
		parts := strings.Split(expr, op)
		if len(parts) != 2 {
			return fmt.Errorf("condició invàlida")
		}
		if strings.TrimSpace(parts[1]) == "" {
			return fmt.Errorf("condició invàlida")
		}
		return nil
	}
	return fmt.Errorf("condició invàlida")
}

func validateTemplateMapTo(list []templateMapTo, allowedTargets map[string]bool) error {
	for _, entry := range list {
		if strings.TrimSpace(entry.Target) == "" {
			return fmt.Errorf("target de mapping buit")
		}
		if err := validateTemplateTargetWithAllowlist(entry.Target, allowedTargets); err != nil {
			return err
		}
		if err := validateTemplateTransforms(entry.Transforms); err != nil {
			return err
		}
		if entry.Condition != nil {
			if err := validateTemplateInlineCondition(entry.Condition); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateTemplateTargetWithAllowlist(target string, allowedTargets map[string]bool) error {
	if err := validateTemplateTarget(target); err != nil {
		return err
	}
	if allowedTargets != nil {
		if !allowedTargets[strings.TrimSpace(target)] {
			return fmt.Errorf("target no permès per aquest tipus de plantilla: %s", target)
		}
	}
	return nil
}

func validateTemplateTransforms(list []templateTransform) error {
	if len(list) > templateMaxTransformsPerLink {
		return fmt.Errorf("massa transforms al mapping")
	}
	for _, tr := range list {
		name := strings.ToLower(strings.TrimSpace(tr.Name))
		if name == "" || !allowedTemplateTransforms[name] {
			return fmt.Errorf("transform no suportat: %s", tr.Name)
		}
		switch name {
		case "map_values":
			if tr.Args == nil || len(tr.Args) == 0 {
				return fmt.Errorf("map_values sense args")
			}
		case "regex_extract":
			if strings.TrimSpace(asString(tr.Args["pattern"])) == "" {
				return fmt.Errorf("regex_extract sense patró")
			}
		case "set_default":
			if strings.TrimSpace(tr.Value) == "" && strings.TrimSpace(asString(tr.Args["value"])) == "" {
				return fmt.Errorf("set_default sense valor")
			}
		case "split_couple_i":
			if sel := strings.TrimSpace(asString(tr.Args["select"])); sel != "" && sel != "left" && sel != "right" {
				return fmt.Errorf("split_couple_i select invàlid")
			}
		}
	}
	return nil
}

func validateTemplateInlineCondition(cond *templateInlineCondition) error {
	if cond == nil {
		return nil
	}
	op := strings.ToLower(strings.TrimSpace(cond.Op))
	switch op {
	case "not_empty":
		if strings.TrimSpace(asString(cond.Args["column"])) == "" {
			return fmt.Errorf("condició not_empty sense columna")
		}
	case "equals":
		if strings.TrimSpace(asString(cond.Args["column"])) == "" {
			return fmt.Errorf("condició equals sense columna")
		}
		if strings.TrimSpace(asString(cond.Args["value"])) == "" {
			return fmt.Errorf("condició equals sense valor")
		}
	default:
		return fmt.Errorf("condició inline no suportada")
	}
	return nil
}

func validateTemplateTarget(target string) error {
	target = strings.TrimSpace(target)
	if strings.HasPrefix(target, "base.") {
		field := strings.TrimPrefix(target, "base.")
		baseField := strings.Split(field, ".")[0]
		if !allowedBaseTargets[baseField] {
			return fmt.Errorf("target base no vàlid: %s", target)
		}
		return nil
	}
	if strings.HasPrefix(target, "person.") {
		field := strings.TrimPrefix(target, "person.")
		parts := strings.Split(field, ".")
		if strings.TrimSpace(parts[0]) == "" {
			return fmt.Errorf("target person sense rol")
		}
		return nil
	}
	if strings.HasPrefix(target, "attr.") {
		field := strings.TrimPrefix(target, "attr.")
		parts := strings.Split(field, ".")
		if strings.TrimSpace(parts[0]) == "" {
			return fmt.Errorf("target attr sense clau")
		}
		return nil
	}
	return fmt.Errorf("target no suportat: %s", target)
}
