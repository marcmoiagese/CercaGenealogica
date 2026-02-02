package core

import (
	"database/sql"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) EnsureSystemImportTemplates() error {
	if a == nil || a.DB == nil {
		return nil
	}
	existing, err := a.DB.ListCSVImportTemplates(db.CSVImportTemplateFilter{
		IncludePublic: true,
		Limit:         500,
	})
	if err != nil {
		return err
	}
	hasGeneric := false
	hasMarcmoia := false
	for _, tpl := range existing {
		name := strings.TrimSpace(tpl.Name)
		if name == "System: Generic" && !tpl.OwnerUserID.Valid {
			hasGeneric = true
		}
		if name == "System: Baptismes Marcmoia (v2)" && !tpl.OwnerUserID.Valid {
			hasMarcmoia = true
		}
	}
	if !hasGeneric {
		_, err := a.DB.CreateCSVImportTemplate(&db.CSVImportTemplate{
			Name:             "System: Generic",
			Description:      "Plantilla base per a capçaleres estàndard.",
			OwnerUserID:      sql.NullInt64{},
			Visibility:       "public",
			DefaultSeparator: ";",
			ModelJSON: `{
  "version": 1,
  "kind": "transcripcions_raw",
  "preset": "system",
  "preset_code": "generic_v1",
  "objective": "Format generic basat en capçaleres estàndard.",
  "header_normalization": "cg_normalize_csv_header_v1",
  "mapping": {
    "columns": []
  }
}`,
		})
		if err != nil {
			return err
		}
	}
	if !hasMarcmoia {
		_, err := a.DB.CreateCSVImportTemplate(&db.CSVImportTemplate{
			Name:             "System: Baptismes Marcmoia (v2)",
			Description:      "Plantilla del preset Marcmoia amb parsers avançats.",
			OwnerUserID:      sql.NullInt64{},
			Visibility:       "public",
			DefaultSeparator: ";",
			ModelJSON: `{
  "version": 1,
  "kind": "transcripcions_raw",
  "preset": "system",
  "preset_code": "baptismes_marcmoia_v2",
  "objective": "Replicar importBaptismesMarcmoiaCSV + millores (dates parcials, parèntesis, matrimonis numerats)",
  "header_normalization": "cg_normalize_csv_header_v1",
  "book_resolution": {
    "mode": "cronologia_lookup",
    "column": "llibre",
    "normalize_cronologia": true,
    "ambiguity_policy": "fail",
    "context_filters": ["municipi_id", "arxiu_id"]
  },
  "base_defaults": {
    "tipus_acte": "baptisme",
    "moderation_status": "pendent"
  },
  "mapping": {
    "columns": [
      { "header": "Llibre", "key": "llibre", "required": true, "aliases": ["cronologia", "periode", "rang", "rango"] },

      { "header": "Pàgina llibre", "key": "pagina_llibre", "required": false, "aliases": ["paginallibre","pag_llibre","pag"],
        "map_to": [{ "target": "base.num_pagina_text", "transform": [{ "op": "trim" }] }]
      },

      { "header": "Pàgina digital", "key": "pagina_digital", "required": false, "aliases": ["paginareal","pagina_real","pag_digital"],
        "map_to": [{ "target": "attr.pagina_digital.text", "transform": [{ "op": "trim" }] }]
      },

      { "header": "Any", "key": "any", "required": false, "aliases": ["anydoc","anno","año","year"],
        "map_to": [{ "target": "base.any_doc.int_nullable", "transform": [{ "op": "parse_int_nullable" }] }]
      },

      { "header": "Cognoms", "key": "cognoms", "required": true, "aliases": ["batejat","infant","persona","nomcomplet"],
        "map_to": [{ "target": "person.batejat", "transform": [{ "op": "parse_person_from_cognoms_marcmoia_v2" }] }]
      },

      { "header": "Pare", "key": "pare", "required": false, "aliases": ["pare_nom","nom_pare"],
        "map_to": [{ "target": "person.pare", "transform": [{ "op": "parse_person_from_nom_marcmoia_v2" }] }]
      },

      { "header": "Mare", "key": "mare", "required": false, "aliases": ["mare_nom","nom_mare"],
        "map_to": [{ "target": "person.mare", "transform": [{ "op": "parse_person_from_nom_marcmoia_v2" }] }]
      },

      { "header": "Avis paterns", "key": "avis_paterns", "required": false, "aliases": ["avispaterns","avis_pare"],
        "map_to": [
          { "target": "person.avi_patern",  "transform": [{ "op": "split_couple_i", "args": { "select": "left" } },  { "op": "parse_person_from_nom_marcmoia_v2" }] },
          { "target": "person.avia_paterna","transform": [{ "op": "split_couple_i", "args": { "select": "right" } }, { "op": "parse_person_from_nom_marcmoia_v2" }] }
        ]
      },

      { "header": "Avis materns", "key": "avis_materns", "required": false, "aliases": ["avismaterns","avis_mare"],
        "map_to": [
          { "target": "person.avi_matern",  "transform": [{ "op": "split_couple_i", "args": { "select": "left" } },  { "op": "parse_person_from_nom_marcmoia_v2" }] },
          { "target": "person.avia_materna","transform": [{ "op": "split_couple_i", "args": { "select": "right" } }, { "op": "parse_person_from_nom_marcmoia_v2" }] }
        ]
      },

      { "header": "Bateig", "key": "bateig", "required": false, "aliases": ["data_bateig","data_acte","acte"],
        "map_to": [
          { "target": "base.data_acte_iso_text_estat", "transform": [{ "op": "parse_date_flexible_to_base_data_acte" }] },
          { "target": "attr.data_bateig.date_or_text_with_quality", "transform": [{ "op": "parse_date_flexible_to_date_or_text_with_quality" }] }
        ]
      },

      { "header": "Nascut", "key": "nascut", "required": false, "aliases": ["naixement","data_naixement"],
        "map_to": [{ "target": "attr.data_naixement.date_or_text_with_quality", "transform": [{ "op": "parse_date_flexible_to_date_or_text_with_quality" }] }]
      },

      { "header": "Defunció", "key": "defuncio", "required": false, "aliases": ["mort","data_defuncio"],
        "map_to": [{ "target": "attr.data_defuncio.date_or_text_with_quality", "transform": [{ "op": "parse_date_flexible_to_date_or_text_with_quality" }] }]
      },

      { "header": "Ofici", "key": "ofici", "required": false, "aliases": ["ocupacio","professio"],
        "map_to": [
          { "target": "person.pare.ofici_text_with_quality", "condition": { "op": "not_empty", "args": { "column": "pare" } },
            "transform": [{ "op": "trim" }, { "op": "default_quality_if_present" }] }
        ]
      },

      { "header": "Casat", "key": "casat", "required": false, "aliases": ["estat_civil","casat_amb","casada_amb"],
        "map_to": [
          { "target": "attr.casat.text", "transform": [{ "op": "trim" }] },
          { "target": "attr.matrimoni_ordre.int_nullable", "transform": [{ "op": "parse_marriage_order_int_nullable" }] }
        ]
      }
    ]
  },
  "policies": {
    "dedup": {
      "within_file": true,
      "key_strategy": "hash_raw_inputs_like_marcmoia",
      "key_columns": ["llibre","pagina_llibre","pagina_digital","any","cognoms","pare","mare","avis_paterns","avis_materns","casat","nascut","bateig","ofici","defuncio"],
      "if_principal_name_missing_add_row_index": true
    },
    "merge_existing": {
      "mode": "by_principal_person_if_book_indexed",
      "principal_roles": ["batejat","persona_principal"],
      "update_missing_only": true,
      "add_missing_people": true,
      "add_missing_attrs": true,
      "avoid_duplicate_rows_by_principal_name_per_book": true
    }
  }
}`,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
