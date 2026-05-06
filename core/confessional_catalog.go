package core

import (
	"database/sql"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type confessionalReligionSeed struct {
	Code     string
	Name     string
	Parent   string
	Category string
}

type confessionalLevelSeed struct {
	Code      string
	Name      string
	Religion  string
	Category  string
	Order     int
	Territory bool
	Children  bool
	Municipi  bool
	Imports   bool
}

func (a *App) EnsureSystemConfessionalCatalogs() error {
	religionIDs, err := a.ensureSystemReligioConfessions()
	if err != nil {
		return err
	}
	return a.ensureSystemNivellsConfessionals(religionIDs)
}

func (a *App) ensureSystemReligioConfessions() (map[string]int, error) {
	existing, err := a.DB.ListReligioConfessions()
	if err != nil {
		return nil, err
	}
	ids := map[string]int{}
	for _, item := range existing {
		if item.Codi != "" {
			ids[item.Codi] = item.ID
		}
	}
	for _, seed := range confessionalReligionSeeds() {
		if id := ids[seed.Code]; id > 0 {
			continue
		}
		parentID := sql.NullInt64{}
		if seed.Parent != "" {
			if id := ids[seed.Parent]; id > 0 {
				parentID = sql.NullInt64{Int64: int64(id), Valid: true}
			}
		}
		item := &db.ReligioConfessio{
			Codi:           seed.Code,
			Nom:            seed.Name,
			PareID:         parentID,
			Categoria:      seed.Category,
			SystemKey:      "confessional.religio." + seed.Code,
			SystemManaged:  true,
			Estat:          "actiu",
			ModeracioEstat: "publicat",
		}
		id, err := a.DB.SaveReligioConfessio(item)
		if err != nil {
			return nil, err
		}
		ids[seed.Code] = id
	}
	return ids, nil
}

func (a *App) ensureSystemNivellsConfessionals(religionIDs map[string]int) error {
	existing, err := a.DB.ListNivellsConfessionals()
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, item := range existing {
		if item.Codi != "" {
			seen[item.Codi] = true
		}
	}
	for _, seed := range confessionalLevelSeeds() {
		if seen[seed.Code] {
			continue
		}
		religionID := sql.NullInt64{}
		if id := religionIDs[seed.Religion]; id > 0 {
			religionID = sql.NullInt64{Int64: int64(id), Valid: true}
		}
		item := &db.NivellConfessional{
			ReligioConfessioID:  religionID,
			Codi:                seed.Code,
			Ordre:               seed.Order,
			NomNivell:           seed.Name,
			TipusNivell:         seed.Code,
			Categoria:           seed.Category,
			PotTenirTerritori:   seed.Territory,
			PotTenirFills:       seed.Children,
			PotVincularMunicipi: seed.Municipi,
			PotSuggerirImports:  seed.Imports,
			SystemKey:           "confessional.nivell." + seed.Code,
			SystemManaged:       true,
			Estat:               "actiu",
			ModeracioEstat:      "publicat",
		}
		if _, err := a.DB.SaveNivellConfessional(item); err != nil {
			return err
		}
	}
	return nil
}

func confessionalReligionSeeds() []confessionalReligionSeed {
	return []confessionalReligionSeed{
		{Code: "cristianisme", Name: "Cristianisme", Category: "religio"},
		{Code: "catolicisme", Name: "Catolicisme", Parent: "cristianisme", Category: "confessio"},
		{Code: "catolicisme_ritu_llati", Name: "Catolicisme - Ritu llati", Parent: "catolicisme", Category: "ritus"},
		{Code: "catolicisme_ritus_orientals", Name: "Catolicisme - Ritus orientals", Parent: "catolicisme", Category: "ritus"},
		{Code: "ortodoxia", Name: "Ortodoxia", Parent: "cristianisme", Category: "confessio"},
		{Code: "protestantisme", Name: "Protestantisme", Parent: "cristianisme", Category: "confessio"},
		{Code: "luteranisme", Name: "Luteranisme", Parent: "protestantisme", Category: "confessio"},
		{Code: "calvinisme_reformats", Name: "Calvinisme / Reformats", Parent: "protestantisme", Category: "confessio"},
		{Code: "anglicanisme", Name: "Anglicanisme", Parent: "protestantisme", Category: "confessio"},
		{Code: "metodisme", Name: "Metodisme", Parent: "protestantisme", Category: "confessio"},
		{Code: "baptisme", Name: "Baptisme", Parent: "protestantisme", Category: "confessio"},
		{Code: "islam", Name: "Islam", Category: "religio"},
		{Code: "sunnisme", Name: "Sunnisme", Parent: "islam", Category: "branca"},
		{Code: "xiisme", Name: "Xiisme", Parent: "islam", Category: "branca"},
		{Code: "judaisme", Name: "Judaisme", Category: "religio"},
		{Code: "hinduisme", Name: "Hinduisme", Category: "religio"},
		{Code: "budisme", Name: "Budisme", Category: "religio"},
		{Code: "theravada", Name: "Theravada", Parent: "budisme", Category: "tradicio"},
		{Code: "mahayana", Name: "Mahayana", Parent: "budisme", Category: "tradicio"},
		{Code: "vajrayana_budisme_tibeta", Name: "Vajrayana / Budisme tibeta", Parent: "budisme", Category: "tradicio"},
		{Code: "taoisme", Name: "Taoisme", Category: "religio"},
		{Code: "confucianisme", Name: "Confucianisme", Category: "religio"},
		{Code: "sintoisme", Name: "Sintoisme", Category: "religio"},
		{Code: "zoroastrisme", Name: "Zoroastrisme", Category: "religio"},
		{Code: "jainisme", Name: "Jainisme", Category: "religio"},
		{Code: "sikhisme", Name: "Sikhisme", Category: "religio"},
		{Code: "fe_bahai", Name: "Fe Bahai", Category: "religio"},
		{Code: "religions_tradicionals_africanes", Name: "Religions tradicionals africanes", Category: "religio"},
		{Code: "neopaganisme_modern", Name: "Neopaganisme modern", Category: "religio"},
	}
}

func confessionalLevelSeeds() []confessionalLevelSeed {
	seeds := []confessionalLevelSeed{
		{Code: "santa_seu", Name: "Santa Seu", Religion: "catolicisme_ritu_llati", Category: "govern_universal", Order: 1, Territory: true, Children: true, Municipi: false, Imports: true},
		{Code: "provincia_eclesiastica", Name: "Provincia eclesiastica", Religion: "catolicisme_ritu_llati", Category: "territorial_major", Order: 2, Territory: true, Children: true, Municipi: false, Imports: true},
		{Code: "arquebisbat_arxidiocesi", Name: "Arquebisbat / Arxidiocesi", Religion: "catolicisme_ritu_llati", Category: "territorial_major", Order: 3, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "bisbat_diocesi", Name: "Bisbat / Diocesi", Religion: "catolicisme_ritu_llati", Category: "territorial_major", Order: 4, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "prelatura_territorial", Name: "Prelatura territorial", Religion: "catolicisme_ritu_llati", Category: "territorial_major", Order: 5, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "abadia_territorial", Name: "Abadia territorial", Religion: "catolicisme_ritu_llati", Category: "territorial_major", Order: 6, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "vicariat_apostolic", Name: "Vicariat apostolic", Religion: "catolicisme_ritu_llati", Category: "territorial_major", Order: 7, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "vicariat_territorial_zona_pastoral", Name: "Vicariat territorial / Zona pastoral", Religion: "catolicisme_ritu_llati", Category: "territorial_intermedi", Order: 8, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "arxiprestat_vicariat_forani", Name: "Arxiprestat / Vicariat forani", Religion: "catolicisme_ritu_llati", Category: "territorial_intermedi", Order: 9, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "unitat_pastoral", Name: "Unitat pastoral", Religion: "catolicisme_ritu_llati", Category: "unitat_pastoral", Order: 10, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "parroquia", Name: "Parroquia", Religion: "catolicisme_ritu_llati", Category: "territorial_local", Order: 11, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "esglesia_filial", Name: "Esglesia filial", Religion: "catolicisme_ritu_llati", Category: "lloc_de_culte", Order: 12, Territory: true, Children: false, Municipi: true, Imports: true},
		{Code: "lloc_de_culte", Name: "Lloc de culte", Religion: "catolicisme_ritu_llati", Category: "lloc_de_culte", Order: 13, Territory: true, Children: false, Municipi: true, Imports: true},
		{Code: "capella_ermita_santuari", Name: "Capella / Ermita / Santuari", Religion: "catolicisme_ritu_llati", Category: "lloc_de_culte", Order: 14, Territory: true, Children: false, Municipi: true, Imports: true},
		{Code: "monestir_comunitat_religiosa", Name: "Monestir / Comunitat religiosa", Religion: "catolicisme_ritu_llati", Category: "comunitat_religiosa", Order: 15, Territory: true, Children: true, Municipi: true, Imports: true},
	}
	seeds = append(seeds, []confessionalLevelSeed{
		{Code: "ortodoxia_autocefalia", Name: "Autocefalia", Religion: "ortodoxia", Category: "govern_universal", Order: 1, Territory: true, Children: true, Municipi: false, Imports: true},
		{Code: "ortodoxia_patriarcat", Name: "Patriarcat", Religion: "ortodoxia", Category: "territorial_major", Order: 2, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "ortodoxia_exarcat", Name: "Exarcat", Religion: "ortodoxia", Category: "territorial_major", Order: 3, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "ortodoxia_metropolis", Name: "Metropolis", Religion: "ortodoxia", Category: "territorial_major", Order: 4, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "ortodoxia_eparquia", Name: "Eparquia", Religion: "ortodoxia", Category: "territorial_major", Order: 5, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "ortodoxia_parroquia", Name: "Parroquia", Religion: "ortodoxia", Category: "territorial_local", Order: 6, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "anglicanisme_provincia", Name: "Provincia", Religion: "anglicanisme", Category: "territorial_major", Order: 1, Territory: true, Children: true, Municipi: false, Imports: true},
		{Code: "anglicanisme_diocesi", Name: "Diocesi", Religion: "anglicanisme", Category: "territorial_major", Order: 2, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "anglicanisme_arxidiaconat", Name: "Arxidiaconat", Religion: "anglicanisme", Category: "territorial_intermedi", Order: 3, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "anglicanisme_deganat", Name: "Deganat", Religion: "anglicanisme", Category: "territorial_intermedi", Order: 4, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "anglicanisme_parroquia", Name: "Parroquia", Religion: "anglicanisme", Category: "territorial_local", Order: 5, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "islam_califat", Name: "Califat", Religion: "islam", Category: "govern_universal", Order: 1, Territory: true, Children: true, Municipi: false, Imports: true},
		{Code: "islam_wilaya", Name: "Wilaya", Religion: "islam", Category: "territorial_major", Order: 2, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "islam_autoritat_religiosa_territorial", Name: "Autoritat religiosa territorial", Religion: "islam", Category: "territorial_intermedi", Order: 3, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "islam_comunitat_local_mesquita", Name: "Comunitat local / mesquita", Religion: "islam", Category: "lloc_de_culte", Order: 4, Territory: true, Children: false, Municipi: true, Imports: true},
		{Code: "judaisme_qahal_comunitat_jueva", Name: "Qahal / comunitat jueva", Religion: "judaisme", Category: "comunitat_religiosa", Order: 1, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "judaisme_rabinat_territorial", Name: "Rabinat territorial", Religion: "judaisme", Category: "territorial_intermedi", Order: 2, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "judaisme_sinagoga", Name: "Sinagoga", Religion: "judaisme", Category: "lloc_de_culte", Order: 3, Territory: true, Children: false, Municipi: true, Imports: true},
		{Code: "budisme_sangha_nacional", Name: "Sangha nacional", Religion: "budisme", Category: "coordinacio", Order: 1, Territory: true, Children: true, Municipi: false, Imports: true},
		{Code: "budisme_provincia_monastica", Name: "Provincia monastica", Religion: "budisme", Category: "territorial_major", Order: 2, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "budisme_monestir", Name: "Monestir", Religion: "budisme", Category: "comunitat_religiosa", Order: 3, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "budisme_temple_local", Name: "Temple local", Religion: "budisme", Category: "lloc_de_culte", Order: 4, Territory: true, Children: false, Municipi: true, Imports: true},
		{Code: "sintoisme_santuari_principal", Name: "Santuari principal", Religion: "sintoisme", Category: "territorial_major", Order: 1, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "sintoisme_santuari_prefectural", Name: "Santuari prefectural", Religion: "sintoisme", Category: "territorial_intermedi", Order: 2, Territory: true, Children: true, Municipi: true, Imports: true},
		{Code: "sintoisme_santuari_local", Name: "Santuari local", Religion: "sintoisme", Category: "lloc_de_culte", Order: 3, Territory: true, Children: false, Municipi: true, Imports: true},
		{Code: "bahai_casa_universal_justicia", Name: "Casa Universal de Justicia", Religion: "fe_bahai", Category: "govern_universal", Order: 1, Territory: false, Children: true, Municipi: false, Imports: true},
		{Code: "bahai_assemblea_espiritual_nacional", Name: "Assemblea Espiritual Nacional", Religion: "fe_bahai", Category: "coordinacio", Order: 2, Territory: true, Children: true, Municipi: false, Imports: true},
		{Code: "bahai_assemblea_espiritual_local", Name: "Assemblea Espiritual Local", Religion: "fe_bahai", Category: "territorial_local", Order: 3, Territory: true, Children: false, Municipi: true, Imports: true},
	}...)
	return seeds
}
