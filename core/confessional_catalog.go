package core

import "strings"

type ConfessionalReligionCatalogItem struct {
	Code          string
	CanonicalName string
	ParentCode    string
	CategoryCode  string
	Active        bool
	SystemManaged bool
	Order         int
}

type ConfessionalLevelCatalogItem struct {
	Code                 string
	CanonicalName        string
	ReligionCode         string
	CategoryCode         string
	Order                int
	ParentCode           string
	CanHaveTerritory     bool
	CanHaveChildren      bool
	CanLinkMunicipi      bool
	CanSuggestForImports bool
	Active               bool
	SystemManaged        bool
}

type ConfessionalCategoryCatalogItem struct {
	Code    string
	I18nKey string
}

func (a *App) EnsureSystemConfessionalCatalogs() error {
	// Legacy compatibility hook. F35-3V keeps the base catalog in code and
	// must not seed religions, confessions, models, or levels into the DB.
	return nil
}

func ListConfessionalReligionCatalog() []ConfessionalReligionCatalogItem {
	items := make([]ConfessionalReligionCatalogItem, len(confessionalReligionCatalog))
	copy(items, confessionalReligionCatalog)
	return items
}

func GetConfessionalReligionCatalogByCode(code string) (ConfessionalReligionCatalogItem, bool) {
	code = normalizeCatalogCode(code)
	for _, item := range confessionalReligionCatalog {
		if item.Code == code {
			return item, true
		}
	}
	return ConfessionalReligionCatalogItem{}, false
}

func ListConfessionalLevelCatalog() []ConfessionalLevelCatalogItem {
	items := make([]ConfessionalLevelCatalogItem, len(confessionalLevelCatalog))
	copy(items, confessionalLevelCatalog)
	return items
}

func ListConfessionalLevelsByReligionCode(code string) []ConfessionalLevelCatalogItem {
	code = normalizeCatalogCode(code)
	out := []ConfessionalLevelCatalogItem{}
	for _, item := range confessionalLevelCatalog {
		if item.ReligionCode == code {
			out = append(out, item)
		}
	}
	return out
}

func GetConfessionalLevelCatalogByCode(code string) (ConfessionalLevelCatalogItem, bool) {
	code = normalizeCatalogCode(code)
	for _, item := range confessionalLevelCatalog {
		if item.Code == code {
			return item, true
		}
	}
	return ConfessionalLevelCatalogItem{}, false
}

func ListConfessionalReligionCategories() []ConfessionalCategoryCatalogItem {
	return []ConfessionalCategoryCatalogItem{
		{Code: "religio", I18nKey: "confessional.religion.category.religio"},
		{Code: "branca", I18nKey: "confessional.religion.category.branca"},
		{Code: "confessio", I18nKey: "confessional.religion.category.confessio"},
		{Code: "ritus", I18nKey: "confessional.religion.category.ritus"},
		{Code: "tradicio", I18nKey: "confessional.religion.category.tradicio"},
	}
}

func ListConfessionalLevelCategories() []ConfessionalCategoryCatalogItem {
	return []ConfessionalCategoryCatalogItem{
		{Code: "govern_universal", I18nKey: "confessional.level.category.govern_universal"},
		{Code: "coordinacio", I18nKey: "confessional.level.category.coordinacio"},
		{Code: "territorial_major", I18nKey: "confessional.level.category.territorial_major"},
		{Code: "territorial_intermedi", I18nKey: "confessional.level.category.territorial_intermedi"},
		{Code: "territorial_local", I18nKey: "confessional.level.category.territorial_local"},
		{Code: "unitat_pastoral", I18nKey: "confessional.level.category.unitat_pastoral"},
		{Code: "lloc_de_culte", I18nKey: "confessional.level.category.lloc_de_culte"},
		{Code: "comunitat_religiosa", I18nKey: "confessional.level.category.comunitat_religiosa"},
		{Code: "pelegrinatge", I18nKey: "confessional.level.category.pelegrinatge"},
		{Code: "llinatge_comunitat", I18nKey: "confessional.level.category.llinatge_comunitat"},
		{Code: "no_territorial", I18nKey: "confessional.level.category.no_territorial"},
	}
}

func normalizeCatalogCode(code string) string {
	return strings.TrimSpace(strings.ToLower(code))
}

var confessionalReligionCatalog = []ConfessionalReligionCatalogItem{
	{Code: "cristianisme", CanonicalName: "Cristianisme", CategoryCode: "religio", Active: true, SystemManaged: true, Order: 1},
	{Code: "catolicisme", CanonicalName: "Catolicisme", ParentCode: "cristianisme", CategoryCode: "confessio", Active: true, SystemManaged: true, Order: 2},
	{Code: "catolicisme_ritu_llati", CanonicalName: "Catolicisme - Ritu llati", ParentCode: "catolicisme", CategoryCode: "ritus", Active: true, SystemManaged: true, Order: 3},
	{Code: "catolicisme_ritus_orientals", CanonicalName: "Catolicisme - Ritus orientals", ParentCode: "catolicisme", CategoryCode: "ritus", Active: true, SystemManaged: true, Order: 4},
	{Code: "ortodoxia", CanonicalName: "Ortodoxia", ParentCode: "cristianisme", CategoryCode: "confessio", Active: true, SystemManaged: true, Order: 5},
	{Code: "protestantisme", CanonicalName: "Protestantisme", ParentCode: "cristianisme", CategoryCode: "confessio", Active: true, SystemManaged: true, Order: 6},
	{Code: "luteranisme", CanonicalName: "Luteranisme", ParentCode: "protestantisme", CategoryCode: "confessio", Active: true, SystemManaged: true, Order: 7},
	{Code: "calvinisme_reformats", CanonicalName: "Calvinisme / Reformats", ParentCode: "protestantisme", CategoryCode: "confessio", Active: true, SystemManaged: true, Order: 8},
	{Code: "anglicanisme", CanonicalName: "Anglicanisme", ParentCode: "protestantisme", CategoryCode: "confessio", Active: true, SystemManaged: true, Order: 9},
	{Code: "metodisme", CanonicalName: "Metodisme", ParentCode: "protestantisme", CategoryCode: "confessio", Active: true, SystemManaged: true, Order: 10},
	{Code: "baptisme", CanonicalName: "Baptisme", ParentCode: "protestantisme", CategoryCode: "confessio", Active: true, SystemManaged: true, Order: 11},
	{Code: "islam", CanonicalName: "Islam", CategoryCode: "religio", Active: true, SystemManaged: true, Order: 12},
	{Code: "sunnisme", CanonicalName: "Sunnisme", ParentCode: "islam", CategoryCode: "branca", Active: true, SystemManaged: true, Order: 13},
	{Code: "xiisme", CanonicalName: "Xiisme", ParentCode: "islam", CategoryCode: "branca", Active: true, SystemManaged: true, Order: 14},
	{Code: "judaisme", CanonicalName: "Judaisme", CategoryCode: "religio", Active: true, SystemManaged: true, Order: 15},
	{Code: "hinduisme", CanonicalName: "Hinduisme", CategoryCode: "religio", Active: true, SystemManaged: true, Order: 16},
	{Code: "budisme", CanonicalName: "Budisme", CategoryCode: "religio", Active: true, SystemManaged: true, Order: 17},
	{Code: "theravada", CanonicalName: "Theravada", ParentCode: "budisme", CategoryCode: "tradicio", Active: true, SystemManaged: true, Order: 18},
	{Code: "mahayana", CanonicalName: "Mahayana", ParentCode: "budisme", CategoryCode: "tradicio", Active: true, SystemManaged: true, Order: 19},
	{Code: "vajrayana_budisme_tibeta", CanonicalName: "Vajrayana / Budisme tibeta", ParentCode: "budisme", CategoryCode: "tradicio", Active: true, SystemManaged: true, Order: 20},
	{Code: "taoisme", CanonicalName: "Taoisme", CategoryCode: "religio", Active: true, SystemManaged: true, Order: 21},
	{Code: "confucianisme", CanonicalName: "Confucianisme", CategoryCode: "religio", Active: true, SystemManaged: true, Order: 22},
	{Code: "sintoisme", CanonicalName: "Sintoisme", CategoryCode: "religio", Active: true, SystemManaged: true, Order: 23},
	{Code: "zoroastrisme", CanonicalName: "Zoroastrisme", CategoryCode: "religio", Active: true, SystemManaged: true, Order: 24},
	{Code: "jainisme", CanonicalName: "Jainisme", CategoryCode: "religio", Active: true, SystemManaged: true, Order: 25},
	{Code: "sikhisme", CanonicalName: "Sikhisme", CategoryCode: "religio", Active: true, SystemManaged: true, Order: 26},
	{Code: "fe_bahai", CanonicalName: "Fe Bahai", CategoryCode: "religio", Active: true, SystemManaged: true, Order: 27},
	{Code: "religions_tradicionals_africanes", CanonicalName: "Religions tradicionals africanes", CategoryCode: "religio", Active: true, SystemManaged: true, Order: 28},
	{Code: "neopaganisme_modern", CanonicalName: "Neopaganisme modern", CategoryCode: "religio", Active: true, SystemManaged: true, Order: 29},
}

var confessionalLevelCatalog = []ConfessionalLevelCatalogItem{
	{Code: "santa_seu", CanonicalName: "Santa Seu", ReligionCode: "catolicisme_ritu_llati", CategoryCode: "govern_universal", Order: 1, CanHaveTerritory: true, CanHaveChildren: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "provincia_eclesiastica", CanonicalName: "Provincia eclesiastica", ReligionCode: "catolicisme_ritu_llati", CategoryCode: "territorial_major", Order: 2, CanHaveTerritory: true, CanHaveChildren: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "arquebisbat_arxidiocesi", CanonicalName: "Arquebisbat / Arxidiocesi", ReligionCode: "catolicisme_ritu_llati", CategoryCode: "territorial_major", Order: 3, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "bisbat_diocesi", CanonicalName: "Bisbat / Diocesi", ReligionCode: "catolicisme_ritu_llati", CategoryCode: "territorial_major", Order: 4, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "prelatura_territorial", CanonicalName: "Prelatura territorial", ReligionCode: "catolicisme_ritu_llati", CategoryCode: "territorial_major", Order: 5, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "abadia_territorial", CanonicalName: "Abadia territorial", ReligionCode: "catolicisme_ritu_llati", CategoryCode: "territorial_major", Order: 6, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "vicariat_apostolic", CanonicalName: "Vicariat apostolic", ReligionCode: "catolicisme_ritu_llati", CategoryCode: "territorial_major", Order: 7, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "vicariat_territorial_zona_pastoral", CanonicalName: "Vicariat territorial / Zona pastoral", ReligionCode: "catolicisme_ritu_llati", CategoryCode: "territorial_intermedi", Order: 8, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "arxiprestat_vicariat_forani", CanonicalName: "Arxiprestat / Vicariat forani", ReligionCode: "catolicisme_ritu_llati", CategoryCode: "territorial_intermedi", Order: 9, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "unitat_pastoral", CanonicalName: "Unitat pastoral", ReligionCode: "catolicisme_ritu_llati", CategoryCode: "unitat_pastoral", Order: 10, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "parroquia", CanonicalName: "Parroquia", ReligionCode: "catolicisme_ritu_llati", CategoryCode: "territorial_local", Order: 11, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "esglesia_filial", CanonicalName: "Esglesia filial", ReligionCode: "catolicisme_ritu_llati", CategoryCode: "lloc_de_culte", Order: 12, CanHaveTerritory: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "lloc_de_culte", CanonicalName: "Lloc de culte", ReligionCode: "catolicisme_ritu_llati", CategoryCode: "lloc_de_culte", Order: 13, CanHaveTerritory: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "capella_ermita_santuari", CanonicalName: "Capella / Ermita / Santuari", ReligionCode: "catolicisme_ritu_llati", CategoryCode: "lloc_de_culte", Order: 14, CanHaveTerritory: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "monestir_comunitat_religiosa", CanonicalName: "Monestir / Comunitat religiosa", ReligionCode: "catolicisme_ritu_llati", CategoryCode: "comunitat_religiosa", Order: 15, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "ortodoxia_autocefalia", CanonicalName: "Autocefalia", ReligionCode: "ortodoxia", CategoryCode: "govern_universal", Order: 1, CanHaveTerritory: true, CanHaveChildren: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "ortodoxia_patriarcat", CanonicalName: "Patriarcat", ReligionCode: "ortodoxia", CategoryCode: "territorial_major", Order: 2, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "ortodoxia_exarcat", CanonicalName: "Exarcat", ReligionCode: "ortodoxia", CategoryCode: "territorial_major", Order: 3, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "ortodoxia_metropolis", CanonicalName: "Metropolis", ReligionCode: "ortodoxia", CategoryCode: "territorial_major", Order: 4, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "ortodoxia_eparquia", CanonicalName: "Eparquia", ReligionCode: "ortodoxia", CategoryCode: "territorial_major", Order: 5, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "ortodoxia_parroquia", CanonicalName: "Parroquia", ReligionCode: "ortodoxia", CategoryCode: "territorial_local", Order: 6, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "anglicanisme_provincia", CanonicalName: "Provincia", ReligionCode: "anglicanisme", CategoryCode: "territorial_major", Order: 1, CanHaveTerritory: true, CanHaveChildren: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "anglicanisme_diocesi", CanonicalName: "Diocesi", ReligionCode: "anglicanisme", CategoryCode: "territorial_major", Order: 2, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "anglicanisme_arxidiaconat", CanonicalName: "Arxidiaconat", ReligionCode: "anglicanisme", CategoryCode: "territorial_intermedi", Order: 3, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "anglicanisme_deganat", CanonicalName: "Deganat", ReligionCode: "anglicanisme", CategoryCode: "territorial_intermedi", Order: 4, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "anglicanisme_parroquia", CanonicalName: "Parroquia", ReligionCode: "anglicanisme", CategoryCode: "territorial_local", Order: 5, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "islam_califat", CanonicalName: "Califat", ReligionCode: "islam", CategoryCode: "govern_universal", Order: 1, CanHaveTerritory: true, CanHaveChildren: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "islam_wilaya", CanonicalName: "Wilaya", ReligionCode: "islam", CategoryCode: "territorial_major", Order: 2, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "islam_autoritat_religiosa_territorial", CanonicalName: "Autoritat religiosa territorial", ReligionCode: "islam", CategoryCode: "territorial_intermedi", Order: 3, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "islam_comunitat_local_mesquita", CanonicalName: "Comunitat local / mesquita", ReligionCode: "islam", CategoryCode: "lloc_de_culte", Order: 4, CanHaveTerritory: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "judaisme_qahal_comunitat_jueva", CanonicalName: "Qahal / comunitat jueva", ReligionCode: "judaisme", CategoryCode: "comunitat_religiosa", Order: 1, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "judaisme_rabinat_territorial", CanonicalName: "Rabinat territorial", ReligionCode: "judaisme", CategoryCode: "territorial_intermedi", Order: 2, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "judaisme_sinagoga", CanonicalName: "Sinagoga", ReligionCode: "judaisme", CategoryCode: "lloc_de_culte", Order: 3, CanHaveTerritory: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "budisme_sangha_nacional", CanonicalName: "Sangha nacional", ReligionCode: "budisme", CategoryCode: "coordinacio", Order: 1, CanHaveTerritory: true, CanHaveChildren: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "budisme_provincia_monastica", CanonicalName: "Provincia monastica", ReligionCode: "budisme", CategoryCode: "territorial_major", Order: 2, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "budisme_monestir", CanonicalName: "Monestir", ReligionCode: "budisme", CategoryCode: "comunitat_religiosa", Order: 3, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "budisme_temple_local", CanonicalName: "Temple local", ReligionCode: "budisme", CategoryCode: "lloc_de_culte", Order: 4, CanHaveTerritory: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "sintoisme_santuari_principal", CanonicalName: "Santuari principal", ReligionCode: "sintoisme", CategoryCode: "territorial_major", Order: 1, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "sintoisme_santuari_prefectural", CanonicalName: "Santuari prefectural", ReligionCode: "sintoisme", CategoryCode: "territorial_intermedi", Order: 2, CanHaveTerritory: true, CanHaveChildren: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "sintoisme_santuari_local", CanonicalName: "Santuari local", ReligionCode: "sintoisme", CategoryCode: "lloc_de_culte", Order: 3, CanHaveTerritory: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "bahai_casa_universal_justicia", CanonicalName: "Casa Universal de Justicia", ReligionCode: "fe_bahai", CategoryCode: "govern_universal", Order: 1, CanHaveChildren: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "bahai_assemblea_espiritual_nacional", CanonicalName: "Assemblea Espiritual Nacional", ReligionCode: "fe_bahai", CategoryCode: "coordinacio", Order: 2, CanHaveTerritory: true, CanHaveChildren: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
	{Code: "bahai_assemblea_espiritual_local", CanonicalName: "Assemblea Espiritual Local", ReligionCode: "fe_bahai", CategoryCode: "territorial_local", Order: 3, CanHaveTerritory: true, CanLinkMunicipi: true, CanSuggestForImports: true, Active: true, SystemManaged: true},
}
