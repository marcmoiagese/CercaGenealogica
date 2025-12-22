package core

import (
	"math"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type LlibreIndexacioView struct {
	TotalRegistres int
	Percentatge    int
	ColorClass     string
}

func (a *App) buildLlibresIndexacioViews(llibres []db.LlibreRow) map[string]LlibreIndexacioView {
	res := map[string]LlibreIndexacioView{}
	if len(llibres) == 0 {
		return res
	}
	ids := make([]int, 0, len(llibres))
	for _, llibre := range llibres {
		ids = append(ids, llibre.ID)
	}
	stats, err := a.DB.GetLlibresIndexacioStats(ids)
	if err != nil {
		stats = map[int]db.LlibreIndexacioStats{}
	}
	for _, llibre := range llibres {
		stat := stats[llibre.ID]
		view := LlibreIndexacioView{
			TotalRegistres: stat.TotalRegistres,
			Percentatge:    stat.Percentatge,
			ColorClass:     indexacioColorClass(stat.Percentatge),
		}
		res[strconv.Itoa(llibre.ID)] = view
	}
	return res
}

func indexacioColorClass(percent int) string {
	switch {
	case percent >= 80:
		return "verd"
	case percent >= 60:
		return "groc"
	case percent >= 30:
		return "taronja"
	default:
		return "rosa"
	}
}

func (a *App) recalcLlibreIndexacioStats(llibreID int) (*db.LlibreIndexacioStats, error) {
	llibre, err := a.DB.GetLlibre(llibreID)
	if err != nil {
		return nil, err
	}
	if llibre == nil {
		return nil, nil
	}
	bookType := normalizeIndexerBookType(llibre.TipusLlibre)
	fields := indexerContentFields(indexerSchema(bookType))
	stats := &db.LlibreIndexacioStats{LlibreID: llibreID}
	registres, err := a.DB.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{Limit: -1})
	if err != nil {
		return nil, err
	}
	stats.TotalRegistres = len(registres)
	if stats.TotalRegistres == 0 || len(fields) == 0 {
		stats.TotalCamps = 0
		stats.CampsEmplenats = 0
		stats.Percentatge = 0
		return stats, a.DB.UpsertLlibreIndexacioStats(stats)
	}
	stats.TotalCamps = len(fields) * stats.TotalRegistres
	for _, registre := range registres {
		persones, _ := a.DB.ListTranscripcioPersones(registre.ID)
		atributs, _ := a.DB.ListTranscripcioAtributs(registre.ID)
		cache := map[string]*db.TranscripcioPersonaRaw{}
		for _, field := range fields {
			if indexerFieldValue(field, registre, persones, atributs, cache) != "" {
				stats.CampsEmplenats++
			}
		}
	}
	stats.Percentatge = int(math.Round(float64(stats.CampsEmplenats) * 100 / float64(stats.TotalCamps)))
	if stats.Percentatge < 0 {
		stats.Percentatge = 0
	}
	if stats.Percentatge > 100 {
		stats.Percentatge = 100
	}
	return stats, a.DB.UpsertLlibreIndexacioStats(stats)
}

func indexerContentFields(fields []indexerField) []indexerField {
	res := make([]indexerField, 0, len(fields))
	for _, field := range fields {
		if field.Key == "" || field.Target == "" {
			continue
		}
		if isIndexerQualityField(field) {
			continue
		}
		res = append(res, field)
	}
	return res
}

func isIndexerQualityField(field indexerField) bool {
	if field.Key == "qualitat_general" || field.RawField == "data_acte_estat" {
		return true
	}
	if field.Target == "person" && strings.HasSuffix(field.PersonField, "_estat") {
		return true
	}
	if field.Target == "raw" && strings.HasSuffix(field.RawField, "_estat") {
		return true
	}
	if field.Target == "attr" && (field.AttrType == "estat" || strings.HasSuffix(field.AttrKey, "_estat")) {
		return true
	}
	return false
}

func indexerFieldValue(field indexerField, raw db.TranscripcioRaw, persones []db.TranscripcioPersonaRaw, attrs []db.TranscripcioAtributRaw, cache map[string]*db.TranscripcioPersonaRaw) string {
	switch field.Target {
	case "raw":
		return strings.TrimSpace(rawFieldValue(raw, field.RawField))
	case "attr":
		return strings.TrimSpace(attrValueByKeysRaw(attrs, field.AttrKey))
	case "person":
		person := personForField(persones, field.Role, field.PersonKey, cache)
		return strings.TrimSpace(personFieldValue(person, field.PersonField))
	default:
		return ""
	}
}
