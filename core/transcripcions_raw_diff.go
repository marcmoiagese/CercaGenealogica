package core

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type fieldDiff struct {
	Before  string
	After   string
	Changed bool
}

type snapshotVersion struct {
	Snap    *transcripcioSnapshot
	Version int
	Label   string
}

type registreRawDiff struct {
	Tipus         fieldDiff
	Any           fieldDiff
	Pagina        fieldDiff
	Posicio       fieldDiff
	DataText      fieldDiff
	DataISO       fieldDiff
	DataQualitat  fieldDiff
	Transcripcio  fieldDiff
	NotesMarginal fieldDiff
	NotesPaleo    fieldDiff
}

type registrePersonDiffRow struct {
	Rol        fieldDiff
	Nom        fieldDiff
	Cognom1    fieldDiff
	Cognom2    fieldDiff
	Sexe       fieldDiff
	Edat       fieldDiff
	EstatCivil fieldDiff
	Municipi   fieldDiff
	Ofici      fieldDiff
	Casa       fieldDiff
	Notes      fieldDiff
}

type registreAttrDiffRow struct {
	Clau  fieldDiff
	Tipus fieldDiff
	Valor fieldDiff
	Estat fieldDiff
	Notes fieldDiff
}

func makeFieldDiff(before, after string) fieldDiff {
	return fieldDiff{
		Before:  strings.TrimSpace(before),
		After:   strings.TrimSpace(after),
		Changed: strings.TrimSpace(before) != strings.TrimSpace(after),
	}
}

func formatDiffLine(value string, version int) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if version > 0 {
		return fmt.Sprintf("%s||v:%s", value, strconv.Itoa(version))
	}
	return value
}

func makeMultiFieldDiff(beforeLines, afterLines []string) fieldDiff {
	before := strings.TrimSpace(strings.Join(beforeLines, "\n"))
	after := strings.TrimSpace(strings.Join(afterLines, "\n"))
	if before == "" && after == "" {
		return fieldDiff{}
	}
	return fieldDiff{
		Before:  before,
		After:   after,
		Changed: true,
	}
}

func tagFieldDiffVersion(fd fieldDiff, version int) fieldDiff {
	if !fd.Changed {
		return fd
	}
	before := formatDiffLine(fd.Before, version)
	after := formatDiffLine(fd.After, version)
	return fieldDiff{
		Before:  before,
		After:   after,
		Changed: true,
	}
}

func hasAnyDiff(row registrePersonDiffRow) bool {
	fields := []fieldDiff{
		row.Rol, row.Nom, row.Cognom1, row.Cognom2, row.Sexe, row.Edat,
		row.EstatCivil, row.Municipi, row.Ofici, row.Casa, row.Notes,
	}
	for _, f := range fields {
		if strings.TrimSpace(f.Before) != "" || strings.TrimSpace(f.After) != "" {
			return true
		}
	}
	return false
}

func displayQualityValue(value, quality, lang string) string {
	value = strings.TrimSpace(value)
	quality = strings.TrimSpace(quality)
	if quality != "" {
		quality = T(lang, "records.quality."+quality)
	}
	if value == "" {
		return quality
	}
	if quality == "" {
		return value
	}
	return fmt.Sprintf("%s (%s)", value, quality)
}

func displayRegistreType(lang string, reg *db.TranscripcioRaw) string {
	if reg == nil || strings.TrimSpace(reg.TipusActe) == "" {
		return ""
	}
	return T(lang, "records.type."+reg.TipusActe)
}

func displayRegistreYear(reg *db.TranscripcioRaw) string {
	if reg == nil || !reg.AnyDoc.Valid {
		return ""
	}
	return fmt.Sprintf("%d", reg.AnyDoc.Int64)
}

func displayRegistrePage(reg *db.TranscripcioRaw) string {
	if reg == nil {
		return ""
	}
	if reg.PaginaID.Valid {
		return fmt.Sprintf("%d", reg.PaginaID.Int64)
	}
	if strings.TrimSpace(reg.NumPaginaText) != "" {
		return reg.NumPaginaText
	}
	return ""
}

func displayRegistrePosicio(reg *db.TranscripcioRaw) string {
	if reg == nil || !reg.PosicioPagina.Valid {
		return ""
	}
	return fmt.Sprintf("%d", reg.PosicioPagina.Int64)
}

func displayRegistreDataISO(reg *db.TranscripcioRaw) string {
	if reg == nil || !reg.DataActeISO.Valid {
		return ""
	}
	return reg.DataActeISO.String
}

func displayRegistreDataQualitat(lang string, reg *db.TranscripcioRaw) string {
	if reg == nil || strings.TrimSpace(reg.DataActeEstat) == "" {
		return ""
	}
	return T(lang, "records.quality."+reg.DataActeEstat)
}

func buildRegistreRawDiff(lang string, before, after *db.TranscripcioRaw) registreRawDiff {
	return registreRawDiff{
		Tipus:         makeFieldDiff(displayRegistreType(lang, before), displayRegistreType(lang, after)),
		Any:           makeFieldDiff(displayRegistreYear(before), displayRegistreYear(after)),
		Pagina:        makeFieldDiff(displayRegistrePage(before), displayRegistrePage(after)),
		Posicio:       makeFieldDiff(displayRegistrePosicio(before), displayRegistrePosicio(after)),
		DataText:      makeFieldDiff(valueOrEmpty(before, func(r *db.TranscripcioRaw) string { return r.DataActeText }), valueOrEmpty(after, func(r *db.TranscripcioRaw) string { return r.DataActeText })),
		DataISO:       makeFieldDiff(displayRegistreDataISO(before), displayRegistreDataISO(after)),
		DataQualitat:  makeFieldDiff(displayRegistreDataQualitat(lang, before), displayRegistreDataQualitat(lang, after)),
		Transcripcio:  makeFieldDiff(valueOrEmpty(before, func(r *db.TranscripcioRaw) string { return r.TranscripcioLiteral }), valueOrEmpty(after, func(r *db.TranscripcioRaw) string { return r.TranscripcioLiteral })),
		NotesMarginal: makeFieldDiff(valueOrEmpty(before, func(r *db.TranscripcioRaw) string { return r.NotesMarginals }), valueOrEmpty(after, func(r *db.TranscripcioRaw) string { return r.NotesMarginals })),
		NotesPaleo:    makeFieldDiff(valueOrEmpty(before, func(r *db.TranscripcioRaw) string { return r.ObservacionsPaleografiques }), valueOrEmpty(after, func(r *db.TranscripcioRaw) string { return r.ObservacionsPaleografiques })),
	}
}

func buildRegistreRawDiffMulti(lang string, entries []snapshotVersion) registreRawDiff {
	type acc struct {
		before []string
		after  []string
	}
	accs := map[string]*acc{
		"Tipus":         {},
		"Any":           {},
		"Pagina":        {},
		"Posicio":       {},
		"DataText":      {},
		"DataISO":       {},
		"DataQualitat":  {},
		"Transcripcio":  {},
		"NotesMarginal": {},
		"NotesPaleo":    {},
	}
	for i := 1; i < len(entries); i++ {
		prev := entries[i-1].Snap
		next := entries[i].Snap
		version := entries[i].Version
		if prev == nil || next == nil {
			continue
		}
		add := func(key, beforeVal, afterVal string) {
			if strings.TrimSpace(beforeVal) == strings.TrimSpace(afterVal) {
				return
			}
			if beforeVal != "" {
				accs[key].before = append(accs[key].before, formatDiffLine(beforeVal, version))
			}
			if afterVal != "" {
				accs[key].after = append(accs[key].after, formatDiffLine(afterVal, version))
			}
		}
		add("Tipus", displayRegistreType(lang, &prev.Raw), displayRegistreType(lang, &next.Raw))
		add("Any", displayRegistreYear(&prev.Raw), displayRegistreYear(&next.Raw))
		add("Pagina", displayRegistrePage(&prev.Raw), displayRegistrePage(&next.Raw))
		add("Posicio", displayRegistrePosicio(&prev.Raw), displayRegistrePosicio(&next.Raw))
		add("DataText", valueOrEmpty(&prev.Raw, func(r *db.TranscripcioRaw) string { return r.DataActeText }), valueOrEmpty(&next.Raw, func(r *db.TranscripcioRaw) string { return r.DataActeText }))
		add("DataISO", displayRegistreDataISO(&prev.Raw), displayRegistreDataISO(&next.Raw))
		add("DataQualitat", displayRegistreDataQualitat(lang, &prev.Raw), displayRegistreDataQualitat(lang, &next.Raw))
		add("Transcripcio", valueOrEmpty(&prev.Raw, func(r *db.TranscripcioRaw) string { return r.TranscripcioLiteral }), valueOrEmpty(&next.Raw, func(r *db.TranscripcioRaw) string { return r.TranscripcioLiteral }))
		add("NotesMarginal", valueOrEmpty(&prev.Raw, func(r *db.TranscripcioRaw) string { return r.NotesMarginals }), valueOrEmpty(&next.Raw, func(r *db.TranscripcioRaw) string { return r.NotesMarginals }))
		add("NotesPaleo", valueOrEmpty(&prev.Raw, func(r *db.TranscripcioRaw) string { return r.ObservacionsPaleografiques }), valueOrEmpty(&next.Raw, func(r *db.TranscripcioRaw) string { return r.ObservacionsPaleografiques }))
	}
	return registreRawDiff{
		Tipus:         makeMultiFieldDiff(accs["Tipus"].before, accs["Tipus"].after),
		Any:           makeMultiFieldDiff(accs["Any"].before, accs["Any"].after),
		Pagina:        makeMultiFieldDiff(accs["Pagina"].before, accs["Pagina"].after),
		Posicio:       makeMultiFieldDiff(accs["Posicio"].before, accs["Posicio"].after),
		DataText:      makeMultiFieldDiff(accs["DataText"].before, accs["DataText"].after),
		DataISO:       makeMultiFieldDiff(accs["DataISO"].before, accs["DataISO"].after),
		DataQualitat:  makeMultiFieldDiff(accs["DataQualitat"].before, accs["DataQualitat"].after),
		Transcripcio:  makeMultiFieldDiff(accs["Transcripcio"].before, accs["Transcripcio"].after),
		NotesMarginal: makeMultiFieldDiff(accs["NotesMarginal"].before, accs["NotesMarginal"].after),
		NotesPaleo:    makeMultiFieldDiff(accs["NotesPaleo"].before, accs["NotesPaleo"].after),
	}
}

func buildRegistrePersonDiffRowsMulti(lang string, entries []snapshotVersion) []registrePersonDiffRow {
	var rows []registrePersonDiffRow
	for i := 1; i < len(entries); i++ {
		prev := entries[i-1].Snap
		next := entries[i].Snap
		if prev == nil || next == nil {
			continue
		}
		version := entries[i].Version
		pairRows := buildRegistrePersonDiffRows(lang, prev.Persones, next.Persones)
		for _, row := range pairRows {
			row.Rol = tagFieldDiffVersion(row.Rol, version)
			row.Nom = tagFieldDiffVersion(row.Nom, version)
			row.Cognom1 = tagFieldDiffVersion(row.Cognom1, version)
			row.Cognom2 = tagFieldDiffVersion(row.Cognom2, version)
			row.Sexe = tagFieldDiffVersion(row.Sexe, version)
			row.Edat = tagFieldDiffVersion(row.Edat, version)
			row.EstatCivil = tagFieldDiffVersion(row.EstatCivil, version)
			row.Municipi = tagFieldDiffVersion(row.Municipi, version)
			row.Ofici = tagFieldDiffVersion(row.Ofici, version)
			row.Casa = tagFieldDiffVersion(row.Casa, version)
			row.Notes = tagFieldDiffVersion(row.Notes, version)
			rows = append(rows, row)
		}
	}
	return rows
}

func buildRegistreAttrDiffRowsMulti(lang string, entries []snapshotVersion) []registreAttrDiffRow {
	var rows []registreAttrDiffRow
	for i := 1; i < len(entries); i++ {
		prev := entries[i-1].Snap
		next := entries[i].Snap
		if prev == nil || next == nil {
			continue
		}
		version := entries[i].Version
		pairRows := buildRegistreAttrDiffRows(lang, prev.Atributs, next.Atributs)
		for _, row := range pairRows {
			row.Clau = tagFieldDiffVersion(row.Clau, version)
			row.Tipus = tagFieldDiffVersion(row.Tipus, version)
			row.Valor = tagFieldDiffVersion(row.Valor, version)
			row.Estat = tagFieldDiffVersion(row.Estat, version)
			row.Notes = tagFieldDiffVersion(row.Notes, version)
			rows = append(rows, row)
		}
	}
	return rows
}

func valueOrEmpty(reg *db.TranscripcioRaw, get func(*db.TranscripcioRaw) string) string {
	if reg == nil {
		return ""
	}
	return strings.TrimSpace(get(reg))
}

func buildRegistrePersonDiffRows(lang string, before, after []db.TranscripcioPersonaRaw) []registrePersonDiffRow {
	maxLen := len(before)
	if len(after) > maxLen {
		maxLen = len(after)
	}
	rows := make([]registrePersonDiffRow, 0, maxLen)
	for i := 0; i < maxLen; i++ {
		var b *db.TranscripcioPersonaRaw
		var a *db.TranscripcioPersonaRaw
		if i < len(before) {
			b = &before[i]
		}
		if i < len(after) {
			a = &after[i]
		}
		row := registrePersonDiffRow{
			Rol:        makeFieldDiff(personFieldValue(b, "rol"), personFieldValue(a, "rol")),
			Nom:        makeFieldDiff(displayQualityValue(personFieldValue(b, "nom"), personFieldValue(b, "nom_estat"), lang), displayQualityValue(personFieldValue(a, "nom"), personFieldValue(a, "nom_estat"), lang)),
			Cognom1:    makeFieldDiff(displayQualityValue(personFieldValue(b, "cognom1"), personFieldValue(b, "cognom1_estat"), lang), displayQualityValue(personFieldValue(a, "cognom1"), personFieldValue(a, "cognom1_estat"), lang)),
			Cognom2:    makeFieldDiff(displayQualityValue(personFieldValue(b, "cognom2"), personFieldValue(b, "cognom2_estat"), lang), displayQualityValue(personFieldValue(a, "cognom2"), personFieldValue(a, "cognom2_estat"), lang)),
			Sexe:       makeFieldDiff(displayQualityValue(personFieldValue(b, "sexe"), personFieldValue(b, "sexe_estat"), lang), displayQualityValue(personFieldValue(a, "sexe"), personFieldValue(a, "sexe_estat"), lang)),
			Edat:       makeFieldDiff(displayQualityValue(personFieldValue(b, "edat"), personFieldValue(b, "edat_estat"), lang), displayQualityValue(personFieldValue(a, "edat"), personFieldValue(a, "edat_estat"), lang)),
			EstatCivil: makeFieldDiff(displayQualityValue(personFieldValue(b, "estat_civil"), personFieldValue(b, "estat_civil_estat"), lang), displayQualityValue(personFieldValue(a, "estat_civil"), personFieldValue(a, "estat_civil_estat"), lang)),
			Municipi:   makeFieldDiff(displayQualityValue(personFieldValue(b, "municipi"), personFieldValue(b, "municipi_estat"), lang), displayQualityValue(personFieldValue(a, "municipi"), personFieldValue(a, "municipi_estat"), lang)),
			Ofici:      makeFieldDiff(displayQualityValue(personFieldValue(b, "ofici"), personFieldValue(b, "ofici_estat"), lang), displayQualityValue(personFieldValue(a, "ofici"), personFieldValue(a, "ofici_estat"), lang)),
			Casa:       makeFieldDiff(displayQualityValue(personFieldValue(b, "casa"), personFieldValue(b, "casa_estat"), lang), displayQualityValue(personFieldValue(a, "casa"), personFieldValue(a, "casa_estat"), lang)),
			Notes:      makeFieldDiff(personFieldValue(b, "notes"), personFieldValue(a, "notes")),
		}
		if hasAnyDiff(row) {
			rows = append(rows, row)
		}
	}
	return rows
}

func buildRegistreAttrDiffRows(lang string, before, after []db.TranscripcioAtributRaw) []registreAttrDiffRow {
	keys := []string{}
	seen := map[string]bool{}
	for _, attr := range before {
		key := strings.TrimSpace(attr.Clau)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		keys = append(keys, key)
	}
	for _, attr := range after {
		key := strings.TrimSpace(attr.Clau)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil
	}
	beforeMap := map[string]db.TranscripcioAtributRaw{}
	for _, attr := range before {
		if strings.TrimSpace(attr.Clau) == "" {
			continue
		}
		beforeMap[attr.Clau] = attr
	}
	afterMap := map[string]db.TranscripcioAtributRaw{}
	for _, attr := range after {
		if strings.TrimSpace(attr.Clau) == "" {
			continue
		}
		afterMap[attr.Clau] = attr
	}
	rows := make([]registreAttrDiffRow, 0, len(keys))
	for _, key := range keys {
		beforeAttr, beforeOK := beforeMap[key]
		afterAttr, afterOK := afterMap[key]
		var beforeVal string
		var afterVal string
		var beforeType string
		var afterType string
		var beforeState string
		var afterState string
		var beforeNotes string
		var afterNotes string
		if beforeOK {
			beforeVal = attrValueString(beforeAttr, lang)
			beforeType = beforeAttr.TipusValor
			beforeState = displayQualityValue("", beforeAttr.Estat, lang)
			beforeNotes = beforeAttr.Notes
		}
		if afterOK {
			afterVal = attrValueString(afterAttr, lang)
			afterType = afterAttr.TipusValor
			afterState = displayQualityValue("", afterAttr.Estat, lang)
			afterNotes = afterAttr.Notes
		}
		row := registreAttrDiffRow{
			Clau:  makeFieldDiff(key, key),
			Tipus: makeFieldDiff(beforeType, afterType),
			Valor: makeFieldDiff(beforeVal, afterVal),
			Estat: makeFieldDiff(beforeState, afterState),
			Notes: makeFieldDiff(beforeNotes, afterNotes),
		}
		rows = append(rows, row)
	}
	return rows
}
