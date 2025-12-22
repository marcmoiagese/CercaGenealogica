package core

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type csvImportResult struct {
	Created int
	Updated int
	Failed  int
	Errors  []importErrorEntry
	BookIDs map[int]struct{}
}

func (r *csvImportResult) markBook(id int) {
	if id <= 0 {
		return
	}
	if r.BookIDs == nil {
		r.BookIDs = map[int]struct{}{}
	}
	r.BookIDs[id] = struct{}{}
}

type importContext struct {
	MunicipiID int
	ArxiuID    int
}

func normalizeCSVHeader(raw string) string {
	raw = strings.TrimSpace(strings.TrimPrefix(raw, "\ufeff"))
	raw = strings.ToLower(raw)
	raw = stripDiacritics(raw)
	raw = strings.ReplaceAll(raw, " ", "")
	raw = strings.ReplaceAll(raw, "_", "")
	raw = strings.ReplaceAll(raw, "-", "")
	raw = strings.ReplaceAll(raw, ".", "")
	raw = strings.ReplaceAll(raw, ":", "")
	return raw
}

func stripDiacritics(val string) string {
	replacer := strings.NewReplacer(
		"à", "a", "á", "a", "â", "a", "ä", "a", "ã", "a", "å", "a",
		"è", "e", "é", "e", "ê", "e", "ë", "e",
		"ì", "i", "í", "i", "î", "i", "ï", "i",
		"ò", "o", "ó", "o", "ô", "o", "ö", "o", "õ", "o",
		"ù", "u", "ú", "u", "û", "u", "ü", "u",
		"ç", "c", "ñ", "n",
		"·", "",
	)
	return replacer.Replace(val)
}

func normalizeCronologia(val string) string {
	val = strings.TrimSpace(val)
	if val == "" {
		return ""
	}
	val = strings.ReplaceAll(val, ".", "/")
	val = strings.ReplaceAll(val, " ", "")
	return val
}

func splitParentheticals(val string) (string, []string) {
	val = strings.TrimSpace(val)
	var extras []string
	for {
		start := strings.Index(val, "(")
		end := strings.Index(val, ")")
		if start == -1 || end == -1 || end < start {
			break
		}
		extra := strings.TrimSpace(val[start+1 : end])
		if extra != "" {
			extras = append(extras, extra)
		}
		val = strings.TrimSpace(val[:start] + " " + val[end+1:])
	}
	val = strings.Join(strings.Fields(val), " ")
	return val, extras
}

func mergeQuality(values ...string) string {
	hasDubtos := false
	for _, v := range values {
		if v == "no_consta" {
			return "no_consta"
		}
		if v == "dubtos" {
			hasDubtos = true
		}
	}
	if hasDubtos {
		return "dubtos"
	}
	return ""
}

func cleanToken(token string) (string, string) {
	qual := ""
	if strings.Contains(token, "¿") {
		qual = "no_consta"
	} else if strings.Contains(token, "?") {
		qual = "dubtos"
	}
	token = strings.ReplaceAll(token, "¿", "")
	token = strings.ReplaceAll(token, "?", "")
	token = strings.Trim(token, " ,.;:")
	return token, qual
}

var surnameJoiners = map[string]bool{
	"de":   true,
	"del":  true,
	"dels": true,
	"da":   true,
	"das":  true,
	"dos":  true,
	"do":   true,
	"du":   true,
	"van":  true,
	"von":  true,
	"di":   true,
	"della": true,
	"d'":   true,
	"l'":   true,
}

var surnameArticles = map[string]bool{
	"la":  true,
	"el":  true,
	"els": true,
	"les": true,
	"los": true,
	"las": true,
	"l":   true,
}

func isSurnameJoiner(token string) bool {
	token = strings.ToLower(strings.TrimSpace(token))
	if token == "" {
		return false
	}
	if strings.HasSuffix(token, "'") {
		return true
	}
	return surnameJoiners[token]
}

func isSurnameArticle(token string) bool {
	token = strings.ToLower(strings.TrimSpace(token))
	if token == "" {
		return false
	}
	return surnameArticles[token]
}

func consumeSurnameFromStart(tokens, quals []string) (string, string, int) {
	if len(tokens) == 0 {
		return "", "", 0
	}
	parts := []string{tokens[0]}
	qualParts := []string{quals[0]}
	consumed := 1
	lower := strings.ToLower(tokens[0])
	if isSurnameJoiner(lower) && len(tokens) > 1 {
		if lower == "de" && len(tokens) > 2 && isSurnameArticle(tokens[1]) {
			parts = append(parts, tokens[1], tokens[2])
			qualParts = append(qualParts, quals[1], quals[2])
			consumed = 3
		} else {
			parts = append(parts, tokens[1])
			qualParts = append(qualParts, quals[1])
			consumed = 2
		}
	} else if isSurnameArticle(lower) && len(tokens) > 1 {
		parts = append(parts, tokens[1])
		qualParts = append(qualParts, quals[1])
		consumed = 2
	}
	return strings.Join(parts, " "), mergeQuality(qualParts...), consumed
}

func consumeSurnameFromEnd(tokens, quals []string) (string, string, int) {
	if len(tokens) == 0 {
		return "", "", 0
	}
	idx := len(tokens) - 1
	parts := []string{tokens[idx]}
	qualParts := []string{quals[idx]}
	consumed := 1
	if idx-1 >= 0 {
		prev := strings.ToLower(tokens[idx-1])
		if isSurnameArticle(prev) {
			parts = append([]string{tokens[idx-1]}, parts...)
			qualParts = append([]string{quals[idx-1]}, qualParts...)
			consumed++
			if idx-2 >= 0 && strings.ToLower(tokens[idx-2]) == "de" {
				parts = append([]string{tokens[idx-2]}, parts...)
				qualParts = append([]string{quals[idx-2]}, qualParts...)
				consumed++
			}
		} else if isSurnameJoiner(prev) {
			parts = append([]string{tokens[idx-1]}, parts...)
			qualParts = append([]string{quals[idx-1]}, qualParts...)
			consumed++
		}
	}
	return strings.Join(parts, " "), mergeQuality(qualParts...), consumed
}

func defaultQuality(val, qual string) string {
	if val == "" {
		return ""
	}
	if qual != "" {
		return qual
	}
	return "clar"
}

func parseDDMMYYYYToISO(raw string) (string, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	qual := ""
	if strings.Contains(raw, "¿") {
		qual = "no_consta"
	} else if strings.Contains(raw, "?") {
		qual = "dubtos"
	}
	cleaned := strings.ReplaceAll(raw, "¿", "")
	cleaned = strings.ReplaceAll(cleaned, "?", "")
	cleaned = strings.TrimSpace(strings.ReplaceAll(cleaned, ".", "/"))
	parts := strings.Split(cleaned, "/")
	if len(parts) != 3 {
		if qual != "" {
			return "", qual
		}
		return "", "incomplet"
	}
	day, errDay := strconv.Atoi(strings.TrimSpace(parts[0]))
	month, errMonth := strconv.Atoi(strings.TrimSpace(parts[1]))
	year, errYear := strconv.Atoi(strings.TrimSpace(parts[2]))
	if errDay != nil || errMonth != nil || errYear != nil {
		if qual != "" {
			return "", qual
		}
		return "", "incomplet"
	}
	if year < 100 {
		year += 1900
	}
	iso := fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	if qual != "" {
		return iso, qual
	}
	return iso, "clar"
}

func parseNameCognoms(raw string) (string, string, string, string, string, string, string) {
	main, extras := splitParentheticals(raw)
	tokens := strings.Fields(main)
	clean := make([]string, 0, len(tokens))
	quals := make([]string, 0, len(tokens))
	for _, tok := range tokens {
		tokClean, qual := cleanToken(tok)
		if tokClean != "" {
			clean = append(clean, tokClean)
			quals = append(quals, qual)
		}
	}
	var cognom1, cognom2, nom string
	var cognom1Qual, cognom2Qual, nomQual string
	if len(clean) >= 1 {
		consumed := 0
		cognom1, cognom1Qual, consumed = consumeSurnameFromStart(clean, quals)
		restTokens := clean[consumed:]
		restQuals := quals[consumed:]
		if len(restTokens) == 1 {
			// Si només queda un token, assumim que és el segon cognom i no el nom.
			cognom2 = restTokens[0]
			cognom2Qual = restQuals[0]
		} else if len(restTokens) >= 2 {
			consumed2 := 0
			cognom2, cognom2Qual, consumed2 = consumeSurnameFromStart(restTokens, restQuals)
			nameTokens := restTokens[consumed2:]
			if len(nameTokens) > 0 {
				nom = strings.Join(nameTokens, " ")
				nomQual = mergeQuality(restQuals[consumed2:]...)
			}
		}
	}
	notes := strings.Join(extras, "; ")
	municipi := ""
	if len(extras) > 0 {
		municipi = extras[0]
	}
	return nom, cognom1, cognom2, nomQual, cognom1Qual, cognom2Qual, notes + "|" + municipi
}

func parseNameNom(raw string) (string, string, string, string, string, string, string) {
	main, extras := splitParentheticals(raw)
	tokens := strings.Fields(main)
	clean := make([]string, 0, len(tokens))
	quals := make([]string, 0, len(tokens))
	for _, tok := range tokens {
		tokClean, qual := cleanToken(tok)
		if tokClean != "" {
			clean = append(clean, tokClean)
			quals = append(quals, qual)
		}
	}
	var cognom1, cognom2, nom string
	var cognom1Qual, cognom2Qual, nomQual string
	if len(clean) >= 1 {
		surname2, surname2Qual, consumed2 := consumeSurnameFromEnd(clean, quals)
		restTokens := clean[:len(clean)-consumed2]
		restQuals := quals[:len(quals)-consumed2]
		if len(restTokens) <= 1 {
			nom = strings.Join(restTokens, " ")
			if len(restQuals) > 0 {
				nomQual = mergeQuality(restQuals...)
			}
			cognom1 = surname2
			cognom1Qual = surname2Qual
		} else {
			cognom2 = surname2
			cognom2Qual = surname2Qual
			surname1, surname1Qual, consumed1 := consumeSurnameFromEnd(restTokens, restQuals)
			cognom1 = surname1
			cognom1Qual = surname1Qual
			nameTokens := restTokens[:len(restTokens)-consumed1]
			if len(nameTokens) > 0 {
				nom = strings.Join(nameTokens, " ")
				nomQual = mergeQuality(restQuals[:len(restQuals)-consumed1]...)
			}
		}
	}
	notes := strings.Join(extras, "; ")
	municipi := ""
	if len(extras) > 0 {
		municipi = extras[0]
	}
	return nom, cognom1, cognom2, nomQual, cognom1Qual, cognom2Qual, notes + "|" + municipi
}

func splitNotesMunicipi(val string) (string, string) {
	if val == "" {
		return "", ""
	}
	parts := strings.SplitN(val, "|", 2)
	notes := strings.TrimSpace(parts[0])
	municipi := ""
	if len(parts) == 2 {
		municipi = strings.TrimSpace(parts[1])
	}
	return notes, municipi
}

func buildPersonFromCognoms(raw, role string) *db.TranscripcioPersonaRaw {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	nom, cognom1, cognom2, nomQual, cognom1Qual, cognom2Qual, extra := parseNameCognoms(raw)
	notes, municipi := splitNotesMunicipi(extra)
	p := &db.TranscripcioPersonaRaw{
		Rol:          role,
		Nom:          nom,
		Cognom1:      cognom1,
		Cognom2:      cognom2,
		NomEstat:     defaultQuality(nom, nomQual),
		Cognom1Estat: defaultQuality(cognom1, cognom1Qual),
		Cognom2Estat: defaultQuality(cognom2, cognom2Qual),
		Notes:        notes,
	}
	if municipi != "" {
		p.MunicipiText = municipi
		p.MunicipiEstat = "clar"
	}
	return p
}

func buildPersonFromNom(raw, role string) *db.TranscripcioPersonaRaw {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	nom, cognom1, cognom2, nomQual, cognom1Qual, cognom2Qual, extra := parseNameNom(raw)
	notes, municipi := splitNotesMunicipi(extra)
	p := &db.TranscripcioPersonaRaw{
		Rol:          role,
		Nom:          nom,
		Cognom1:      cognom1,
		Cognom2:      cognom2,
		NomEstat:     defaultQuality(nom, nomQual),
		Cognom1Estat: defaultQuality(cognom1, cognom1Qual),
		Cognom2Estat: defaultQuality(cognom2, cognom2Qual),
		Notes:        notes,
	}
	if municipi != "" {
		p.MunicipiText = municipi
		p.MunicipiEstat = "clar"
	}
	return p
}

func splitCouple(val string) (string, string) {
	lower := strings.ToLower(val)
	idx := strings.Index(lower, " i ")
	if idx == -1 {
		return strings.TrimSpace(val), ""
	}
	return strings.TrimSpace(val[:idx]), strings.TrimSpace(val[idx+3:])
}

func valueFromRow(headers map[string]int, row []string, keys ...string) string {
	for _, key := range keys {
		if idx, ok := headers[key]; ok && idx < len(row) {
			return strings.TrimSpace(row[idx])
		}
	}
	return ""
}

func buildDedupKey(parts ...string) string {
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		clean = append(clean, strings.ToLower(strings.TrimSpace(part)))
	}
	return strings.Join(clean, "|")
}

func normalizeNameKey(nom, cognom1, cognom2 string) string {
	parts := []string{nom, cognom1, cognom2}
	for i, part := range parts {
		part = strings.ToLower(strings.TrimSpace(part))
		part = stripDiacritics(part)
		part = strings.Join(strings.Fields(part), " ")
		parts[i] = part
	}
	return strings.Join(parts, "|")
}

func personKey(p *db.TranscripcioPersonaRaw) string {
	if p == nil {
		return ""
	}
	return p.Rol + "|" + normalizeNameKey(p.Nom, p.Cognom1, p.Cognom2)
}

func marcmoiaImportFields(llibreRaw, paginaLlibre, paginaReal, anyDoc, cognoms, pare, mare, avisPaterns, avisMaterns, nascut, bateig, ofici, defuncio string) map[string]string {
	return map[string]string{
		"llibre":         llibreRaw,
		"pagina_llibre":  paginaLlibre,
		"pagina_digital": paginaReal,
		"any":            anyDoc,
		"cognoms":        cognoms,
		"pare":           pare,
		"mare":           mare,
		"avis_paterns":   avisPaterns,
		"avis_materns":   avisMaterns,
		"nascut":         nascut,
		"bateig":         bateig,
		"ofici":          ofici,
		"defuncio":       defuncio,
	}
}

func (a *App) importBaptismesMarcmoiaCSV(reader io.Reader, sep rune, userID int, ctx importContext) csvImportResult {
	result := csvImportResult{}
	csvReader := csv.NewReader(reader)
	csvReader.Comma = sep
	csvReader.TrimLeadingSpace = true
	headers, err := csvReader.Read()
	if err != nil {
		result.Failed = 1
		result.Errors = append(result.Errors, importErrorEntry{Row: 0, Reason: "capçalera CSV invàlida"})
		return result
	}
	headerMap := map[string]int{}
	for i, h := range headers {
		headerMap[normalizeCSVHeader(h)] = i
	}
	filter := db.LlibreFilter{}
	if ctx.MunicipiID != 0 {
		filter.MunicipiID = ctx.MunicipiID
	}
	if ctx.ArxiuID != 0 {
		filter.ArxiuID = ctx.ArxiuID
	}
	llibres, _ := a.DB.ListLlibres(filter)
	type bookInfo struct {
		ID      int
		Indexed bool
	}
	bookMap := map[string]bookInfo{}
	for _, l := range llibres {
		norm := normalizeCronologia(l.Cronologia)
		if norm == "" {
			continue
		}
		if existing, ok := bookMap[norm]; ok {
			if existing.ID != l.ID {
				bookMap[norm] = bookInfo{ID: -1}
			}
		} else {
			bookMap[norm] = bookInfo{ID: l.ID, Indexed: l.IndexacioCompleta}
		}
	}
	existingByBook := map[int]map[string]int{}
	seenMatch := map[string]int{}
	seen := map[string]int{}
	rowNum := 1
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		rowNum++
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "error llegint fila"})
			continue
		}
		llibreRaw := valueFromRow(headerMap, record, "llibre")
		if llibreRaw == "" {
			result.Failed++
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "llibre buit", Fields: map[string]string{"llibre": llibreRaw}})
			continue
		}
		bookInfo, ok := bookMap[normalizeCronologia(llibreRaw)]
		if !ok {
			result.Failed++
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "llibre no trobat: " + llibreRaw, Fields: map[string]string{"llibre": llibreRaw}})
			continue
		}
		if bookInfo.ID == -1 {
			result.Failed++
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "llibre ambigu: " + llibreRaw, Fields: map[string]string{"llibre": llibreRaw}})
			continue
		}
		llibreID := bookInfo.ID
		paginaLlibre := valueFromRow(headerMap, record, "paginallibre")
		paginaReal := valueFromRow(headerMap, record, "paginareal")
		anyDoc := valueFromRow(headerMap, record, "any")
		cognoms := valueFromRow(headerMap, record, "cognoms")
		pare := valueFromRow(headerMap, record, "pare")
		mare := valueFromRow(headerMap, record, "mare")
		avisPaterns := valueFromRow(headerMap, record, "avispaterns")
		avisMaterns := valueFromRow(headerMap, record, "avismaterns")
		casat := valueFromRow(headerMap, record, "casat")
		nascut := valueFromRow(headerMap, record, "nascut")
		padri := valueFromRow(headerMap, record, "padridbateig", "padribateig")
		padrina := valueFromRow(headerMap, record, "padrinetadebateig", "padrinadebateig")
		bateig := valueFromRow(headerMap, record, "bateig")
		ofici := valueFromRow(headerMap, record, "ofici")
		defuncio := valueFromRow(headerMap, record, "defuncio")
		fields := marcmoiaImportFields(llibreRaw, paginaLlibre, paginaReal, anyDoc, cognoms, pare, mare, avisPaterns, avisMaterns, nascut, bateig, ofici, defuncio)

		t := db.TranscripcioRaw{
			LlibreID:       llibreID,
			NumPaginaText:  paginaLlibre,
			AnyDoc:         parseIntNull(anyDoc),
			TipusActe:      "baptisme",
			ModeracioEstat: "pendent",
			CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
		}
		if bateig != "" {
			t.DataActeText = bateig
			iso, estat := parseDDMMYYYYToISO(bateig)
			if iso != "" {
				t.DataActeISO = sql.NullString{String: iso, Valid: true}
			}
			if estat != "" {
				t.DataActeEstat = estat
			}
		}
		if t.DataActeEstat == "" {
			if t.DataActeText != "" {
				t.DataActeEstat = "clar"
			} else {
				t.DataActeEstat = "no_consta"
			}
		}
		if !validTipusActe(t.TipusActe) {
			result.Failed++
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "tipus_acte invàlid"})
			continue
		}
		persones := []*db.TranscripcioPersonaRaw{}
		if p := buildPersonFromCognoms(cognoms, "batejat"); p != nil {
			persones = append(persones, p)
		}
		if p := buildPersonFromNom(pare, "pare"); p != nil {
			p.OficiText = ofici
			p.OficiEstat = defaultQuality(p.OficiText, "")
			persones = append(persones, p)
		}
		if p := buildPersonFromNom(mare, "mare"); p != nil {
			persones = append(persones, p)
		}
		if avisPaterns != "" {
			avi, avia := splitCouple(avisPaterns)
			if p := buildPersonFromNom(avi, "avi_patern"); p != nil {
				persones = append(persones, p)
			}
			if p := buildPersonFromNom(avia, "avia_paterna"); p != nil {
				persones = append(persones, p)
			}
		}
		if avisMaterns != "" {
			avi, avia := splitCouple(avisMaterns)
			if p := buildPersonFromNom(avi, "avi_matern"); p != nil {
				persones = append(persones, p)
			}
			if p := buildPersonFromNom(avia, "avia_materna"); p != nil {
				persones = append(persones, p)
			}
		}
		if p := buildPersonFromNom(padri, "padri"); p != nil {
			persones = append(persones, p)
		}
		if p := buildPersonFromNom(padrina, "padrina"); p != nil {
			persones = append(persones, p)
		}
		attrs := []*db.TranscripcioAtributRaw{}
		if paginaReal != "" {
			attrs = append(attrs, &db.TranscripcioAtributRaw{
				Clau:       "pagina_digital",
				TipusValor: "text",
				ValorText:  paginaReal,
				Estat:      "clar",
			})
		}
		if nascut != "" {
			iso, estat := parseDDMMYYYYToISO(nascut)
			attr := &db.TranscripcioAtributRaw{Clau: "data_naixement", TipusValor: "date", Estat: estat}
			if iso != "" {
				attr.ValorDate = sql.NullString{String: iso, Valid: true}
			} else {
				attr.TipusValor = "text"
				attr.ValorText = nascut
			}
			attrs = append(attrs, attr)
		}
		if bateig != "" {
			iso, estat := parseDDMMYYYYToISO(bateig)
			attr := &db.TranscripcioAtributRaw{Clau: "data_bateig", TipusValor: "date", Estat: estat}
			if iso != "" {
				attr.ValorDate = sql.NullString{String: iso, Valid: true}
			} else {
				attr.TipusValor = "text"
				attr.ValorText = bateig
			}
			attrs = append(attrs, attr)
		}
		if defuncio != "" {
			iso, estat := parseDDMMYYYYToISO(defuncio)
			attr := &db.TranscripcioAtributRaw{Clau: "data_defuncio", TipusValor: "date", Estat: estat}
			if iso != "" {
				attr.ValorDate = sql.NullString{String: iso, Valid: true}
			} else {
				attr.TipusValor = "text"
				attr.ValorText = defuncio
			}
			attrs = append(attrs, attr)
		}
		if casat != "" {
			attrs = append(attrs, &db.TranscripcioAtributRaw{
				Clau:       "casat",
				TipusValor: "text",
				ValorText:  casat,
				Estat:      "clar",
			})
		}
		nomDetectat, cognom1Detectat, cognom2Detectat, _, _, _, _ := parseNameCognoms(cognoms)
		key := buildDedupKey(
			strconv.Itoa(llibreID),
			paginaLlibre,
			paginaReal,
			anyDoc,
			cognoms,
			pare,
			mare,
			avisPaterns,
			avisMaterns,
			casat,
			nascut,
			padri,
			padrina,
			bateig,
			ofici,
			defuncio,
		)
		if nomDetectat == "" {
			key = key + "|row:" + strconv.Itoa(rowNum)
		}
		matchKey := ""
		matchSeenKey := ""
		if bookInfo.Indexed {
			matchKey = normalizeNameKey(nomDetectat, cognom1Detectat, cognom2Detectat)
			if matchKey != "" {
				matchSeenKey = strconv.Itoa(llibreID) + "|" + matchKey
				if firstRow, ok := seenMatch[matchSeenKey]; ok {
					result.Failed++
					fields["duplicate_row"] = strconv.Itoa(firstRow)
					result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "registre duplicat", Fields: fields})
					continue
				}
			}
		}
		if bookInfo.Indexed && matchKey != "" {
			existingMap, ok := existingByBook[llibreID]
			if !ok {
				existingMap = map[string]int{}
				trans, _ := a.DB.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{})
				for _, tr := range trans {
					personesExistents, _ := a.DB.ListTranscripcioPersones(tr.ID)
					for _, p := range personesExistents {
						if p.Rol != "batejat" && p.Rol != "persona_principal" {
							continue
						}
						nameKey := normalizeNameKey(p.Nom, p.Cognom1, p.Cognom2)
						if nameKey == "" {
							continue
						}
						if _, exists := existingMap[nameKey]; !exists {
							existingMap[nameKey] = tr.ID
						}
						break
					}
				}
				existingByBook[llibreID] = existingMap
			}
			if existingID, ok := existingMap[matchKey]; ok {
				existing, err := a.DB.GetTranscripcioRaw(existingID)
				if err != nil || existing == nil {
					result.Failed++
					reason := "no s'ha pogut actualitzar el registre"
					if err != nil {
						reason = fmt.Sprintf("no s'ha pogut actualitzar el registre: %v", err)
					}
					result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: reason, Fields: fields})
					continue
				}
				updated := false
				if existing.NumPaginaText == "" && t.NumPaginaText != "" {
					existing.NumPaginaText = t.NumPaginaText
					updated = true
				}
				if !existing.PosicioPagina.Valid && t.PosicioPagina.Valid {
					existing.PosicioPagina = t.PosicioPagina
					updated = true
				}
				if !existing.AnyDoc.Valid && t.AnyDoc.Valid {
					existing.AnyDoc = t.AnyDoc
					updated = true
				}
				if existing.DataActeText == "" && t.DataActeText != "" {
					existing.DataActeText = t.DataActeText
					updated = true
				}
				if !existing.DataActeISO.Valid && t.DataActeISO.Valid {
					existing.DataActeISO = t.DataActeISO
					updated = true
				}
				if (existing.DataActeEstat == "" || existing.DataActeEstat == "no_consta") && t.DataActeEstat != "" {
					existing.DataActeEstat = t.DataActeEstat
					updated = true
				}
				if updated {
					_ = a.DB.UpdateTranscripcioRaw(existing)
				}
				personesExistents, _ := a.DB.ListTranscripcioPersones(existingID)
				personKeys := map[string]bool{}
				for _, p := range personesExistents {
					personKeys[personKey(&p)] = true
				}
				for _, p := range persones {
					if p.Nom == "" && p.Cognom1 == "" && p.Cognom2 == "" && p.Notes == "" && p.MunicipiText == "" && p.OficiText == "" {
						continue
					}
					keyPerson := personKey(p)
					if personKeys[keyPerson] {
						continue
					}
					p.TranscripcioID = existingID
					_, _ = a.DB.CreateTranscripcioPersona(p)
					personKeys[keyPerson] = true
				}
				attrsExistents, _ := a.DB.ListTranscripcioAtributs(existingID)
				attrKeys := map[string]bool{}
				for _, a := range attrsExistents {
					attrKeys[a.Clau] = true
				}
				for _, attr := range attrs {
					if attr.ValorText == "" && !attr.ValorInt.Valid && !attr.ValorDate.Valid && !attr.ValorBool.Valid && attr.Estat == "" {
						continue
					}
					if attrKeys[attr.Clau] {
						continue
					}
					attr.TranscripcioID = existingID
					_, _ = a.DB.CreateTranscripcioAtribut(attr)
					attrKeys[attr.Clau] = true
				}
				if matchSeenKey != "" {
					seenMatch[matchSeenKey] = rowNum
				}
				result.Updated++
				result.markBook(llibreID)
				continue
			}
		}
		if firstRow, ok := seen[key]; ok {
			result.Failed++
			fields["duplicate_row"] = strconv.Itoa(firstRow)
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "registre duplicat", Fields: fields})
			continue
		}
		seen[key] = rowNum
		id, err := a.DB.CreateTranscripcioRaw(&t)
		if err != nil || id == 0 {
			result.Failed++
			reason := "no s'ha pogut crear el registre"
			if err != nil {
				reason = fmt.Sprintf("no s'ha pogut crear el registre: %v", err)
			}
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: reason, Fields: fields})
			continue
		}
		for _, p := range persones {
			if p.Nom == "" && p.Cognom1 == "" && p.Cognom2 == "" && p.Notes == "" && p.MunicipiText == "" && p.OficiText == "" {
				continue
			}
			p.TranscripcioID = id
			_, _ = a.DB.CreateTranscripcioPersona(p)
		}
		for _, attr := range attrs {
			if attr.ValorText == "" && !attr.ValorInt.Valid && !attr.ValorDate.Valid && !attr.ValorBool.Valid && attr.Estat == "" {
				continue
			}
			attr.TranscripcioID = id
			_, _ = a.DB.CreateTranscripcioAtribut(attr)
		}
		result.Created++
		result.markBook(llibreID)
		if bookInfo.Indexed && matchKey != "" {
			if existingMap, ok := existingByBook[llibreID]; ok {
				existingMap[matchKey] = id
			} else {
				existingByBook[llibreID] = map[string]int{matchKey: id}
			}
			if matchSeenKey != "" {
				seenMatch[matchSeenKey] = rowNum
			}
		}
	}
	return result
}
