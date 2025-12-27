package core

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"
	"unicode"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const cognomsImportBatchSize = 2000

type cognomIndexEntry struct {
	ID    int
	Forma string
	Key   string
}

type cognomStatsKey struct {
	cognomID   int
	municipiID int
	anyDoc     int
}

func (a *App) AdminCognomsImport(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
		return
	}
	q := r.URL.Query()
	importRun := q.Get("import") == "1"
	statsRun := q.Get("stats") == "1"
	importTotal := parseIntQuery(q.Get("total"))
	importCreated := parseIntQuery(q.Get("created"))
	importSkipped := parseIntQuery(q.Get("skipped"))
	importErrors := parseIntQuery(q.Get("errors"))
	statsTotal := parseIntQuery(q.Get("stats_total"))
	msg := ""
	if q.Get("err") != "" {
		msg = T(ResolveLang(r), "common.error")
	}
	RenderPrivateTemplate(w, r, "admin-cognoms-import.html", map[string]interface{}{
		"ImportRun":     importRun,
		"StatsRun":      statsRun,
		"ImportTotal":   importTotal,
		"ImportCreated": importCreated,
		"ImportSkipped": importSkipped,
		"ImportErrors":  importErrors,
		"StatsTotal":    statsTotal,
		"Msg":           msg,
	})
}

func (a *App) AdminCognomsImportRun(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin/cognoms/import?err=1", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Redirect(w, r, "/admin/cognoms/import?err=1", http.StatusSeeOther)
		return
	}
	if purged, err := a.purgeInvalidCognoms(); err != nil {
		Errorf("Error netejant cognoms invalids: %v", err)
		http.Redirect(w, r, "/admin/cognoms/import?err=1", http.StatusSeeOther)
		return
	} else if purged > 0 {
		Infof("Cognoms invalids eliminats abans de l'import: %d", purged)
	}
	cognoms, err := a.DB.ListCognoms("", 0, 0)
	if err != nil {
		http.Redirect(w, r, "/admin/cognoms/import?err=1", http.StatusSeeOther)
		return
	}
	cognomIndex := make(map[string]cognomIndexEntry, len(cognoms))
	for _, c := range cognoms {
		cognomIndex[c.Key] = cognomIndexEntry{ID: c.ID, Forma: c.Forma, Key: c.Key}
	}
	variantIndex := map[int]map[string]struct{}{}
	for _, entry := range cognomIndex {
		if _, ok := variantIndex[entry.ID]; !ok {
			variantIndex[entry.ID] = map[string]struct{}{}
		}
		if entry.Key != "" {
			variantIndex[entry.ID][entry.Key] = struct{}{}
		}
	}
	if variants, err := a.DB.ListCognomVariants(db.CognomVariantFilter{}); err == nil {
		for _, v := range variants {
			key := strings.TrimSpace(v.Key)
			if key == "" {
				key = NormalizeCognomKey(v.Variant)
			}
			if key == "" {
				continue
			}
			if _, ok := variantIndex[v.CognomID]; !ok {
				variantIndex[v.CognomID] = map[string]struct{}{}
			}
			variantIndex[v.CognomID][key] = struct{}{}
		}
	}
	total := 0
	created := 0
	skipped := 0
	errors := 0
	offset := 0
	for {
		rows, err := a.DB.ListCognomImportRows(cognomsImportBatchSize, offset)
		if err != nil {
			http.Redirect(w, r, "/admin/cognoms/import?err=1", http.StatusSeeOther)
			return
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			createdNow, skippedNow, errNow := a.importCognomValue(row.Cognom1, row.Cognom1Estat, cognomIndex, variantIndex)
			total += createdNow + skippedNow
			if createdNow > 0 {
				created += createdNow
			}
			if skippedNow > 0 {
				skipped += skippedNow
			}
			if errNow {
				errors++
			}
			createdNow, skippedNow, errNow = a.importCognomValue(row.Cognom2, row.Cognom2Estat, cognomIndex, variantIndex)
			total += createdNow + skippedNow
			if createdNow > 0 {
				created += createdNow
			}
			if skippedNow > 0 {
				skipped += skippedNow
			}
			if errNow {
				errors++
			}
		}
		offset += cognomsImportBatchSize
	}
	redirect := "/admin/cognoms/import?import=1" +
		"&total=" + strconv.Itoa(total) +
		"&created=" + strconv.Itoa(created) +
		"&skipped=" + strconv.Itoa(skipped) +
		"&errors=" + strconv.Itoa(errors)
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}

func (a *App) AdminCognomsStatsRun(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin/cognoms/import?err=1", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Redirect(w, r, "/admin/cognoms/import?err=1", http.StatusSeeOther)
		return
	}
	if purged, err := a.purgeInvalidCognoms(); err != nil {
		Errorf("Error netejant cognoms invalids: %v", err)
		http.Redirect(w, r, "/admin/cognoms/import?err=1", http.StatusSeeOther)
		return
	} else if purged > 0 {
		Infof("Cognoms invalids eliminats abans de les estadistiques: %d", purged)
	}
	if _, err := a.DB.Exec("DELETE FROM cognoms_freq_municipi_any"); err != nil {
		http.Redirect(w, r, "/admin/cognoms/import?err=1", http.StatusSeeOther)
		return
	}
	cognoms, err := a.DB.ListCognoms("", 0, 0)
	if err != nil {
		http.Redirect(w, r, "/admin/cognoms/import?err=1", http.StatusSeeOther)
		return
	}
	cognomIndex := make(map[string]cognomIndexEntry, len(cognoms))
	for _, c := range cognoms {
		cognomIndex[c.Key] = cognomIndexEntry{ID: c.ID, Forma: c.Forma, Key: c.Key}
	}
	counts := make(map[cognomStatsKey]int)
	offset := 0
	for {
		rows, err := a.DB.ListCognomStatsRows(cognomsImportBatchSize, offset)
		if err != nil {
			http.Redirect(w, r, "/admin/cognoms/import?err=1", http.StatusSeeOther)
			return
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			if !row.AnyDoc.Valid || !row.MunicipiID.Valid {
				continue
			}
			anyDoc := int(row.AnyDoc.Int64)
			munID := int(row.MunicipiID.Int64)
			a.countCognomStats(row.Cognom1, row.Cognom1Estat, anyDoc, munID, cognomIndex, counts)
			a.countCognomStats(row.Cognom2, row.Cognom2Estat, anyDoc, munID, cognomIndex, counts)
		}
		offset += cognomsImportBatchSize
	}
	statsTotal := 0
	for key, freq := range counts {
		if err := a.DB.UpsertCognomFreqMunicipiAny(key.cognomID, key.municipiID, key.anyDoc, freq); err != nil {
			http.Redirect(w, r, "/admin/cognoms/import?err=1", http.StatusSeeOther)
			return
		}
		statsTotal++
	}
	http.Redirect(w, r, "/admin/cognoms/import?stats=1&stats_total="+strconv.Itoa(statsTotal), http.StatusSeeOther)
}

func (a *App) importCognomValue(value sql.NullString, estat sql.NullString, index map[string]cognomIndexEntry, variants map[int]map[string]struct{}) (int, int, bool) {
	forma := cleanCognomImportValue(value, estat)
	if forma == "" {
		return 0, 1, false
	}
	key := NormalizeCognomKey(forma)
	if key == "" {
		return 0, 1, false
	}
	if entry, exists := index[key]; exists {
		if strings.EqualFold(strings.TrimSpace(entry.Forma), forma) {
			return 0, 1, false
		}
		if entry.Key == key {
			return 0, 1, false
		}
		if variants != nil {
			if _, ok := variants[entry.ID]; !ok {
				variants[entry.ID] = map[string]struct{}{}
			}
			if _, ok := variants[entry.ID][key]; ok {
				return 0, 1, false
			}
			cv := &db.CognomVariant{
				CognomID:       entry.ID,
				Variant:        forma,
				Key:            key,
				ModeracioEstat: "publicat",
				ModeracioMotiu: "import_auto",
			}
			if _, err := a.DB.CreateCognomVariant(cv); err != nil {
				return 0, 0, true
			}
			variants[entry.ID][key] = struct{}{}
			return 1, 0, false
		}
		return 0, 1, false
	}
	id, err := a.DB.UpsertCognom(forma, key, "import_auto", "import_auto", nil)
	if err != nil {
		return 0, 0, true
	}
	index[key] = cognomIndexEntry{ID: id, Forma: forma, Key: key}
	return 1, 0, false
}

func (a *App) countCognomStats(value sql.NullString, estat sql.NullString, anyDoc, municipiID int, index map[string]cognomIndexEntry, counts map[cognomStatsKey]int) {
	forma := cleanCognomImportValue(value, estat)
	if forma == "" {
		return
	}
	key := NormalizeCognomKey(forma)
	if key == "" {
		return
	}
	entry, ok := index[key]
	if !ok {
		newID, err := a.DB.UpsertCognom(forma, key, "import_auto", "import_auto", nil)
		if err != nil {
			return
		}
		entry = cognomIndexEntry{ID: newID, Forma: forma, Key: key}
		index[key] = entry
	}
	k := cognomStatsKey{cognomID: entry.ID, municipiID: municipiID, anyDoc: anyDoc}
	counts[k]++
}

func cleanCognomImportValue(value sql.NullString, estat sql.NullString) string {
	if !value.Valid {
		return ""
	}
	trimmed := strings.TrimSpace(value.String)
	if trimmed == "" {
		return ""
	}
	if strings.Contains(trimmed, "?") {
		return ""
	}
	if estat.Valid {
		status := strings.ToLower(strings.TrimSpace(estat.String))
		if status == "illegible" || status == "no_consta" {
			return ""
		}
	}
	cleaned := sanitizeCognomLiteral(trimmed)
	if cleaned == "" {
		return ""
	}
	return cleaned
}

func sanitizeCognomLiteral(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "\"'“”«»")
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.ContainsAny(value, "0123456789") {
		return ""
	}
	if strings.ContainsAny(value, "()[]{}") {
		return ""
	}
	for _, r := range value {
		if unicode.IsLetter(r) || r == ' ' || r == '-' || r == '\'' || r == '’' || r == '·' || r == '.' {
			continue
		}
		return ""
	}
	value = strings.Join(strings.Fields(value), " ")
	if len([]rune(value)) < 2 {
		return ""
	}
	return value
}

func (a *App) purgeInvalidCognoms() (int, error) {
	cognoms, err := a.DB.ListCognoms("", 0, 0)
	if err != nil {
		return 0, err
	}
	purged := 0
	for _, c := range cognoms {
		if sanitizeCognomLiteral(c.Forma) == "" {
			if _, err := a.DB.Exec("DELETE FROM cognoms WHERE id = " + strconv.Itoa(c.ID)); err != nil {
				return purged, err
			}
			purged++
		}
	}
	return purged, nil
}

func parseIntQuery(v string) int {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}
	return n
}
