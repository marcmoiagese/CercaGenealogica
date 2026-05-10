package core

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var Templates *template.Template

type DataContext struct {
	UserLoggedIn bool
	Lang         string
	Platform     PlatformSettings
	Maintenance  *MaintenanceBanner
	Data         interface{}
}

// Funcions personalitzades per a les plantilles
var templateFuncs = template.FuncMap{
	"default": func(value interface{}, defaultValue interface{}) interface{} {
		if value == nil || value == "" {
			return defaultValue
		}
		return value
	},
	"t": func(lang interface{}, key string, args ...interface{}) string {
		langStr := fmt.Sprint(lang)
		text := T(langStr, key)
		if len(args) > 0 {
			return fmt.Sprintf(text, args...)
		}
		return text
	},
	"index": templateIndex,
	"list": func(values ...string) []string {
		return values
	},
	"dict": func(values ...interface{}) map[string]interface{} {
		result := make(map[string]interface{})
		for i := 0; i+1 < len(values); i += 2 {
			key, ok := values[i].(string)
			if !ok {
				continue
			}
			result[key] = values[i+1]
		}
		return result
	},
	"slice": func(values ...interface{}) []interface{} {
		return values
	},
	"add": func(a, b int) int {
		return a + b
	},
	"upper": func(s string) string {
		return strings.ToUpper(s)
	},
	"messageHTML": renderMessageHTML,
	"diffField": func(v interface{}) template.HTML {
		var fd fieldDiff
		switch t := v.(type) {
		case fieldDiff:
			fd = t
		case *fieldDiff:
			if t == nil {
				return template.HTML("")
			}
			fd = *t
		default:
			return template.HTML("")
		}
		before := strings.TrimSpace(fd.Before)
		after := strings.TrimSpace(fd.After)
		changed := fd.Changed || before != after
		if !changed {
			if after != "" {
				return template.HTML(template.HTMLEscapeString(after))
			}
			return template.HTML(template.HTMLEscapeString(before))
		}
		var parts []string
		renderLines := func(value string, class string, sign string) {
			lines := strings.Split(value, "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				tag := ""
				if idx := strings.LastIndex(line, "||v:"); idx != -1 {
					tag = strings.TrimSpace(line[idx+4:])
					line = strings.TrimSpace(line[:idx])
				}
				if line == "" {
					continue
				}
				suffix := ""
				if tag != "" {
					suffix = fmt.Sprintf(`<sup class="diff-version">v%s</sup>`, template.HTMLEscapeString(tag))
				}
				parts = append(parts, fmt.Sprintf(`<div class="%s">%s %s%s</div>`, class, sign, template.HTMLEscapeString(line), suffix))
			}
		}
		if before != "" {
			renderLines(before, "diff-before", "−")
		}
		if after != "" {
			renderLines(after, "diff-after", "+")
		}
		if len(parts) == 0 {
			return template.HTML("")
		}
		return template.HTML(`<div class="diff-block">` + strings.Join(parts, "") + `</div>`)
	},
	// idx: accés segur a slices/arrays/mapes via reflect
	"idx": func(collection interface{}, index int) interface{} {
		v := reflect.ValueOf(collection)
		if !v.IsValid() {
			return nil
		}
		switch v.Kind() {
		case reflect.Slice, reflect.Array:
			if index >= 0 && index < v.Len() {
				return v.Index(index).Interface()
			}
		case reflect.Map:
			key := reflect.ValueOf(index)
			if v.MapIndex(key).IsValid() {
				return v.MapIndex(key).Interface()
			}
		}
		return nil
	},
	"int": func(v interface{}) int {
		switch t := v.(type) {
		case int:
			return t
		case int8:
			return int(t)
		case int16:
			return int(t)
		case int32:
			return int(t)
		case int64:
			return int(t)
		case uint:
			return int(t)
		case uint8:
			return int(t)
		case uint16:
			return int(t)
		case uint32:
			return int(t)
		case uint64:
			return int(t)
		case sql.NullInt64:
			if t.Valid {
				return int(t.Int64)
			}
			return 0
		default:
			return 0
		}
	},
	"toJson": func(v interface{}) string {
		b, err := json.Marshal(v)
		if err != nil {
			return "[]"
		}
		return string(b)
	},
	"toJsonJS": func(v interface{}) template.JS {
		b, err := json.Marshal(v)
		if err != nil {
			return template.JS("null")
		}
		return template.JS(b)
	},
}

func init() {
	// Crear template amb funcions personalitzades
	Templates = template.New("").Funcs(templateFuncs)

	// Carregar plantilles
	parsePattern := func(pattern string) {
		matches, gerr := filepath.Glob(pattern)
		if gerr != nil {
			log.Printf("Error buscant plantilles amb patró %s: %v", pattern, gerr)
			return
		}
		if len(matches) == 0 {
			Debugf("Cap plantilla trobada per al patró %s (omitint)", pattern)
			return
		}
		Templates = template.Must(Templates.ParseFiles(matches...))
	}

	parsePattern("templates/*.html")
	parsePattern("templates/layouts/*.html")
	parsePattern("templates/admin/*.html")

	Infof("Plantilles carregades:")
	for _, t := range Templates.Templates() {
		Debugf(" - %q", t.Name())
	}
}

// LogLoadedTemplates – permet registrar plantilles carregades quan es canvia el nivell de log
func LogLoadedTemplates() {
	if Templates == nil {
		return
	}
	Infof("Plantilles carregades:")
	for _, t := range Templates.Templates() {
		Debugf(" - %q", t.Name())
	}
}

func RenderTemplate(w http.ResponseWriter, r *http.Request, templateName string, data interface{}) {
	lang := ResolveLang(r)
	csrfToken, _ := ensureCSRF(w, r)
	data = injectCSRFToken(data, csrfToken)
	err := Templates.ExecuteTemplate(w, templateName, &DataContext{
		UserLoggedIn: false,
		Lang:         lang,
		Platform:     GetPlatformSettings(lang),
		Maintenance:  GetMaintenanceBanner(time.Now()),
		Data:         data,
	})
	if err != nil {
		Errorf("Error renderitzant plantilla %s: %v", templateName, err)
		// No cridem http.Error() aquí per evitar "superfluous response.WriteHeader call"
		// ja que ExecuteTemplate ja ha escrit al ResponseWriter
		return
	}
}

func RenderPrivateTemplate(w http.ResponseWriter, r *http.Request, tmpl string, data interface{}) {
	lang := ResolveLang(r)
	csrfToken, _ := ensureCSRF(w, r)
	data = injectPermsIfMissing(r, injectUserIfMissing(r, injectCSRFToken(data, csrfToken)))
	err := Templates.ExecuteTemplate(w, tmpl, &DataContext{
		UserLoggedIn: true,
		Lang:         lang,
		Platform:     GetPlatformSettings(lang),
		Maintenance:  GetMaintenanceBanner(time.Now()),
		Data:         data,
	})
	if err != nil {
		Errorf("Error renderitzant plantilla %s: %v", tmpl, err)
		// No cridem http.Error() aquí per evitar "superfluous response.WriteHeader call"
		// ja que ExecuteTemplate ja ha escrit al ResponseWriter
		return
	}
}

// RenderPrivateTemplateLang permet forçar l'idioma (p.ex. idioma preferit de l'usuari logat).
func RenderPrivateTemplateLang(w http.ResponseWriter, r *http.Request, tmpl string, lang string, data interface{}) {
	csrfToken, _ := ensureCSRF(w, r)
	data = injectPermsIfMissing(r, injectUserIfMissing(r, injectCSRFToken(data, csrfToken)))
	err := Templates.ExecuteTemplate(w, tmpl, &DataContext{
		UserLoggedIn: true,
		Lang:         lang,
		Platform:     GetPlatformSettings(lang),
		Maintenance:  GetMaintenanceBanner(time.Now()),
		Data:         data,
	})
	if err != nil {
		Errorf("Error renderitzant plantilla %s: %v", tmpl, err)
		return
	}
}

// injectCSRFToken insereix CSRFToken i retorna el data (map o struct) amb el token aplicat.
func injectCSRFToken(data interface{}, token string) interface{} {
	if data == nil {
		return map[string]interface{}{"CSRFToken": token}
	}
	if m, ok := data.(map[string]interface{}); ok {
		m["CSRFToken"] = token
		return m
	}

	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return map[string]interface{}{"CSRFToken": token}
		}
		elem := v.Elem()
		if elem.Kind() == reflect.Struct {
			field := elem.FieldByName("CSRFToken")
			if field.IsValid() && field.CanSet() && field.Kind() == reflect.String {
				field.SetString(token)
				return data
			}
		}
	}

	if v.Kind() == reflect.Struct {
		// crear còpia addressable
		copyVal := reflect.New(v.Type()).Elem()
		copyVal.Set(v)
		field := copyVal.FieldByName("CSRFToken")
		if field.IsValid() && field.CanSet() && field.Kind() == reflect.String {
			field.SetString(token)
			// retorna pointer a la còpia per mantenir addressable
			ptr := copyVal.Addr().Interface()
			return ptr
		}
	}

	return data
}

func boolTemplateFlag(m map[string]interface{}, key string) bool {
	v, ok := m[key]
	if !ok {
		return false
	}
	b, ok := v.(bool)
	return ok && b
}

// injectUserIfMissing intenta inserir l'usuari obtingut del context (RequireLogin/requirePermission)
// quan el handler no l'ha passat explícitament al mapa de dades. Només actua sobre map[string]interface{}.
func injectUserIfMissing(r *http.Request, data interface{}) interface{} {
	m, ok := data.(map[string]interface{})
	if !ok {
		return data
	}
	if _, found := m["User"]; found {
		return data
	}
	if u := userFromContext(r); u != nil {
		m["User"] = u
	}
	return m
}

func templateIndex(m interface{}, k interface{}) interface{} {
	if m == nil {
		return nil
	}
	v := reflect.ValueOf(m)
	if !v.IsValid() {
		return nil
	}
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Map:
		key, ok := coerceTemplateKey(k, v.Type().Key())
		if !ok {
			return nil
		}
		val := v.MapIndex(key)
		if val.IsValid() {
			return val.Interface()
		}
	case reflect.Slice, reflect.Array:
		idx, ok := coerceIntIndex(k)
		if !ok || idx < 0 || idx >= v.Len() {
			return nil
		}
		return v.Index(idx).Interface()
	}
	return nil
}

func coerceTemplateKey(k interface{}, keyType reflect.Type) (reflect.Value, bool) {
	if k == nil || keyType == nil {
		return reflect.Value{}, false
	}
	key := reflect.ValueOf(k)
	if !key.IsValid() {
		return reflect.Value{}, false
	}
	if key.Type().AssignableTo(keyType) {
		return key, true
	}
	if key.Type().ConvertibleTo(keyType) && keyType.Kind() != reflect.String {
		return key.Convert(keyType), true
	}
	switch keyType.Kind() {
	case reflect.String:
		return reflect.ValueOf(fmt.Sprint(k)), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if v, ok := coerceInt64(k); ok {
			return reflect.ValueOf(v).Convert(keyType), true
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if v, ok := coerceUint64(k); ok {
			return reflect.ValueOf(v).Convert(keyType), true
		}
	}
	return reflect.Value{}, false
}

func coerceIntIndex(k interface{}) (int, bool) {
	if v, ok := coerceInt64(k); ok {
		return int(v), true
	}
	return 0, false
}

func coerceInt64(k interface{}) (int64, bool) {
	switch v := k.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		if v > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(v), true
	case float32:
		return int64(v), true
	case float64:
		return int64(v), true
	case string:
		if v == "" {
			return 0, false
		}
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func coerceUint64(k interface{}) (uint64, bool) {
	switch v := k.(type) {
	case int:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int8:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int16:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int32:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case uint:
		return uint64(v), true
	case uint8:
		return uint64(v), true
	case uint16:
		return uint64(v), true
	case uint32:
		return uint64(v), true
	case uint64:
		return v, true
	case float32:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case float64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case string:
		if v == "" {
			return 0, false
		}
		if parsed, err := strconv.ParseUint(v, 10, 64); err == nil {
			return parsed, true
		}
	}
	return 0, false
}

// injectPermsIfMissing afegeix flags de permisos per renderitzar el menú privat quan no s'han passat explícitament.
func injectPermsIfMissing(r *http.Request, data interface{}) interface{} {
	m, ok := data.(map[string]interface{})
	if !ok {
		return data
	}
	effectiveAdmin := false
	if isAdmin, found := effectiveAdminFromContext(r); found {
		effectiveAdmin = isAdmin
	}
	permKeys, permKeysFound := permissionKeysFromContext(r)
	hasKey := func(key string) bool {
		if !permKeysFound {
			return false
		}
		return permKeys[key]
	}
	hasModeracioKey := hasModularModerationKey(hasKey)
	hasManageTerritoryKey := hasModularTerritoryManageKey(hasKey)
	hasViewNivellsKey := hasModularNivellsViewKey(hasKey)
	hasViewMunicipisKey := hasModularMunicipisViewKey(hasKey)
	hasManageEclesiaKey := hasModularEclesiaManageKey(hasKey)
	hasViewEclesiaKey := hasModularEclesiaViewKey(hasKey)
	hasViewConfessionalKey := hasModularConfessionalViewKey(hasKey)
	hasManageDocumentalsKey := hasModularDocumentalsManageKey(hasKey)
	hasViewArxiusKey := hasModularArxiusViewKey(hasKey)
	hasViewLlibresKey := hasModularLlibresViewKey(hasKey)
	if _, found := m["CanManageArxius"]; !found {
		m["CanManageArxius"] = effectiveAdmin || hasManageDocumentalsKey
	}
	if _, found := m["CanManageTerritory"]; !found {
		m["CanManageTerritory"] = effectiveAdmin || hasManageTerritoryKey
	}
	if _, found := m["CanManageEclesia"]; !found {
		m["CanManageEclesia"] = effectiveAdmin || hasManageEclesiaKey
	}
	if _, found := m["CanManageUsers"]; !found {
		m["CanManageUsers"] = effectiveAdmin || hasKey(permKeyAdminUsersManage)
	}
	if _, found := m["CanManagePolicies"]; !found {
		m["CanManagePolicies"] = effectiveAdmin || hasKey(permKeyAdminPoliciesManage)
	}
	if _, found := m["CanViewAdminAudit"]; !found {
		m["CanViewAdminAudit"] = effectiveAdmin || hasKey(permKeyAdminAuditView)
	}
	if _, found := m["CanManageAdminJobs"]; !found {
		m["CanManageAdminJobs"] = effectiveAdmin || hasKey(permKeyAdminJobsManage)
	}
	if _, found := m["CanViewAdminControl"]; !found {
		m["CanViewAdminControl"] = effectiveAdmin ||
			hasKey(permKeyAdminAnalyticsView) ||
			hasKey(permKeyAdminAuditView) ||
			hasKey(permKeyAdminJobsManage) ||
			hasKey(permKeyAdminPlatformSettingsEdit) ||
			hasKey(permKeyAdminMaintenanceManage) ||
			hasKey(permKeyAdminTransparencyManage)
	}
	if _, found := m["CanModerate"]; !found {
		m["CanModerate"] = effectiveAdmin || hasModeracioKey
	}
	if _, found := m["IsAdmin"]; !found {
		m["IsAdmin"] = effectiveAdmin
	}
	if _, found := m["CanViewArxius"]; !found {
		m["CanViewArxius"] = effectiveAdmin || hasViewArxiusKey
	}
	if _, found := m["CanViewHome"]; !found {
		m["CanViewHome"] = effectiveAdmin || hasKey(permKeyHomeView)
	}
	if _, found := m["CanViewMessages"]; !found {
		m["CanViewMessages"] = effectiveAdmin || hasKey(permKeyMessagesView)
	}
	if _, found := m["CanViewSearch"]; !found {
		m["CanViewSearch"] = effectiveAdmin || hasKey(permKeySearchAdvancedView)
	}
	if _, found := m["CanViewRanking"]; !found {
		m["CanViewRanking"] = effectiveAdmin || hasKey(permKeyRankingView)
	}
	if _, found := m["CanViewPersones"]; !found {
		m["CanViewPersones"] = effectiveAdmin || hasKey(permKeyPersonsView) || hasKey(permKeyPersonesCreate)
	}
	if _, found := m["CanCreatePerson"]; !found {
		m["CanCreatePerson"] = effectiveAdmin || hasKey(permKeyPersonesCreate)
	}
	if _, found := m["CanViewCognoms"]; !found {
		m["CanViewCognoms"] = effectiveAdmin || hasKey(permKeyCognomsView)
	}
	if _, found := m["CanViewMedia"]; !found {
		m["CanViewMedia"] = effectiveAdmin || hasKey(permKeyMediaView)
	}
	if _, found := m["CanViewEvents"]; !found {
		m["CanViewEvents"] = effectiveAdmin || hasKey(permKeyEventsView)
	}
	if _, found := m["CanViewLlibres"]; !found {
		m["CanViewLlibres"] = effectiveAdmin || hasViewLlibresKey
	}
	if _, found := m["CanViewDocumentals"]; !found {
		m["CanViewDocumentals"] = m["CanViewArxius"].(bool) || m["CanViewLlibres"].(bool)
	}
	if _, found := m["CanViewImportTemplates"]; !found {
		m["CanViewImportTemplates"] = effectiveAdmin || hasKey(permKeyImportTemplatesView) ||
			hasKey(permKeyDocumentalsLlibresImportCSV) || hasKey(permKeyDocumentalsLlibresImport)
	}
	if _, found := m["CanImportTemplates"]; !found {
		m["CanImportTemplates"] = effectiveAdmin ||
			hasKey(permKeyDocumentalsLlibresImportCSV) || hasKey(permKeyDocumentalsLlibresImport)
	}
	if _, found := m["CanIndexRegistres"]; !found {
		m["CanIndexRegistres"] = effectiveAdmin ||
			hasKey(permKeyDocumentalsRegistresEdit) || hasKey(permKeyDocumentalsRegistresEditInline)
	}
	if _, found := m["CanBulkIndex"]; !found {
		m["CanBulkIndex"] = effectiveAdmin || hasKey(permKeyDocumentalsLlibresBulkIndex)
	}
	if _, found := m["CanViewNivells"]; !found {
		m["CanViewNivells"] = effectiveAdmin || hasViewNivellsKey
	}
	if _, found := m["CanViewMunicipis"]; !found {
		m["CanViewMunicipis"] = effectiveAdmin || hasViewMunicipisKey
	}
	if _, found := m["CanViewEcles"]; !found {
		m["CanViewEcles"] = effectiveAdmin || hasViewEclesiaKey
	}
	if _, found := m["CanViewConfessional"]; !found {
		m["CanViewConfessional"] = effectiveAdmin || hasViewConfessionalKey
	}
	if _, found := m["CanViewConfessionalReligions"]; !found {
		m["CanViewConfessionalReligions"] = effectiveAdmin || hasModularConfessionalReligionsViewKey(hasKey)
	}
	if _, found := m["CanViewConfessionalModels"]; !found {
		m["CanViewConfessionalModels"] = effectiveAdmin || hasModularConfessionalModelsViewKey(hasKey)
	}
	if _, found := m["CanViewConfessionalNivells"]; !found {
		m["CanViewConfessionalNivells"] = effectiveAdmin || hasModularConfessionalNivellsViewKey(hasKey)
	}
	if _, found := m["CanViewConfessionalEntitats"]; !found {
		m["CanViewConfessionalEntitats"] = effectiveAdmin || hasModularConfessionalEntitatsViewKey(hasKey)
	}
	if _, found := m["CanViewConfessionalRelacionsEntitats"]; !found {
		m["CanViewConfessionalRelacionsEntitats"] = effectiveAdmin || hasModularConfessionalRelacionsEntitatsViewKey(hasKey)
	}
	if _, found := m["CanViewConfessionalMunicipisEntitats"]; !found {
		m["CanViewConfessionalMunicipisEntitats"] = effectiveAdmin || hasModularConfessionalMunicipisEntitatsViewKey(hasKey)
	}
	if _, found := m["CanViewConfessionalDiagnostic"]; !found {
		m["CanViewConfessionalDiagnostic"] = effectiveAdmin || hasModularConfessionalDiagnosticViewKey(hasKey)
	}
	if _, found := m["ShowLegacyEclesMenu"]; !found {
		hasLegacyEcles := boolTemplateFlag(m, "CanViewEcles") || boolTemplateFlag(m, "CanManageEclesia")
		hasConfessional := boolTemplateFlag(m, "CanViewConfessionalEntitats") ||
			boolTemplateFlag(m, "CanViewConfessionalRelacionsEntitats") ||
			boolTemplateFlag(m, "CanViewConfessionalMunicipisEntitats") ||
			boolTemplateFlag(m, "CanViewConfessionalDiagnostic")
		m["ShowLegacyEclesMenu"] = hasLegacyEcles && !hasConfessional
	}
	if _, found := m["CanViewTerritory"]; !found {
		m["CanViewTerritory"] = boolTemplateFlag(m, "CanViewNivells") || boolTemplateFlag(m, "CanViewMunicipis") || boolTemplateFlag(m, "CanViewEcles")
	}
	if _, found := m["UnreadMessagesCount"]; !found {
		if count, ok := unreadMessagesCountFromContext(r); ok {
			m["UnreadMessagesCount"] = count
		}
	}
	return m
}

func hasModularModerationKey(hasKey func(string) bool) bool {
	if hasKey == nil {
		return false
	}
	return hasKey(permKeyModeracioMassiva) ||
		hasKey(permKeyPersonesModerate) ||
		hasKey(permKeyCognomsModerate) ||
		hasKey(permKeyMediaModerate) ||
		hasKey(permKeyEventsModerate) ||
		hasKey(permKeyAdminExternalLinksModerate) ||
		hasKey(permKeyTerritoriMunicipisMapesModerate) ||
		hasKey(permKeyTerritoriMunicipisHistoriaModerate) ||
		hasKey(permKeyTerritoriMunicipisAnecdotesModerate) ||
		hasKey(permKeyTerritoriMunicipisEdit) ||
		hasKey(permKeyTerritoriNivellsEdit) ||
		hasKey(permKeyTerritoriEclesEdit) ||
		hasKey(permKeyTerritoriConfessionalReligionsEdit) ||
		hasKey(permKeyTerritoriConfessionalModelsEdit) ||
		hasKey(permKeyTerritoriConfessionalNivellsEdit) ||
		hasKey(permKeyTerritoriConfessionalEntitatsEdit) ||
		hasKey(permKeyTerritoriConfessionalRelacionsEntitatsEdit) ||
		hasKey(permKeyTerritoriConfessionalMunicipisEntitatsEdit) ||
		hasKey(permKeyDocumentalsArxiusEdit) ||
		hasKey(permKeyDocumentalsLlibresEdit) ||
		hasKey(permKeyDocumentalsRegistresEdit)
}

func hasModularDocumentalsManageKey(hasKey func(string) bool) bool {
	if hasKey == nil {
		return false
	}
	return hasKey(permKeyDocumentalsArxiusCreate) ||
		hasKey(permKeyDocumentalsArxiusEdit) ||
		hasKey(permKeyDocumentalsArxiusDelete) ||
		hasKey(permKeyDocumentalsArxiusImport) ||
		hasKey(permKeyDocumentalsArxiusExport) ||
		hasKey(permKeyDocumentalsLlibresCreate) ||
		hasKey(permKeyDocumentalsLlibresEdit) ||
		hasKey(permKeyDocumentalsLlibresDelete) ||
		hasKey(permKeyDocumentalsLlibresImport) ||
		hasKey(permKeyDocumentalsLlibresExport) ||
		hasKey(permKeyDocumentalsLlibresExportCSV) ||
		hasKey(permKeyDocumentalsLlibresImportCSV) ||
		hasKey(permKeyDocumentalsLlibresBulkIndex) ||
		hasKey(permKeyDocumentalsLlibresMarkIndexed) ||
		hasKey(permKeyDocumentalsLlibresRecalcIndex) ||
		hasKey(permKeyDocumentalsRegistresEdit) ||
		hasKey(permKeyDocumentalsRegistresEditInline) ||
		hasKey(permKeyDocumentalsRegistresLinkPerson) ||
		hasKey(permKeyDocumentalsRegistresConvertToPerson)
}

func hasModularArxiusViewKey(hasKey func(string) bool) bool {
	if hasKey == nil {
		return false
	}
	return hasKey(permKeyDocumentalsArxiusView) || hasModularDocumentalsManageKey(func(key string) bool {
		switch key {
		case permKeyDocumentalsArxiusCreate, permKeyDocumentalsArxiusEdit, permKeyDocumentalsArxiusDelete,
			permKeyDocumentalsArxiusImport, permKeyDocumentalsArxiusExport:
			return hasKey(key)
		default:
			return false
		}
	})
}

func hasModularLlibresViewKey(hasKey func(string) bool) bool {
	if hasKey == nil {
		return false
	}
	return hasKey(permKeyDocumentalsLlibresView) ||
		hasKey(permKeyDocumentalsLlibresCreate) ||
		hasKey(permKeyDocumentalsLlibresEdit) ||
		hasKey(permKeyDocumentalsLlibresDelete) ||
		hasKey(permKeyDocumentalsLlibresImport) ||
		hasKey(permKeyDocumentalsLlibresExport) ||
		hasKey(permKeyDocumentalsLlibresExportCSV) ||
		hasKey(permKeyDocumentalsLlibresImportCSV) ||
		hasKey(permKeyDocumentalsLlibresViewRegistres) ||
		hasKey(permKeyDocumentalsLlibresBulkIndex) ||
		hasKey(permKeyDocumentalsLlibresMarkIndexed) ||
		hasKey(permKeyDocumentalsLlibresRecalcIndex) ||
		hasKey(permKeyDocumentalsRegistresEdit) ||
		hasKey(permKeyDocumentalsRegistresEditInline) ||
		hasKey(permKeyDocumentalsRegistresLinkPerson) ||
		hasKey(permKeyDocumentalsRegistresConvertToPerson)
}

func hasModularTerritoryManageKey(hasKey func(string) bool) bool {
	if hasKey == nil {
		return false
	}
	return hasKey(permKeyTerritoriPaisosCreate) ||
		hasKey(permKeyTerritoriPaisosEdit) ||
		hasKey(permKeyTerritoriNivellsCreate) ||
		hasKey(permKeyTerritoriNivellsEdit) ||
		hasKey(permKeyTerritoriNivellsRebuild) ||
		hasKey(permKeyTerritoriMunicipisCreate) ||
		hasKey(permKeyTerritoriMunicipisEdit)
}

func hasModularNivellsViewKey(hasKey func(string) bool) bool {
	if hasKey == nil {
		return false
	}
	return hasKey(permKeyTerritoriNivellsView) ||
		hasKey(permKeyTerritoriNivellsCreate) ||
		hasKey(permKeyTerritoriNivellsEdit) ||
		hasKey(permKeyTerritoriNivellsRebuild)
}

func hasModularMunicipisViewKey(hasKey func(string) bool) bool {
	if hasKey == nil {
		return false
	}
	return hasKey(permKeyTerritoriMunicipisView) ||
		hasKey(permKeyTerritoriMunicipisCreate) ||
		hasKey(permKeyTerritoriMunicipisEdit) ||
		hasKey(permKeyTerritoriMunicipisMapesView) ||
		hasKey(permKeyTerritoriMunicipisMapesCreate) ||
		hasKey(permKeyTerritoriMunicipisMapesEdit) ||
		hasKey(permKeyTerritoriMunicipisMapesSubmit) ||
		hasKey(permKeyTerritoriMunicipisMapesModerate) ||
		hasKey(permKeyTerritoriMunicipisHistoriaCreate) ||
		hasKey(permKeyTerritoriMunicipisHistoriaEdit) ||
		hasKey(permKeyTerritoriMunicipisHistoriaSubmit) ||
		hasKey(permKeyTerritoriMunicipisHistoriaModerate) ||
		hasKey(permKeyTerritoriMunicipisAnecdotesCreate) ||
		hasKey(permKeyTerritoriMunicipisAnecdotesEdit) ||
		hasKey(permKeyTerritoriMunicipisAnecdotesSubmit) ||
		hasKey(permKeyTerritoriMunicipisAnecdotesComment) ||
		hasKey(permKeyTerritoriMunicipisAnecdotesModerate)
}

func hasModularEclesiaManageKey(hasKey func(string) bool) bool {
	if hasKey == nil {
		return false
	}
	return hasKey(permKeyTerritoriEclesCreate) ||
		hasKey(permKeyTerritoriEclesEdit) ||
		hasKey(permKeyTerritoriEclesImportJSON) ||
		hasKey(permKeyAdminEclesImport)
}

func hasModularEclesiaViewKey(hasKey func(string) bool) bool {
	if hasKey == nil {
		return false
	}
	return hasKey(permKeyTerritoriEclesView) || hasModularEclesiaManageKey(hasKey)
}

func hasModularConfessionalManageKey(hasKey func(string) bool) bool {
	if hasKey == nil {
		return false
	}
	return hasKey(permKeyTerritoriConfessionalReligionsCreate) ||
		hasKey(permKeyTerritoriConfessionalReligionsEdit) ||
		hasKey(permKeyTerritoriConfessionalReligionsDelete) ||
		hasKey(permKeyTerritoriConfessionalModelsCreate) ||
		hasKey(permKeyTerritoriConfessionalModelsEdit) ||
		hasKey(permKeyTerritoriConfessionalModelsDelete) ||
		hasKey(permKeyTerritoriConfessionalNivellsCreate) ||
		hasKey(permKeyTerritoriConfessionalNivellsEdit) ||
		hasKey(permKeyTerritoriConfessionalNivellsDelete) ||
		hasKey(permKeyTerritoriConfessionalEntitatsCreate) ||
		hasKey(permKeyTerritoriConfessionalEntitatsEdit) ||
		hasKey(permKeyTerritoriConfessionalEntitatsDelete) ||
		hasKey(permKeyTerritoriConfessionalRelacionsEntitatsCreate) ||
		hasKey(permKeyTerritoriConfessionalRelacionsEntitatsEdit) ||
		hasKey(permKeyTerritoriConfessionalRelacionsEntitatsDelete) ||
		hasKey(permKeyTerritoriConfessionalMunicipisEntitatsCreate) ||
		hasKey(permKeyTerritoriConfessionalMunicipisEntitatsEdit) ||
		hasKey(permKeyTerritoriConfessionalMunicipisEntitatsDelete)
}

func hasModularConfessionalViewKey(hasKey func(string) bool) bool {
	if hasKey == nil {
		return false
	}
	return hasModularConfessionalReligionsViewKey(hasKey) ||
		hasModularConfessionalModelsViewKey(hasKey) ||
		hasModularConfessionalNivellsViewKey(hasKey) ||
		hasModularConfessionalEntitatsViewKey(hasKey) ||
		hasModularConfessionalRelacionsEntitatsViewKey(hasKey) ||
		hasModularConfessionalMunicipisEntitatsViewKey(hasKey) ||
		hasModularConfessionalDiagnosticViewKey(hasKey)
}

func hasModularConfessionalReligionsViewKey(hasKey func(string) bool) bool {
	return hasKey != nil && (hasKey(permKeyTerritoriConfessionalReligionsView) ||
		hasKey(permKeyTerritoriConfessionalReligionsCreate) ||
		hasKey(permKeyTerritoriConfessionalReligionsEdit) ||
		hasKey(permKeyTerritoriConfessionalReligionsDelete))
}

func hasModularConfessionalModelsViewKey(hasKey func(string) bool) bool {
	return hasKey != nil && (hasKey(permKeyTerritoriConfessionalModelsView) ||
		hasKey(permKeyTerritoriConfessionalModelsCreate) ||
		hasKey(permKeyTerritoriConfessionalModelsEdit) ||
		hasKey(permKeyTerritoriConfessionalModelsDelete))
}

func hasModularConfessionalNivellsViewKey(hasKey func(string) bool) bool {
	return hasKey != nil && (hasKey(permKeyTerritoriConfessionalNivellsView) ||
		hasKey(permKeyTerritoriConfessionalNivellsCreate) ||
		hasKey(permKeyTerritoriConfessionalNivellsEdit) ||
		hasKey(permKeyTerritoriConfessionalNivellsDelete))
}

func hasModularConfessionalEntitatsViewKey(hasKey func(string) bool) bool {
	return hasKey != nil && (hasKey(permKeyTerritoriConfessionalEntitatsView) ||
		hasKey(permKeyTerritoriConfessionalEntitatsCreate) ||
		hasKey(permKeyTerritoriConfessionalEntitatsEdit) ||
		hasKey(permKeyTerritoriConfessionalEntitatsDelete))
}

func hasModularConfessionalRelacionsEntitatsViewKey(hasKey func(string) bool) bool {
	return hasKey != nil && (hasKey(permKeyTerritoriConfessionalRelacionsEntitatsView) ||
		hasKey(permKeyTerritoriConfessionalRelacionsEntitatsCreate) ||
		hasKey(permKeyTerritoriConfessionalRelacionsEntitatsEdit) ||
		hasKey(permKeyTerritoriConfessionalRelacionsEntitatsDelete))
}

func hasModularConfessionalMunicipisEntitatsViewKey(hasKey func(string) bool) bool {
	return hasKey != nil && (hasKey(permKeyTerritoriConfessionalMunicipisEntitatsView) ||
		hasKey(permKeyTerritoriConfessionalMunicipisEntitatsCreate) ||
		hasKey(permKeyTerritoriConfessionalMunicipisEntitatsEdit) ||
		hasKey(permKeyTerritoriConfessionalMunicipisEntitatsDelete))
}

func hasModularConfessionalDiagnosticViewKey(hasKey func(string) bool) bool {
	return hasKey != nil && hasKey(permKeyTerritoriConfessionalDiagnosticView)
}
