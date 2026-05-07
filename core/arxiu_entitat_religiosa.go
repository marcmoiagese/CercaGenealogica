package core

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type arxiuEntitatReligiosaType struct {
	Code string
	Key  string
}

type arxiuEntitatReligiosaFormData struct {
	Relacio                *db.ArxiuEntitatReligiosa
	Arxius                 []db.ArxiuWithCount
	Entitats               []db.EntitatReligiosa
	RelacioTypes           []arxiuEntitatReligiosaType
	ReligionCatalogLabels  map[string]string
	LevelCatalogLabels     map[string]string
	ArxiuLabels            map[int]string
	EntitatOptionLabels    map[int]string
	TypeLabels             map[string]string
	Error                  string
	ReturnURL              string
	IsNew                  bool
	FromArxiuID            int
	FromEntitatReligiosaID int
	User                   *db.User
}

var arxiuEntitatReligiosaTypes = []arxiuEntitatReligiosaType{
	{Code: "arxiu_institucional", Key: "confessional.archive_relation.type.arxiu_institucional"},
	{Code: "custodia_documentacio", Key: "confessional.archive_relation.type.custodia_documentacio"},
	{Code: "productor_fons", Key: "confessional.archive_relation.type.productor_fons"},
	{Code: "fons_documental", Key: "confessional.archive_relation.type.fons_documental"},
	{Code: "diposit_documental", Key: "confessional.archive_relation.type.diposit_documental"},
	{Code: "jurisdiccio_eclesiastica", Key: "confessional.archive_relation.type.jurisdiccio_eclesiastica"},
	{Code: "context_religios", Key: "confessional.archive_relation.type.context_religios"},
	{Code: "altres", Key: "confessional.archive_relation.type.altres"},
}

func arxiuEntitatReligiosaTypeValid(code string) bool {
	code = strings.TrimSpace(code)
	for _, item := range arxiuEntitatReligiosaTypes {
		if item.Code == code {
			return true
		}
	}
	return false
}

func arxiuEntitatReligiosaTypeLabels(lang string) map[string]string {
	labels := map[string]string{}
	for _, item := range arxiuEntitatReligiosaTypes {
		labels[item.Code] = T(lang, item.Key)
	}
	return labels
}

func (a *App) AdminNewArxiuEntitatReligiosaFromArxiu(w http.ResponseWriter, r *http.Request) {
	arxiuID := extractIDBeforeSegment(r.URL.Path, "entitats-religioses")
	if arxiuID <= 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveArxiuTarget(arxiuID)
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriConfessionalArxiusEntitatsCreate, target)
	if !ok {
		return
	}
	rel := &db.ArxiuEntitatReligiosa{
		ArxiuID:        arxiuID,
		TipusRelacio:   "arxiu_institucional",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}
	a.renderArxiuEntitatReligiosaForm(w, r, user, rel, true, arxiuID, 0, "", strings.TrimSpace(r.URL.Query().Get("return_to")))
}

func (a *App) AdminNewArxiuEntitatReligiosaFromEntitat(w http.ResponseWriter, r *http.Request) {
	entitatID := extractIDBeforeSegment(r.URL.Path, "arxius")
	if entitatID <= 0 {
		http.NotFound(w, r)
		return
	}
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriConfessionalArxiusEntitatsCreate, PermissionTarget{})
	if !ok {
		return
	}
	rel := &db.ArxiuEntitatReligiosa{
		EntitatReligiosaID: entitatID,
		TipusRelacio:       "arxiu_institucional",
		Estat:              "actiu",
		ModeracioEstat:     "pendent",
	}
	a.renderArxiuEntitatReligiosaForm(w, r, user, rel, true, 0, entitatID, "", strings.TrimSpace(r.URL.Query().Get("return_to")))
}

func (a *App) AdminEditArxiuEntitatReligiosa(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	if id <= 0 {
		http.NotFound(w, r)
		return
	}
	rel, err := a.DB.GetArxiuEntitatReligiosa(id)
	if err != nil || rel == nil {
		http.NotFound(w, r)
		return
	}
	target := a.resolveArxiuTarget(rel.ArxiuID)
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriConfessionalArxiusEntitatsEdit, target)
	if !ok {
		return
	}
	a.renderArxiuEntitatReligiosaForm(w, r, user, rel, false, rel.ArxiuID, rel.EntitatReligiosaID, "", strings.TrimSpace(r.URL.Query().Get("return_to")))
}

func (a *App) AdminSaveArxiuEntitatReligiosa(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/documentals/arxius", http.StatusSeeOther)
		return
	}
	id, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("id")))
	rel := parseArxiuEntitatReligiosaForm(r)
	rel.ID = id
	target := a.resolveArxiuTarget(rel.ArxiuID)
	perm := permKeyTerritoriConfessionalArxiusEntitatsCreate
	if id > 0 {
		perm = permKeyTerritoriConfessionalArxiusEntitatsEdit
	}
	user, ok := a.requirePermissionKey(w, r, perm, target)
	if !ok {
		return
	}
	returnURL := strings.TrimSpace(r.FormValue("return_to"))
	fromArxiuID := parsePositiveIntDefault(r.FormValue("from_arxiu_id"), 0, 0, 1000000000)
	fromEntitatID := parsePositiveIntDefault(r.FormValue("from_entitat_religiosa_id"), 0, 0, 1000000000)
	if errMsg := a.validateArxiuEntitatReligiosa(rel); errMsg != "" {
		a.renderArxiuEntitatReligiosaForm(w, r, user, rel, id == 0, fromArxiuID, fromEntitatID, errMsg, returnURL)
		return
	}
	author := sqlNullIntFromInt(user.ID)
	rel.UpdatedBy = author
	rel.ModeracioEstat = "pendent"
	rel.ModeracioMotiu = ""
	rel.ModeratedBy = sql.NullInt64{}
	rel.ModeratedAt = sql.NullTime{}
	if id == 0 {
		rel.CreatedBy = author
	} else if current, err := a.DB.GetArxiuEntitatReligiosa(id); err == nil && current != nil {
		if current.ModeracioEstat == "publicat" && !a.canModerateModular(user) {
			a.renderArxiuEntitatReligiosaForm(w, r, user, rel, false, fromArxiuID, fromEntitatID, T(ResolveLangForUser(r, user.PreferredLang), "confessional.archive_relation.error.published_edit_restricted"), returnURL)
			return
		}
		rel.CreatedBy = current.CreatedBy
	} else {
		http.NotFound(w, r)
		return
	}
	savedID, err := a.DB.SaveArxiuEntitatReligiosa(rel)
	if err != nil {
		a.renderArxiuEntitatReligiosaForm(w, r, user, rel, id == 0, fromArxiuID, fromEntitatID, T(ResolveLangForUser(r, user.PreferredLang), "confessional.archive_relation.error.save"), returnURL)
		return
	}
	if id == 0 {
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleArxiuCreate, "crear", "arxiu_entitat_religiosa", &savedID, "pendent", nil, "")
	}
	if returnURL == "" {
		returnURL = fmt.Sprintf("/documentals/arxius/%d?notice=relation-pending", rel.ArxiuID)
	}
	http.Redirect(w, r, returnURL, http.StatusSeeOther)
}

func (a *App) AdminDeleteArxiuEntitatReligiosa(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	rel, err := a.DB.GetArxiuEntitatReligiosa(id)
	if err != nil || rel == nil {
		http.NotFound(w, r)
		return
	}
	target := a.resolveArxiuTarget(rel.ArxiuID)
	if _, ok := a.requirePermissionKey(w, r, permKeyTerritoriConfessionalArxiusEntitatsDelete, target); !ok {
		return
	}
	_ = a.DB.DeleteArxiuEntitatReligiosa(id)
	http.Redirect(w, r, fmt.Sprintf("/documentals/arxius/%d", rel.ArxiuID), http.StatusSeeOther)
}

func parseArxiuEntitatReligiosaForm(r *http.Request) *db.ArxiuEntitatReligiosa {
	_ = r.ParseForm()
	return &db.ArxiuEntitatReligiosa{
		ArxiuID:            parsePositiveIntDefault(r.FormValue("arxiu_id"), 0, 0, 1000000000),
		EntitatReligiosaID: parsePositiveIntDefault(r.FormValue("entitat_religiosa_id"), 0, 0, 1000000000),
		TipusRelacio:       strings.TrimSpace(r.FormValue("tipus_relacio")),
		AnyInici:           sqlNullInt(r.FormValue("any_inici")),
		AnyFi:              sqlNullInt(r.FormValue("any_fi")),
		Observacions:       strings.TrimSpace(r.FormValue("observacions")),
		Estat:              normalizeArxiuEntitatReligiosaEstat(r.FormValue("estat")),
	}
}

func (a *App) validateArxiuEntitatReligiosa(rel *db.ArxiuEntitatReligiosa) string {
	if rel == nil || rel.ArxiuID <= 0 {
		return "Cal indicar l'arxiu."
	}
	if arxiu, err := a.DB.GetArxiu(rel.ArxiuID); err != nil || arxiu == nil {
		return "L'arxiu indicat no existeix."
	}
	if rel.EntitatReligiosaID <= 0 {
		return "Cal indicar l'entitat religiosa."
	}
	entitat, err := a.DB.GetEntitatReligiosa(rel.EntitatReligiosaID)
	if err != nil || entitat == nil {
		return "L'entitat religiosa indicada no existeix."
	}
	if entitat.ModeracioEstat != "publicat" || entitat.Estat != "actiu" {
		return "L'entitat religiosa ha d'estar publicada."
	}
	if !arxiuEntitatReligiosaTypeValid(rel.TipusRelacio) {
		return "El tipus de relacio no es valid."
	}
	if rel.AnyInici.Valid && rel.AnyFi.Valid && rel.AnyInici.Int64 > rel.AnyFi.Int64 {
		return "L'any inicial no pot ser posterior a l'any final."
	}
	existing, err := a.DB.ListArxiuEntitatsReligioses(rel.ArxiuID, rel.EntitatReligiosaID, "")
	if err != nil {
		return "No s'ha pogut validar la relacio."
	}
	for _, row := range existing {
		if row.ID == rel.ID || row.ModeracioEstat == "rebutjat" {
			continue
		}
		if row.TipusRelacio == rel.TipusRelacio && nullIntEqual(row.AnyInici, rel.AnyInici) && nullIntEqual(row.AnyFi, rel.AnyFi) {
			return "Ja existeix una relacio equivalent."
		}
	}
	return ""
}

func (a *App) renderArxiuEntitatReligiosaForm(w http.ResponseWriter, r *http.Request, user *db.User, rel *db.ArxiuEntitatReligiosa, isNew bool, fromArxiuID, fromEntitatID int, errMsg, returnURL string) {
	lang := ResolveLangForUser(r, user.PreferredLang)
	arxius, _ := a.DB.ListArxius(db.ArxiuFilter{Status: "publicat", Limit: -1})
	allEntitats, _ := a.DB.ListEntitatsReligioses()
	entitats := publishedEntitatsReligioses(allEntitats)
	religionLabels := confessionalReligionCatalogLabels(lang)
	levelLabels := confessionalLevelCatalogLabels(lang)
	arxiuLabels := map[int]string{}
	for _, arxiu := range arxius {
		arxiuLabels[arxiu.ID] = arxiu.Nom
	}
	entitatOptionLabels := arxiuEntitatReligiosaEntitatOptionLabels(entitats, religionLabels, levelLabels)
	RenderPrivateTemplate(w, r, "admin-arxiu-entitat-religiosa-form.html", arxiuEntitatReligiosaFormData{
		Relacio:                rel,
		Arxius:                 arxius,
		Entitats:               entitats,
		RelacioTypes:           arxiuEntitatReligiosaTypes,
		ReligionCatalogLabels:  religionLabels,
		LevelCatalogLabels:     levelLabels,
		ArxiuLabels:            arxiuLabels,
		EntitatOptionLabels:    entitatOptionLabels,
		TypeLabels:             arxiuEntitatReligiosaTypeLabels(lang),
		Error:                  errMsg,
		ReturnURL:              returnURL,
		IsNew:                  isNew,
		FromArxiuID:            fromArxiuID,
		FromEntitatReligiosaID: fromEntitatID,
		User:                   user,
	})
}

func arxiuEntitatReligiosaEntitatOptionLabels(entitats []db.EntitatReligiosa, religionLabels, levelLabels map[string]string) map[int]string {
	labels := map[int]string{}
	for _, entitat := range entitats {
		parts := []string{strings.TrimSpace(entitat.Nom)}
		context := strings.TrimSpace(strings.Join([]string{religionLabels[entitat.ReligioConfessioCodi], levelLabels[entitat.NivellConfessionalCodi]}, " - "))
		if context != "" {
			parts = append(parts, context)
		}
		labels[entitat.ID] = strings.Join(parts, " -- ")
	}
	return labels
}

func entitatsReligiosesByID(entitats []db.EntitatReligiosa) map[int]db.EntitatReligiosa {
	out := map[int]db.EntitatReligiosa{}
	for _, entitat := range entitats {
		out[entitat.ID] = entitat
	}
	return out
}

func arxiuLabels(arxius []db.ArxiuWithCount) map[int]string {
	labels := map[int]string{}
	for _, arxiu := range arxius {
		labels[arxiu.ID] = arxiu.Nom
	}
	return labels
}

func normalizeArxiuEntitatReligiosaEstat(raw string) string {
	if strings.TrimSpace(raw) == "historic" {
		return "historic"
	}
	return "actiu"
}

func nullIntEqual(a, b sql.NullInt64) bool {
	if a.Valid != b.Valid {
		return false
	}
	if !a.Valid {
		return true
	}
	return a.Int64 == b.Int64
}

func extractIDBeforeSegment(path, segment string) int {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, part := range parts {
		if part == segment && i > 0 {
			id, _ := strconv.Atoi(parts[i-1])
			return id
		}
	}
	return 0
}
