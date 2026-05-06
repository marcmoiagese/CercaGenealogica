package core

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type confessionalFormData struct {
	Kind      string
	IsNew     bool
	ReturnURL string
	Error     string
	Religio   *db.ReligioConfessio
	Model     *db.ModelConfessional
	Nivell    *db.NivellConfessional
	Entitat   *db.EntitatReligiosa
	Relacio   *db.MunicipiEntitatReligiosa
	RelEnt    *db.EntitatReligiosaRelacio
}

type confessionalSection struct {
	Kind       string
	Slug       string
	Title      string
	NewLabel   string
	ViewPerm   string
	CreatePerm string
	EditPerm   string
	DeletePerm string
}

var confessionalSections = map[string]confessionalSection{
	"religio": {Kind: "religio", Slug: "religions", Title: "Religions/confessions", NewLabel: "Nova religio/confessio", ViewPerm: permKeyTerritoriConfessionalReligionsView, CreatePerm: permKeyTerritoriConfessionalReligionsCreate, EditPerm: permKeyTerritoriConfessionalReligionsEdit, DeletePerm: permKeyTerritoriConfessionalReligionsDelete},
	"model":   {Kind: "model", Slug: "models", Title: "Models confessionals", NewLabel: "Nou model", ViewPerm: permKeyTerritoriConfessionalModelsView, CreatePerm: permKeyTerritoriConfessionalModelsCreate, EditPerm: permKeyTerritoriConfessionalModelsEdit, DeletePerm: permKeyTerritoriConfessionalModelsDelete},
	"nivell":  {Kind: "nivell", Slug: "nivells", Title: "Nivells confessionals", NewLabel: "Nou nivell", ViewPerm: permKeyTerritoriConfessionalNivellsView, CreatePerm: permKeyTerritoriConfessionalNivellsCreate, EditPerm: permKeyTerritoriConfessionalNivellsEdit, DeletePerm: permKeyTerritoriConfessionalNivellsDelete},
	"entitat": {Kind: "entitat", Slug: "entitats", Title: "Entitats religioses", NewLabel: "Nova entitat", ViewPerm: permKeyTerritoriConfessionalEntitatsView, CreatePerm: permKeyTerritoriConfessionalEntitatsCreate, EditPerm: permKeyTerritoriConfessionalEntitatsEdit, DeletePerm: permKeyTerritoriConfessionalEntitatsDelete},
	"rel_ent": {Kind: "rel_ent", Slug: "relacions-entitats", Title: "Relacions entre entitats religioses", NewLabel: "Nova relacio entre entitats", ViewPerm: permKeyTerritoriConfessionalRelacionsEntitatsView, CreatePerm: permKeyTerritoriConfessionalRelacionsEntitatsCreate, EditPerm: permKeyTerritoriConfessionalRelacionsEntitatsEdit, DeletePerm: permKeyTerritoriConfessionalRelacionsEntitatsDelete},
	"relacio": {Kind: "relacio", Slug: "municipis-entitats", Title: "Relacions municipi/nucli - entitat religiosa", NewLabel: "Nova relacio territorial", ViewPerm: permKeyTerritoriConfessionalMunicipisEntitatsView, CreatePerm: permKeyTerritoriConfessionalMunicipisEntitatsCreate, EditPerm: permKeyTerritoriConfessionalMunicipisEntitatsEdit, DeletePerm: permKeyTerritoriConfessionalMunicipisEntitatsDelete},
}

func (a *App) AdminConfessionalList(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/confessional/entitats", http.StatusSeeOther)
}

func (a *App) AdminConfessionalSectionList(w http.ResponseWriter, r *http.Request) {
	section, okSection := confessionalSectionFromPath(r.URL.Path)
	if !okSection {
		http.NotFound(w, r)
		return
	}
	if confessionalCatalogSection(section.Kind) {
		http.Redirect(w, r, "/confessional/entitats", http.StatusSeeOther)
		return
	}
	user, ok := a.requirePermissionKey(w, r, section.ViewPerm, PermissionTarget{})
	if !ok {
		return
	}
	var religions []db.ReligioConfessio
	var models []db.ModelConfessional
	var nivells []db.NivellConfessional
	var entitats []db.EntitatReligiosa
	var relacions []db.MunicipiEntitatReligiosa
	var relEntitats []db.EntitatReligiosaRelacio
	var municipis []db.MunicipiRow
	var paisos []db.Pais
	switch section.Kind {
	case "model":
		religions, _ = a.DB.ListReligioConfessions()
		models, _ = a.DB.ListModelsConfessionals()
		paisos, _ = a.DB.ListPaisos()
	case "entitat":
		allEntitats, _ := a.DB.ListEntitatsReligioses()
		entitats = publishedEntitatsReligioses(allEntitats)
	case "rel_ent":
		allEntitats, _ := a.DB.ListEntitatsReligioses()
		allRelEntitats, _ := a.DB.ListEntitatReligiosaRelacions()
		entitats = publishedEntitatsReligioses(allEntitats)
		relEntitats = publishedEntitatReligiosaRelacions(allRelEntitats)
	case "relacio":
		allEntitats, _ := a.DB.ListEntitatsReligioses()
		allRelacions, _ := a.DB.ListMunicipiEntitatsReligioses(0)
		entitats = publishedEntitatsReligioses(allEntitats)
		relacions = publishedMunicipiEntitatsReligioses(allRelacions)
		municipis, _ = a.DB.ListMunicipis(db.MunicipiFilter{})
	}
	canCreate := a.HasPermission(user.ID, section.CreatePerm, PermissionTarget{})
	canEdit := a.HasPermission(user.ID, section.EditPerm, PermissionTarget{})
	canDelete := a.HasPermission(user.ID, section.DeletePerm, PermissionTarget{})
	lang := ResolveLangForUser(r, user.PreferredLang)
	RenderPrivateTemplate(w, r, "admin-confessional-list.html", map[string]interface{}{
		"Section":               section,
		"Religions":             religions,
		"Models":                models,
		"Nivells":               nivells,
		"Entitats":              entitats,
		"Relacions":             relacions,
		"RelEntitats":           relEntitats,
		"Municipis":             municipis,
		"Paisos":                paisos,
		"CanCreate":             canCreate,
		"CanEdit":               canEdit,
		"CanDelete":             canDelete,
		"Notice":                strings.TrimSpace(r.URL.Query().Get("notice")),
		"Error":                 strings.TrimSpace(r.URL.Query().Get("error")),
		"ReligionLabels":        religioLabels(religions),
		"ReligionCatalogLabels": confessionalReligionCatalogLabels(lang),
		"ModelLabels":           modelLabels(models),
		"NivellLabels":          nivellConfessionalLabels(nivells),
		"LevelCatalogLabels":    confessionalLevelCatalogLabels(lang),
		"EntitatLabels":         entitatReligiosaLabels(entitats),
		"MunicipiLabels":        municipiLabels(municipis),
		"PaisLabels":            paisLabels(paisos),
		"CanManageArxius":       a.canManageAnyDocumentalsModular(user),
		"User":                  user,
	})
}

func (a *App) AdminNewConfessional(w http.ResponseWriter, r *http.Request) {
	section, okSection := confessionalSectionFromPath(r.URL.Path)
	if !okSection {
		kind := confessionalKind(r.URL.Query().Get("kind"))
		section, okSection = confessionalSectionByKind(kind)
	}
	if !okSection {
		http.NotFound(w, r)
		return
	}
	user, ok := a.requirePermissionKey(w, r, section.CreatePerm, PermissionTarget{})
	if !ok {
		return
	}
	kind := section.Kind
	if kind == "" {
		http.NotFound(w, r)
		return
	}
	if confessionalCatalogSection(kind) {
		http.NotFound(w, r)
		return
	}
	data := confessionalFormData{Kind: kind, IsNew: true, ReturnURL: firstNonEmpty(strings.TrimSpace(r.URL.Query().Get("return_to")), confessionalSectionURL(section, ""))}
	switch kind {
	case "model":
		data.Model = &db.ModelConfessional{Estat: "actiu", ModeracioEstat: "pendent"}
	case "entitat":
		data.Entitat = &db.EntitatReligiosa{Estat: "actiu", ModeracioEstat: "pendent"}
	case "relacio":
		data.Relacio = &db.MunicipiEntitatReligiosa{TipusRelacio: "principal", ModeracioEstat: "pendent"}
	case "rel_ent":
		data.RelEnt = &db.EntitatReligiosaRelacio{ModeracioEstat: "pendent"}
	}
	a.renderConfessionalForm(w, r, user, data)
}

func (a *App) AdminEditConfessional(w http.ResponseWriter, r *http.Request) {
	section, okSection := confessionalSectionFromPath(r.URL.Path)
	if !okSection {
		kind, _ := extractConfessionalPath(r.URL.Path)
		section, okSection = confessionalSectionByKind(kind)
		if !okSection {
			http.NotFound(w, r)
			return
		}
	}
	user, ok := a.requirePermissionKey(w, r, section.EditPerm, PermissionTarget{})
	if !ok {
		return
	}
	kind, id := extractConfessionalPath(r.URL.Path)
	if kind == "" || id == 0 {
		http.NotFound(w, r)
		return
	}
	if confessionalCatalogSection(kind) {
		http.NotFound(w, r)
		return
	}
	data := confessionalFormData{Kind: kind, ReturnURL: strings.TrimSpace(r.URL.Query().Get("return_to"))}
	var err error
	switch kind {
	case "model":
		data.Model, err = a.DB.GetModelConfessional(id)
	case "entitat":
		data.Entitat, err = a.DB.GetEntitatReligiosa(id)
	case "relacio":
		data.Relacio, err = a.DB.GetMunicipiEntitatReligiosa(id)
	case "rel_ent":
		data.RelEnt, err = a.DB.GetEntitatReligiosaRelacio(id)
	}
	if err != nil {
		http.NotFound(w, r)
		return
	}
	a.renderConfessionalForm(w, r, user, data)
}

func (a *App) AdminSaveConfessional(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/confessional", http.StatusSeeOther)
		return
	}
	id, _ := strconv.Atoi(r.FormValue("id"))
	kind := confessionalKind(r.FormValue("kind"))
	if kind == "" {
		http.NotFound(w, r)
		return
	}
	section, okSection := confessionalSectionByKind(kind)
	if !okSection {
		http.NotFound(w, r)
		return
	}
	permKey := section.CreatePerm
	if id != 0 {
		permKey = section.EditPerm
	}
	user, ok := a.requirePermissionKey(w, r, permKey, PermissionTarget{})
	if !ok {
		return
	}
	if confessionalCatalogSection(kind) {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLangForUser(r, user.PreferredLang)
	data, errMsg := a.parseConfessionalForm(kind, id, r, lang)
	if errMsg != "" {
		data.Error = errMsg
		a.renderConfessionalForm(w, r, user, data)
		return
	}
	if err := a.applyConfessionalAuthorship(kind, id, user.ID, &data); err != nil {
		data.Error = "No s'ha pogut preparar l'autoria del registre confessional."
		a.renderConfessionalForm(w, r, user, data)
		return
	}
	if kind == "entitat" && id == 0 && data.Entitat != nil && data.Entitat.ParentID.Valid && !a.HasPermission(user.ID, permKeyTerritoriConfessionalRelacionsEntitatsCreate, PermissionTarget{}) {
		data.Error = "No tens permis per crear la relacio jerarquica."
		a.renderConfessionalForm(w, r, user, data)
		return
	}
	if kind == "entitat" && id > 0 {
		if proposed, err := a.createEntitatReligiosaWikiProposal(data.Entitat, user.ID); err != nil {
			data.Error = "No s'ha pogut crear la proposta de canvi."
			a.renderConfessionalForm(w, r, user, data)
			return
		} else if proposed {
			returnURL := data.ReturnURL
			if returnURL == "" {
				returnURL = fmt.Sprintf("/confessional/entitats/%d?notice=pending", id)
			}
			http.Redirect(w, r, returnURL, http.StatusSeeOther)
			return
		}
	}
	savedID, err := a.saveConfessionalData(kind, data)
	if err != nil {
		data.Error = "No s'ha pogut desar el registre confessional."
		a.renderConfessionalForm(w, r, user, data)
		return
	}
	if kind == "entitat" && id == 0 && data.Entitat != nil && data.Entitat.ParentID.Valid {
		if err := a.createConfessionalParentRelation(data.Entitat, savedID, user.ID); err != nil {
			data.Error = "No s'ha pogut crear la relacio jerarquica."
			a.renderConfessionalForm(w, r, user, data)
			return
		}
	}
	returnURL := data.ReturnURL
	if returnURL == "" {
		returnURL = confessionalSectionURL(section, "notice=saved")
	}
	http.Redirect(w, r, returnURL, http.StatusSeeOther)
}

func (a *App) AdminConfessionalEntityShow(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriConfessionalEntitatsView, PermissionTarget{})
	if !ok {
		return
	}
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	entitat, err := a.DB.GetEntitatReligiosa(id)
	if err != nil || entitat == nil {
		http.NotFound(w, r)
		return
	}
	canModerate := a.canModerateWikiObject(user, "entitat_religiosa", id)
	canEdit := a.HasPermission(user.ID, permKeyTerritoriConfessionalEntitatsEdit, PermissionTarget{})
	canDelete := a.HasPermission(user.ID, permKeyTerritoriConfessionalEntitatsDelete, PermissionTarget{})
	if entitat.ModeracioEstat != "publicat" && !(canEdit || canModerate) {
		http.NotFound(w, r)
		return
	}

	allEntitats, _ := a.DB.ListEntitatsReligioses()
	allRelEnt, _ := a.DB.ListEntitatReligiosaRelacions()
	allRelTerr, _ := a.DB.ListMunicipiEntitatsReligioses(0)
	municipis, _ := a.DB.ListMunicipis(db.MunicipiFilter{})
	paisos, _ := a.DB.ListPaisos()
	changes, _ := a.DB.ListWikiChanges("entitat_religiosa", id)

	relsSuperiors := make([]db.EntitatReligiosaRelacio, 0)
	relsInferiors := make([]db.EntitatReligiosaRelacio, 0)
	for _, rel := range allRelEnt {
		if rel.ModeracioEstat != "publicat" && !canModerate {
			continue
		}
		if rel.EntitatDestiID == id {
			relsSuperiors = append(relsSuperiors, rel)
		}
		if rel.EntitatOrigenID == id {
			relsInferiors = append(relsInferiors, rel)
		}
	}
	relsTerritori := make([]db.MunicipiEntitatReligiosa, 0)
	for _, rel := range allRelTerr {
		if rel.EntitatReligiosaID == id && (rel.ModeracioEstat == "publicat" || canModerate) {
			relsTerritori = append(relsTerritori, rel)
		}
	}
	hasPending := entitat.ModeracioEstat == "pendent"
	for _, ch := range changes {
		if ch.ModeracioEstat == "pendent" {
			hasPending = true
			break
		}
	}

	lang := ResolveLangForUser(r, user.PreferredLang)
	relationTypeLabels := confessionalLevelCatalogLabels(lang)
	for _, rel := range allRelEnt {
		if _, ok := relationTypeLabels[rel.TipusRelacio]; !ok {
			relationTypeLabels[rel.TipusRelacio] = rel.TipusRelacio
		}
	}
	for _, rel := range allRelTerr {
		if _, ok := relationTypeLabels[rel.TipusRelacio]; !ok {
			relationTypeLabels[rel.TipusRelacio] = rel.TipusRelacio
		}
	}
	RenderPrivateTemplate(w, r, "admin-confessional-entity-show.html", map[string]interface{}{
		"Entitat":               entitat,
		"EntitatLabels":         entitatReligiosaLabels(allEntitats),
		"ReligionCatalogLabels": confessionalReligionCatalogLabels(lang),
		"LevelCatalogLabels":    confessionalLevelCatalogLabels(lang),
		"RelationTypeLabels":    relationTypeLabels,
		"PaisLabels":            paisLabels(paisos),
		"MunicipiLabels":        municipiLabels(municipis),
		"RelacionsSuperiors":    relsSuperiors,
		"RelacionsInferiors":    relsInferiors,
		"RelacionsTerritori":    relsTerritori,
		"HasPendingChanges":     hasPending,
		"CanEdit":               canEdit,
		"CanDelete":             canDelete,
		"CanModerate":           canModerate,
		"Creator":               a.confessionalUserLabel(entitat.CreatedBy),
		"Updater":               a.confessionalUserLabel(entitat.UpdatedBy),
		"Moderator":             a.confessionalUserLabel(entitat.ModeratedBy),
		"Notice":                strings.TrimSpace(r.URL.Query().Get("notice")),
		"User":                  user,
	})
}

func (a *App) confessionalUserLabel(id sql.NullInt64) string {
	if !id.Valid || id.Int64 <= 0 {
		return "-"
	}
	u, err := a.DB.GetUserByID(int(id.Int64))
	if err != nil || u == nil {
		return fmt.Sprintf("#%d", id.Int64)
	}
	label := strings.TrimSpace(u.Usuari)
	if label == "" {
		label = strings.TrimSpace(strings.TrimSpace(u.Name) + " " + strings.TrimSpace(u.Surname))
	}
	if label == "" {
		label = fmt.Sprintf("#%d", u.ID)
	}
	return label
}

func (a *App) EntitatReligiosaWikiHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriConfessionalEntitatsView, PermissionTarget{})
	if !ok {
		return
	}
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	entitat, err := a.DB.GetEntitatReligiosa(id)
	if err != nil || entitat == nil {
		http.NotFound(w, r)
		return
	}
	canModerate := a.canModerateWikiObject(user, "entitat_religiosa", id)
	if entitat.ModeracioEstat != "publicat" && !canModerate {
		http.NotFound(w, r)
		return
	}
	changes, _ := a.DB.ListWikiChanges("entitat_religiosa", id)
	changes = filterVisibleWikiChanges(changes, user.ID, canModerate)
	totalChanges := len(changes)
	history := make([]wikiHistoryItem, 0, len(changes))
	for idx, ch := range changes {
		changedByID := 0
		if ch.ChangedBy.Valid {
			changedByID = int(ch.ChangedBy.Int64)
		}
		moderatedBy := ""
		if ch.ModeratedBy.Valid {
			moderatedBy = a.confessionalUserLabel(ch.ModeratedBy)
		}
		changedAt := ""
		if !ch.ChangedAt.IsZero() {
			changedAt = ch.ChangedAt.Format("02/01/2006 15:04")
		}
		moderatedAt := ""
		if ch.ModeratedAt.Valid {
			moderatedAt = ch.ModeratedAt.Time.Format("02/01/2006 15:04")
		}
		before, after := parseWikiChangeMeta(ch.Metadata)
		history = append(history, wikiHistoryItem{
			ID:             ch.ID,
			Seq:            totalChanges - idx,
			ChangeType:     ch.ChangeType,
			FieldKey:       ch.FieldKey,
			OldValue:       ch.OldValue,
			NewValue:       ch.NewValue,
			ChangedAt:      changedAt,
			ChangedBy:      a.confessionalUserLabel(sql.NullInt64{Int64: int64(changedByID), Valid: changedByID > 0}),
			ChangedByID:    changedByID,
			ModeratedBy:    moderatedBy,
			ModeratedAt:    moderatedAt,
			ModeracioEstat: ch.ModeracioEstat,
			HasSnapshot:    len(before) > 0 || len(after) > 0,
			CanRevert:      false,
		})
	}
	RenderPrivateTemplate(w, r, "wiki-history.html", map[string]interface{}{
		"Title":      entitat.Nom,
		"BackURL":    fmt.Sprintf("/confessional/entitats/%d", id),
		"HistoryURL": fmt.Sprintf("/confessional/entitats/%d/history", id),
		"RevertURL":  "",
		"History":    history,
		"User":       user,
	})
}

func (a *App) AdminDeleteConfessional(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/confessional", http.StatusSeeOther)
		return
	}
	kind := confessionalKind(r.FormValue("kind"))
	id, _ := strconv.Atoi(r.FormValue("id"))
	if kind == "" || id == 0 {
		http.NotFound(w, r)
		return
	}
	section, okSection := confessionalSectionByKind(kind)
	if !okSection {
		http.NotFound(w, r)
		return
	}
	if _, ok := a.requirePermissionKey(w, r, section.DeletePerm, PermissionTarget{}); !ok {
		return
	}
	if confessionalCatalogSection(kind) {
		http.NotFound(w, r)
		return
	}
	var err error
	switch kind {
	case "model":
		err = a.DB.DeleteModelConfessional(id)
	case "entitat":
		err = a.DB.DeleteEntitatReligiosa(id)
	case "relacio":
		err = a.DB.DeleteMunicipiEntitatReligiosa(id)
	case "rel_ent":
		err = a.DB.DeleteEntitatReligiosaRelacio(id)
	}
	if err != nil {
		msg := "delete"
		if errors.Is(err, db.ErrUnsafeDelete) {
			msg = "unsafe_delete"
		}
		http.Redirect(w, r, confessionalSectionURL(section, "error="+msg), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, confessionalSectionURL(section, "notice=deleted"), http.StatusSeeOther)
}

func (a *App) renderConfessionalForm(w http.ResponseWriter, r *http.Request, user *db.User, data confessionalFormData) {
	var allReligions []db.ReligioConfessio
	var models []db.ModelConfessional
	var allEntitats []db.EntitatReligiosa
	var municipis []db.MunicipiRow
	var paisos []db.Pais
	switch data.Kind {
	case "model":
		allReligions, _ = a.DB.ListReligioConfessions()
		paisos, _ = a.DB.ListPaisos()
	case "relacio":
		allEntitats, _ = a.DB.ListEntitatsReligioses()
		municipis, _ = a.DB.ListMunicipis(db.MunicipiFilter{})
	case "rel_ent":
		allEntitats, _ = a.DB.ListEntitatsReligioses()
	case "entitat":
		allEntitats, _ = a.DB.ListEntitatsReligioses()
		paisos, _ = a.DB.ListPaisos()
		if data.Entitat != nil && data.Entitat.NivellConfessionalCodi != "" {
			_, _, _, _, compatible := ConfessionalLevelCompatibleWithReligion(data.Entitat.ReligioConfessioCodi, data.Entitat.NivellConfessionalCodi)
			if !compatible {
				data.Entitat.NivellConfessionalCodi = ""
			}
		}
	}
	nuclis := a.compatibleNucliRows(municipis, selectedRelacioMunicipiID(data.Relacio))
	lang := ResolveLangForUser(r, user.PreferredLang)
	RenderPrivateTemplate(w, r, "admin-confessional-form.html", map[string]interface{}{
		"Section":               confessionalSectionMust(data.Kind),
		"Form":                  data,
		"Religions":             allReligions,
		"SelectableReligions":   ListConfessionalReligionCatalog(),
		"Models":                models,
		"Nivells":               ListConfessionalLevelCatalog(),
		"SelectableNivells":     ListConfessionalLevelCatalog(),
		"ReligionCatalogLabels": confessionalReligionCatalogLabels(lang),
		"LevelCatalogLabels":    confessionalLevelCatalogLabels(lang),
		"Entitats":              allEntitats,
		"SelectableEntitats":    publishedEntitatsReligioses(allEntitats),
		"Municipis":             municipis,
		"Nuclis":                nuclis,
		"Paisos":                paisos,
		"CanManageArxius":       a.canManageAnyDocumentalsModular(user),
		"User":                  user,
	})
}

func (a *App) parseConfessionalForm(kind string, id int, r *http.Request, lang string) (confessionalFormData, string) {
	data := confessionalFormData{Kind: kind, IsNew: id == 0, ReturnURL: strings.TrimSpace(r.FormValue("return_to"))}
	estat := normalizeConfessionalEstat(r.FormValue("estat"))
	status := a.confessionalModerationStatusForSave(kind, id)
	switch kind {
	case "model":
		item := &db.ModelConfessional{
			ID:                 id,
			Nom:                strings.TrimSpace(r.FormValue("nom")),
			ReligioConfessioID: sqlNullInt(r.FormValue("religio_confessio_id")),
			PaisID:             sqlNullInt(r.FormValue("pais_id")),
			Descripcio:         strings.TrimSpace(r.FormValue("descripcio")),
			AnyInici:           sqlNullInt(r.FormValue("any_inici")),
			AnyFi:              sqlNullInt(r.FormValue("any_fi")),
			Estat:              estat,
			Observacions:       strings.TrimSpace(r.FormValue("observacions")),
			ModeracioEstat:     status,
		}
		data.Model = item
		if item.Nom == "" {
			return data, "El nom es obligatori."
		}
	case "entitat":
		item := &db.EntitatReligiosa{
			ID:                     id,
			Codi:                   normalizeConfessionalCode(r.FormValue("codi")),
			Nom:                    strings.TrimSpace(r.FormValue("nom")),
			ReligioConfessioCodi:   normalizeConfessionalCode(r.FormValue("religio_confessio_codi")),
			NivellConfessionalCodi: normalizeConfessionalCode(r.FormValue("nivell_confessional_codi")),
			ParentID:               sqlNullInt(r.FormValue("parent_id")),
			PaisID:                 sqlNullInt(r.FormValue("pais_id")),
			AnyInici:               sqlNullInt(r.FormValue("any_inici")),
			AnyFi:                  sqlNullInt(r.FormValue("any_fi")),
			Estat:                  estat,
			Web:                    strings.TrimSpace(r.FormValue("web")),
			WebWikipedia:           strings.TrimSpace(r.FormValue("web_wikipedia")),
			Descripcio:             strings.TrimSpace(r.FormValue("descripcio")),
			Observacions:           strings.TrimSpace(r.FormValue("observacions")),
			ModeracioEstat:         status,
		}
		data.Entitat = item
		if item.Nom == "" {
			return data, "El nom es obligatori."
		}
		if item.Codi == "" {
			return data, "El codi es obligatori."
		}
		if item.ReligioConfessioCodi == "" {
			return data, "Cal indicar la religio/confessio."
		}
		if item.NivellConfessionalCodi == "" {
			return data, "Cal indicar el nivell confessional."
		}
		_, _, religionOK, levelOK, compatible := ConfessionalLevelCompatibleWithReligion(item.ReligioConfessioCodi, item.NivellConfessionalCodi)
		if !religionOK {
			return data, T(lang, "confessional.error.religion_unknown")
		}
		if !levelOK {
			return data, T(lang, "confessional.error.level_unknown")
		}
		if !compatible {
			return data, T(lang, "confessional.error.level_incompatible")
		}
		if item.ParentID.Valid {
			if id > 0 && item.ParentID.Int64 == int64(id) {
				return data, "L'entitat pare i la filla no poden ser la mateixa."
			}
			parent, err := a.DB.GetEntitatReligiosa(int(item.ParentID.Int64))
			if err != nil || parent == nil {
				return data, "L'entitat pare indicada no existeix."
			}
			if parent.ModeracioEstat != "publicat" {
				return data, "L'entitat pare indicada no esta publicada."
			}
			if parent.ReligioConfessioCodi != "" && parent.ReligioConfessioCodi != item.ReligioConfessioCodi {
				return data, "L'entitat pare ha de compartir religio/confessio."
			}
			if err := validateConfessionalEntityRelation(parent, item); err != nil {
				return data, err.Error()
			}
		}
	case "relacio":
		municipiID, _ := strconv.Atoi(r.FormValue("municipi_id"))
		entitatID, _ := strconv.Atoi(r.FormValue("entitat_religiosa_id"))
		item := &db.MunicipiEntitatReligiosa{
			ID:                 id,
			MunicipiID:         municipiID,
			NucliID:            sqlNullInt(r.FormValue("nucli_id")),
			EntitatReligiosaID: entitatID,
			TipusRelacio:       "",
			AnyInici:           sqlNullInt(r.FormValue("any_inici")),
			AnyFi:              sqlNullInt(r.FormValue("any_fi")),
			Observacions:       strings.TrimSpace(r.FormValue("observacions")),
			ModeracioEstat:     status,
		}
		data.Relacio = item
		if item.MunicipiID == 0 {
			return data, "Cal indicar el municipi."
		}
		if _, err := a.DB.GetMunicipi(item.MunicipiID); err != nil {
			return data, "El municipi indicat no existeix."
		}
		if item.NucliID.Valid {
			if item.NucliID.Int64 == int64(item.MunicipiID) {
				return data, "El nucli no pot ser el mateix registre que el municipi."
			}
			nucli, err := a.DB.GetMunicipi(int(item.NucliID.Int64))
			if err != nil || nucli == nil {
				return data, "El nucli indicat no existeix."
			}
			if !nucli.MunicipiID.Valid || nucli.MunicipiID.Int64 != int64(item.MunicipiID) {
				return data, "El nucli indicat no pertany al municipi seleccionat."
			}
		}
		if item.EntitatReligiosaID == 0 {
			return data, "Cal indicar l'entitat religiosa."
		}
		entitat, err := a.DB.GetEntitatReligiosa(item.EntitatReligiosaID)
		if err != nil || entitat == nil {
			return data, "L'entitat religiosa indicada no existeix."
		}
		if entitat.ModeracioEstat != "publicat" {
			return data, "L'entitat religiosa indicada no esta publicada."
		}
		item.TipusRelacio = suggestConfessionalRelationType(entitat.NivellConfessionalCodi)
	case "rel_ent":
		origenID, _ := strconv.Atoi(r.FormValue("entitat_origen_id"))
		destiID, _ := strconv.Atoi(r.FormValue("entitat_desti_id"))
		item := &db.EntitatReligiosaRelacio{
			ID:              id,
			EntitatOrigenID: origenID,
			EntitatDestiID:  destiID,
			TipusRelacio:    "",
			AnyInici:        sqlNullInt(r.FormValue("any_inici")),
			AnyFi:           sqlNullInt(r.FormValue("any_fi")),
			Observacions:    strings.TrimSpace(r.FormValue("observacions")),
			ModeracioEstat:  status,
		}
		data.RelEnt = item
		if item.EntitatOrigenID == 0 {
			return data, "Cal indicar l'entitat pare."
		}
		if item.EntitatDestiID == 0 {
			return data, "Cal indicar l'entitat filla."
		}
		if item.EntitatOrigenID == item.EntitatDestiID {
			return data, "L'entitat pare i la filla no poden ser la mateixa."
		}
		parent, err := a.DB.GetEntitatReligiosa(item.EntitatOrigenID)
		if err != nil || parent == nil {
			return data, "L'entitat pare indicada no existeix."
		}
		child, err := a.DB.GetEntitatReligiosa(item.EntitatDestiID)
		if err != nil || child == nil {
			return data, "L'entitat filla indicada no existeix."
		}
		if parent.ModeracioEstat != "publicat" || child.ModeracioEstat != "publicat" {
			return data, "Les entitats de la relacio han d'estar publicades."
		}
		if parent.ReligioConfessioCodi != "" && child.ReligioConfessioCodi != "" && parent.ReligioConfessioCodi != child.ReligioConfessioCodi {
			return data, "Les entitats de la relacio han de compartir religio/confessio."
		}
		if err := validateConfessionalEntityRelation(parent, child); err != nil {
			return data, err.Error()
		}
		item.TipusRelacio = suggestConfessionalRelationType(child.NivellConfessionalCodi)
	}
	return data, ""
}

func selectedRelacioMunicipiID(rel *db.MunicipiEntitatReligiosa) int {
	if rel == nil {
		return 0
	}
	return rel.MunicipiID
}

func (a *App) compatibleNucliRows(municipis []db.MunicipiRow, municipiID int) []db.MunicipiRow {
	if municipiID == 0 {
		return nil
	}
	nuclis := make([]db.MunicipiRow, 0)
	for _, row := range municipis {
		if row.ID == municipiID {
			continue
		}
		full, err := a.DB.GetMunicipi(row.ID)
		if err != nil || full == nil || !full.MunicipiID.Valid || full.MunicipiID.Int64 != int64(municipiID) {
			continue
		}
		nuclis = append(nuclis, row)
	}
	return nuclis
}

func (a *App) confessionalModerationStatusForSave(kind string, id int) string {
	if id == 0 {
		return "pendent"
	}
	var status string
	switch kind {
	case "model":
		if item, err := a.DB.GetModelConfessional(id); err == nil && item != nil {
			status = item.ModeracioEstat
		}
	case "entitat":
		if item, err := a.DB.GetEntitatReligiosa(id); err == nil && item != nil {
			status = item.ModeracioEstat
		}
	case "relacio":
		if item, err := a.DB.GetMunicipiEntitatReligiosa(id); err == nil && item != nil {
			status = item.ModeracioEstat
		}
	case "rel_ent":
		if item, err := a.DB.GetEntitatReligiosaRelacio(id); err == nil && item != nil {
			status = item.ModeracioEstat
		}
	}
	return normalizeModerationStatus(status)
}

func (a *App) saveConfessionalData(kind string, data confessionalFormData) (int, error) {
	switch kind {
	case "model":
		return a.DB.SaveModelConfessional(data.Model)
	case "entitat":
		return a.DB.SaveEntitatReligiosa(data.Entitat)
	case "relacio":
		return a.DB.SaveMunicipiEntitatReligiosa(data.Relacio)
	case "rel_ent":
		return a.DB.SaveEntitatReligiosaRelacio(data.RelEnt)
	default:
		return 0, errors.New("tipus confessional no valid")
	}
}

func (a *App) createConfessionalParentRelation(entitat *db.EntitatReligiosa, childID, userID int) error {
	if entitat == nil || childID == 0 || !entitat.ParentID.Valid {
		return nil
	}
	parent, err := a.DB.GetEntitatReligiosa(int(entitat.ParentID.Int64))
	if err != nil || parent == nil {
		if err != nil {
			return err
		}
		return errors.New("entitat pare inexistent")
	}
	if err := validateConfessionalEntityRelation(parent, entitat); err != nil {
		return err
	}
	_, err = a.DB.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
		EntitatOrigenID: int(entitat.ParentID.Int64),
		EntitatDestiID:  childID,
		TipusRelacio:    suggestConfessionalRelationType(entitat.NivellConfessionalCodi),
		AnyInici:        entitat.AnyInici,
		AnyFi:           entitat.AnyFi,
		ModeracioEstat:  "pendent",
		CreatedBy:       sqlNullIntFromInt(userID),
		UpdatedBy:       sqlNullIntFromInt(userID),
	})
	return err
}

func validateConfessionalEntityRelation(parent, child *db.EntitatReligiosa) error {
	if parent == nil || child == nil {
		return errors.New("Cal indicar entitat pare i entitat filla.")
	}
	if parent.ID != 0 && child.ID != 0 && parent.ID == child.ID {
		return errors.New("L'entitat pare i la filla no poden ser la mateixa.")
	}
	if parent.ReligioConfessioCodi != "" && child.ReligioConfessioCodi != "" && parent.ReligioConfessioCodi != child.ReligioConfessioCodi {
		return errors.New("Les entitats de la relacio han de compartir religio/confessio.")
	}
	parentLevel, parentOK := GetConfessionalLevelCatalogByCode(parent.NivellConfessionalCodi)
	childLevel, childOK := GetConfessionalLevelCatalogByCode(child.NivellConfessionalCodi)
	if parentOK && childOK {
		if !parentLevel.CanHaveChildren {
			return errors.New("El nivell de l'entitat pare no admet entitats filles.")
		}
		if parentLevel.ReligionCode != "" && childLevel.ReligionCode != "" && parentLevel.ReligionCode != childLevel.ReligionCode {
			return errors.New("Els nivells de la relacio han de pertanyer a la mateixa religio/confessio.")
		}
	}
	return nil
}

func (a *App) applyConfessionalAuthorship(kind string, id, userID int, data *confessionalFormData) error {
	author := sqlNullIntFromInt(userID)
	switch kind {
	case "entitat":
		if data.Entitat == nil {
			return nil
		}
		data.Entitat.UpdatedBy = author
		if id == 0 {
			data.Entitat.CreatedBy = author
			return nil
		}
		current, err := a.DB.GetEntitatReligiosa(id)
		if err != nil || current == nil {
			return err
		}
		data.Entitat.CreatedBy = current.CreatedBy
		data.Entitat.ModeratedBy = current.ModeratedBy
		data.Entitat.ModeratedAt = current.ModeratedAt
		data.Entitat.ModeracioMotiu = current.ModeracioMotiu
	case "relacio":
		if data.Relacio == nil {
			return nil
		}
		data.Relacio.UpdatedBy = author
		if id == 0 {
			data.Relacio.CreatedBy = author
			return nil
		}
		current, err := a.DB.GetMunicipiEntitatReligiosa(id)
		if err != nil || current == nil {
			return err
		}
		data.Relacio.CreatedBy = current.CreatedBy
		data.Relacio.ModeratedBy = current.ModeratedBy
		data.Relacio.ModeratedAt = current.ModeratedAt
		data.Relacio.ModeracioMotiu = current.ModeracioMotiu
	case "rel_ent":
		if data.RelEnt == nil {
			return nil
		}
		data.RelEnt.UpdatedBy = author
		if id == 0 {
			data.RelEnt.CreatedBy = author
			return nil
		}
		current, err := a.DB.GetEntitatReligiosaRelacio(id)
		if err != nil || current == nil {
			return err
		}
		data.RelEnt.CreatedBy = current.CreatedBy
		data.RelEnt.ModeratedBy = current.ModeratedBy
		data.RelEnt.ModeratedAt = current.ModeratedAt
		data.RelEnt.ModeracioMotiu = current.ModeracioMotiu
	}
	return nil
}

func (a *App) createEntitatReligiosaWikiProposal(after *db.EntitatReligiosa, userID int) (bool, error) {
	if after == nil || after.ID == 0 {
		return false, nil
	}
	before, err := a.DB.GetEntitatReligiosa(after.ID)
	if err != nil || before == nil {
		return false, err
	}
	if before.ModeracioEstat != "publicat" {
		return false, nil
	}
	next := *after
	next.ID = before.ID
	next.CreatedBy = before.CreatedBy
	next.ModeracioEstat = "publicat"
	next.ModeracioMotiu = before.ModeracioMotiu
	next.ModeratedBy = before.ModeratedBy
	next.ModeratedAt = before.ModeratedAt
	if !next.UpdatedBy.Valid {
		next.UpdatedBy = sqlNullIntFromInt(userID)
	}
	beforeJSON, err := json.Marshal(before)
	if err != nil {
		return false, err
	}
	afterJSON, err := json.Marshal(next)
	if err != nil {
		return false, err
	}
	metadata, err := buildWikiChangeMetadata(beforeJSON, afterJSON, 0)
	if err != nil {
		return false, err
	}
	_, err = a.createWikiChange(&db.WikiChange{
		ObjectType:     "entitat_religiosa",
		ObjectID:       before.ID,
		ChangeType:     "update",
		FieldKey:       "*",
		OldValue:       before.Nom,
		NewValue:       next.Nom,
		Metadata:       metadata,
		ModeracioEstat: "pendent",
		ModeracioMotiu: "",
		ChangedBy:      sqlNullIntFromInt(userID),
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func confessionalKind(raw string) string {
	switch strings.TrimSpace(raw) {
	case "religio", "model", "nivell", "entitat", "relacio", "rel_ent":
		return strings.TrimSpace(raw)
	default:
		return ""
	}
}

func confessionalSectionByKind(kind string) (confessionalSection, bool) {
	section, ok := confessionalSections[confessionalKind(kind)]
	return section, ok
}

func confessionalCatalogSection(kind string) bool {
	return kind == "religio" || kind == "nivell"
}

func confessionalSectionMust(kind string) confessionalSection {
	section, _ := confessionalSectionByKind(kind)
	return section
}

func confessionalSectionFromSlug(slug string) (confessionalSection, bool) {
	slug = strings.Trim(strings.TrimSpace(slug), "/")
	for _, section := range confessionalSections {
		if section.Slug == slug {
			return section, true
		}
	}
	return confessionalSection{}, false
}

func confessionalSectionFromPath(path string) (confessionalSection, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] != "confessional" {
			continue
		}
		return confessionalSectionFromSlug(parts[i+1])
	}
	return confessionalSection{}, false
}

func confessionalSectionURL(section confessionalSection, query string) string {
	url := "/confessional/" + section.Slug
	query = strings.TrimSpace(query)
	if query != "" {
		url += "?" + query
	}
	return url
}

func extractConfessionalPath(path string) (string, int) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i := 0; i+2 < len(parts); i++ {
		if parts[i] != "confessional" {
			continue
		}
		if section, ok := confessionalSectionFromSlug(parts[i+1]); ok {
			id, _ := strconv.Atoi(parts[i+2])
			return section.Kind, id
		}
		kind := confessionalKind(parts[i+1])
		id, _ := strconv.Atoi(parts[i+2])
		return kind, id
	}
	return "", 0
}

func normalizeReligioEstat(raw string) string {
	if strings.TrimSpace(raw) == "inactiu" {
		return "inactiu"
	}
	return "actiu"
}

func normalizeConfessionalCode(raw string) string {
	code := strings.ToLower(strings.TrimSpace(raw))
	replacer := strings.NewReplacer(" ", "_", "-", "_", ".", "_", "/", "_")
	code = replacer.Replace(code)
	code = strings.Trim(code, "_")
	return code
}

func normalizeReligioCategoria(raw string) string {
	switch strings.TrimSpace(raw) {
	case "religio", "branca", "confessio", "ritus", "tradicio":
		return strings.TrimSpace(raw)
	default:
		return "religio"
	}
}

func normalizeNivellConfessionalCategoria(raw string) string {
	switch strings.TrimSpace(raw) {
	case "govern_universal", "coordinacio", "territorial_major", "territorial_intermedi", "territorial_local", "unitat_pastoral", "lloc_de_culte", "comunitat_religiosa", "pelegrinatge", "llinatge_comunitat", "no_territorial":
		return strings.TrimSpace(raw)
	default:
		return "no_territorial"
	}
}

func normalizeConfessionalEstat(raw string) string {
	switch strings.TrimSpace(raw) {
	case "inactiu", "historic":
		return strings.TrimSpace(raw)
	default:
		return "actiu"
	}
}

func normalizeModerationStatus(raw string) string {
	switch strings.TrimSpace(raw) {
	case "publicat":
		return "publicat"
	case "pendent", "rebutjat":
		return strings.TrimSpace(raw)
	default:
		return "pendent"
	}
}

func normalizeRelacioTipus(raw string) string {
	switch strings.TrimSpace(raw) {
	case "principal", "parroquia", "bisbat", "arxiprestat", "historica", "altres":
		return strings.TrimSpace(raw)
	default:
		return "principal"
	}
}

func publishedReligioConfessions(items []db.ReligioConfessio) []db.ReligioConfessio {
	out := make([]db.ReligioConfessio, 0, len(items))
	for _, item := range items {
		if item.ModeracioEstat == "publicat" && item.Estat == "actiu" {
			out = append(out, item)
		}
	}
	return out
}

func publishedNivellsConfessionals(items []db.NivellConfessional) []db.NivellConfessional {
	out := make([]db.NivellConfessional, 0, len(items))
	for _, item := range items {
		if item.ModeracioEstat == "publicat" && item.Estat == "actiu" {
			out = append(out, item)
		}
	}
	return out
}

func publishedEntitatsReligioses(items []db.EntitatReligiosa) []db.EntitatReligiosa {
	out := make([]db.EntitatReligiosa, 0, len(items))
	for _, item := range items {
		if item.ModeracioEstat == "publicat" && item.Estat == "actiu" {
			out = append(out, item)
		}
	}
	return out
}

func publishedEntitatReligiosaRelacions(items []db.EntitatReligiosaRelacio) []db.EntitatReligiosaRelacio {
	out := make([]db.EntitatReligiosaRelacio, 0, len(items))
	for _, item := range items {
		if item.ModeracioEstat == "publicat" {
			out = append(out, item)
		}
	}
	return out
}

func publishedMunicipiEntitatsReligioses(items []db.MunicipiEntitatReligiosa) []db.MunicipiEntitatReligiosa {
	out := make([]db.MunicipiEntitatReligiosa, 0, len(items))
	for _, item := range items {
		if item.ModeracioEstat == "publicat" {
			out = append(out, item)
		}
	}
	return out
}

func suggestConfessionalRelationType(levelCode string) string {
	level, ok := GetConfessionalLevelCatalogByCode(levelCode)
	if !ok || strings.TrimSpace(level.Code) == "" {
		return "relacio"
	}
	return level.Code
}

func religioLabels(items []db.ReligioConfessio) map[int]string {
	labels := map[int]string{}
	for _, item := range items {
		labels[item.ID] = item.Nom
	}
	return labels
}

func confessionalReligionCatalogLabels(lang string) map[string]string {
	labels := map[string]string{}
	for _, item := range ListConfessionalReligionCatalog() {
		labels[item.Code] = confessionalCatalogLabel(lang, "confessional.religion."+item.Code, item.CanonicalName)
	}
	return labels
}

func confessionalLevelCatalogLabels(lang string) map[string]string {
	labels := map[string]string{}
	for _, item := range ListConfessionalLevelCatalog() {
		key := strings.TrimSpace(item.I18nKey)
		if key == "" {
			key = "confessional.level." + item.Code
		}
		labels[item.Code] = confessionalCatalogLabel(lang, key, item.CanonicalName)
	}
	return labels
}

func confessionalCatalogLabel(lang, key, fallback string) string {
	label := strings.TrimSpace(T(lang, key))
	if label == "" || label == key {
		return fallback
	}
	return label
}

func modelLabels(items []db.ModelConfessional) map[int]string {
	labels := map[int]string{}
	for _, item := range items {
		labels[item.ID] = item.Nom
	}
	return labels
}

func nivellConfessionalLabels(items []db.NivellConfessional) map[int]string {
	labels := map[int]string{}
	for _, item := range items {
		labels[item.ID] = item.NomNivell
	}
	return labels
}

func entitatReligiosaLabels(items []db.EntitatReligiosa) map[int]string {
	labels := map[int]string{}
	for _, item := range items {
		labels[item.ID] = item.Nom
	}
	return labels
}

func paisLabels(items []db.Pais) map[int]string {
	labels := map[int]string{}
	for _, item := range items {
		labels[item.ID] = item.CodiISO3
	}
	return labels
}

func municipiLabels(items []db.MunicipiRow) map[int]string {
	labels := map[int]string{}
	for _, item := range items {
		label := strings.TrimSpace(item.Nom)
		if item.Tipus != "" {
			label += " (" + item.Tipus + ")"
		}
		labels[item.ID] = label
	}
	return labels
}
