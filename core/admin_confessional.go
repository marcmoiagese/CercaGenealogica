package core

import (
	"database/sql"
	"errors"
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
	http.Redirect(w, r, "/territori/confessional/entitats", http.StatusSeeOther)
}

func (a *App) AdminConfessionalSectionList(w http.ResponseWriter, r *http.Request) {
	section, okSection := confessionalSectionFromPath(r.URL.Path)
	if !okSection {
		http.NotFound(w, r)
		return
	}
	user, ok := a.requirePermissionKey(w, r, section.ViewPerm, PermissionTarget{})
	if !ok {
		return
	}
	religions, _ := a.DB.ListReligioConfessions()
	models, _ := a.DB.ListModelsConfessionals()
	nivells, _ := a.DB.ListNivellsConfessionals()
	entitats, _ := a.DB.ListEntitatsReligioses()
	relacions, _ := a.DB.ListMunicipiEntitatsReligioses(0)
	relEntitats, _ := a.DB.ListEntitatReligiosaRelacions()
	municipis, _ := a.DB.ListMunicipis(db.MunicipiFilter{})
	paisos, _ := a.DB.ListPaisos()
	canCreate := a.HasPermission(user.ID, section.CreatePerm, PermissionTarget{})
	canEdit := a.HasPermission(user.ID, section.EditPerm, PermissionTarget{})
	canDelete := a.HasPermission(user.ID, section.DeletePerm, PermissionTarget{})
	RenderPrivateTemplate(w, r, "admin-confessional-list.html", map[string]interface{}{
		"Section":         section,
		"Religions":       religions,
		"Models":          models,
		"Nivells":         nivells,
		"Entitats":        entitats,
		"Relacions":       relacions,
		"RelEntitats":     relEntitats,
		"Municipis":       municipis,
		"Paisos":          paisos,
		"CanCreate":       canCreate,
		"CanEdit":         canEdit,
		"CanDelete":       canDelete,
		"Notice":          strings.TrimSpace(r.URL.Query().Get("notice")),
		"Error":           strings.TrimSpace(r.URL.Query().Get("error")),
		"ReligionLabels":  religioLabels(religions),
		"ModelLabels":     modelLabels(models),
		"NivellLabels":    nivellConfessionalLabels(nivells),
		"EntitatLabels":   entitatReligiosaLabels(entitats),
		"MunicipiLabels":  municipiLabels(municipis),
		"PaisLabels":      paisLabels(paisos),
		"CanManageArxius": a.canManageAnyDocumentalsModular(user),
		"User":            user,
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
	data := confessionalFormData{Kind: kind, IsNew: true, ReturnURL: firstNonEmpty(strings.TrimSpace(r.URL.Query().Get("return_to")), confessionalSectionURL(section, ""))}
	switch kind {
	case "religio":
		data.Religio = &db.ReligioConfessio{Estat: "actiu", ModeracioEstat: "pendent"}
	case "model":
		data.Model = &db.ModelConfessional{Estat: "actiu", ModeracioEstat: "pendent"}
	case "nivell":
		data.Nivell = &db.NivellConfessional{Ordre: 1, Estat: "actiu", ModeracioEstat: "pendent"}
	case "entitat":
		data.Entitat = &db.EntitatReligiosa{Estat: "actiu", ModeracioEstat: "pendent"}
	case "relacio":
		data.Relacio = &db.MunicipiEntitatReligiosa{TipusRelacio: "principal", ModeracioEstat: "pendent"}
	case "rel_ent":
		data.RelEnt = &db.EntitatReligiosaRelacio{TipusRelacio: "jerarquia", ModeracioEstat: "pendent"}
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
	data := confessionalFormData{Kind: kind, ReturnURL: strings.TrimSpace(r.URL.Query().Get("return_to"))}
	var err error
	switch kind {
	case "religio":
		data.Religio, err = a.DB.GetReligioConfessio(id)
	case "model":
		data.Model, err = a.DB.GetModelConfessional(id)
	case "nivell":
		data.Nivell, err = a.DB.GetNivellConfessional(id)
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
		http.Redirect(w, r, "/territori/confessional", http.StatusSeeOther)
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
	data, errMsg := a.parseConfessionalForm(kind, id, r)
	if errMsg != "" {
		data.Error = errMsg
		a.renderConfessionalForm(w, r, user, data)
		return
	}
	if err := a.saveConfessionalData(kind, data); err != nil {
		data.Error = "No s'ha pogut desar el registre confessional."
		a.renderConfessionalForm(w, r, user, data)
		return
	}
	returnURL := data.ReturnURL
	if returnURL == "" {
		returnURL = confessionalSectionURL(section, "notice=saved")
	}
	http.Redirect(w, r, returnURL, http.StatusSeeOther)
}

func (a *App) AdminDeleteConfessional(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/territori/confessional", http.StatusSeeOther)
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
	var err error
	switch kind {
	case "religio":
		err = a.DB.DeleteReligioConfessio(id)
	case "model":
		err = a.DB.DeleteModelConfessional(id)
	case "nivell":
		err = a.DB.DeleteNivellConfessional(id)
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
	allReligions, _ := a.DB.ListReligioConfessions()
	models, _ := a.DB.ListModelsConfessionals()
	allNivells, _ := a.DB.ListNivellsConfessionals()
	allEntitats, _ := a.DB.ListEntitatsReligioses()
	religions := publishedReligioConfessions(allReligions)
	nivells := publishedNivellsConfessionals(allNivells)
	entitats := publishedEntitatsReligioses(allEntitats)
	municipis, _ := a.DB.ListMunicipis(db.MunicipiFilter{})
	paisos, _ := a.DB.ListPaisos()
	nuclis := a.compatibleNucliRows(municipis, selectedRelacioMunicipiID(data.Relacio))
	RenderPrivateTemplate(w, r, "admin-confessional-form.html", map[string]interface{}{
		"Section":             confessionalSectionMust(data.Kind),
		"Form":                data,
		"Religions":           allReligions,
		"SelectableReligions": religions,
		"Models":              models,
		"Nivells":             allNivells,
		"SelectableNivells":   nivells,
		"Entitats":            allEntitats,
		"SelectableEntitats":  entitats,
		"Municipis":           municipis,
		"Nuclis":              nuclis,
		"Paisos":              paisos,
		"CanManageArxius":     a.canManageAnyDocumentalsModular(user),
		"User":                user,
	})
}

func (a *App) parseConfessionalForm(kind string, id int, r *http.Request) (confessionalFormData, string) {
	data := confessionalFormData{Kind: kind, IsNew: id == 0, ReturnURL: strings.TrimSpace(r.FormValue("return_to"))}
	estat := normalizeConfessionalEstat(r.FormValue("estat"))
	status := a.confessionalModerationStatusForSave(kind, id)
	switch kind {
	case "religio":
		item := &db.ReligioConfessio{
			ID:             id,
			Codi:           normalizeConfessionalCode(r.FormValue("codi")),
			Nom:            strings.TrimSpace(r.FormValue("nom")),
			PareID:         sqlNullInt(r.FormValue("pare_id")),
			Categoria:      normalizeReligioCategoria(r.FormValue("categoria")),
			Descripcio:     strings.TrimSpace(r.FormValue("descripcio")),
			Estat:          normalizeReligioEstat(r.FormValue("estat")),
			Observacions:   strings.TrimSpace(r.FormValue("observacions")),
			ModeracioEstat: status,
		}
		data.Religio = item
		if item.Nom == "" {
			return data, "El nom es obligatori."
		}
		if item.PareID.Valid && item.ID != 0 && item.PareID.Int64 == int64(item.ID) {
			return data, "La religio/confessio no pot ser pare de si mateixa."
		}
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
	case "nivell":
		ordre, _ := strconv.Atoi(r.FormValue("ordre"))
		modelID, _ := strconv.Atoi(r.FormValue("model_confessional_id"))
		item := &db.NivellConfessional{
			ID:                  id,
			ModelConfessionalID: modelID,
			ReligioConfessioID:  sqlNullInt(r.FormValue("religio_confessio_id")),
			Codi:                normalizeConfessionalCode(r.FormValue("codi")),
			Ordre:               ordre,
			NomNivell:           strings.TrimSpace(r.FormValue("nom_nivell")),
			NomPlural:           strings.TrimSpace(r.FormValue("nom_plural")),
			TipusNivell:         strings.TrimSpace(r.FormValue("tipus_nivell")),
			Categoria:           normalizeNivellConfessionalCategoria(r.FormValue("categoria")),
			CodiOficial:         strings.TrimSpace(r.FormValue("codi_oficial")),
			PotTenirTerritori:   true,
			PotTenirFills:       true,
			PotVincularMunicipi: true,
			PotSuggerirImports:  true,
			ParentID:            sqlNullInt(r.FormValue("parent_id")),
			AnyInici:            sqlNullInt(r.FormValue("any_inici")),
			AnyFi:               sqlNullInt(r.FormValue("any_fi")),
			Estat:               estat,
			Observacions:        strings.TrimSpace(r.FormValue("observacions")),
			ModeracioEstat:      status,
		}
		data.Nivell = item
		if !item.ReligioConfessioID.Valid && item.ModelConfessionalID == 0 {
			return data, "Cal indicar la religio/confessio."
		}
		if item.Ordre <= 0 {
			return data, "L'ordre ha de ser positiu."
		}
		if item.NomNivell == "" {
			return data, "El nom del nivell es obligatori."
		}
		if item.ParentID.Valid && item.ID != 0 && item.ParentID.Int64 == int64(item.ID) {
			return data, "El nivell no pot ser pare de si mateix."
		}
	case "entitat":
		item := &db.EntitatReligiosa{
			ID:                   id,
			Codi:                 normalizeConfessionalCode(r.FormValue("codi")),
			Nom:                  strings.TrimSpace(r.FormValue("nom")),
			ReligioConfessioID:   sqlNullInt(r.FormValue("religio_confessio_id")),
			NivellConfessionalID: sqlNullInt(r.FormValue("nivell_confessional_id")),
			PaisID:               sqlNullInt(r.FormValue("pais_id")),
			AnyInici:             sqlNullInt(r.FormValue("any_inici")),
			AnyFi:                sqlNullInt(r.FormValue("any_fi")),
			Estat:                estat,
			Web:                  strings.TrimSpace(r.FormValue("web")),
			WebWikipedia:         strings.TrimSpace(r.FormValue("web_wikipedia")),
			Descripcio:           strings.TrimSpace(r.FormValue("descripcio")),
			Observacions:         strings.TrimSpace(r.FormValue("observacions")),
			ModeracioEstat:       status,
		}
		data.Entitat = item
		if item.Nom == "" {
			return data, "El nom es obligatori."
		}
		if item.Codi == "" {
			return data, "El codi es obligatori."
		}
		if !item.ReligioConfessioID.Valid {
			return data, "Cal indicar la religio/confessio."
		}
		if !item.NivellConfessionalID.Valid {
			return data, "Cal indicar el nivell confessional."
		}
		if !a.confessionalCatalogItemIsPublished("religio", int(item.ReligioConfessioID.Int64)) {
			return data, "La religio/confessio indicada no esta publicada."
		}
		if !a.confessionalCatalogItemIsPublished("nivell", int(item.NivellConfessionalID.Int64)) {
			return data, "El nivell confessional indicat no esta publicat."
		}
	case "relacio":
		municipiID, _ := strconv.Atoi(r.FormValue("municipi_id"))
		entitatID, _ := strconv.Atoi(r.FormValue("entitat_religiosa_id"))
		item := &db.MunicipiEntitatReligiosa{
			ID:                 id,
			MunicipiID:         municipiID,
			NucliID:            sqlNullInt(r.FormValue("nucli_id")),
			EntitatReligiosaID: entitatID,
			TipusRelacio:       strings.TrimSpace(r.FormValue("tipus_relacio")),
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
		if item.TipusRelacio == "" {
			item.TipusRelacio = a.suggestConfessionalRelationType(entitat.NivellConfessionalID)
		}
	case "rel_ent":
		origenID, _ := strconv.Atoi(r.FormValue("entitat_origen_id"))
		destiID, _ := strconv.Atoi(r.FormValue("entitat_desti_id"))
		item := &db.EntitatReligiosaRelacio{
			ID:              id,
			EntitatOrigenID: origenID,
			EntitatDestiID:  destiID,
			TipusRelacio:    firstNonEmpty(strings.TrimSpace(r.FormValue("tipus_relacio")), "jerarquia"),
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
		if parent.ReligioConfessioID.Valid && child.ReligioConfessioID.Valid && parent.ReligioConfessioID.Int64 != child.ReligioConfessioID.Int64 {
			return data, "Les entitats de la relacio han de compartir religio/confessio."
		}
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
	case "religio":
		if item, err := a.DB.GetReligioConfessio(id); err == nil && item != nil {
			status = item.ModeracioEstat
		}
	case "model":
		if item, err := a.DB.GetModelConfessional(id); err == nil && item != nil {
			status = item.ModeracioEstat
		}
	case "nivell":
		if item, err := a.DB.GetNivellConfessional(id); err == nil && item != nil {
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

func (a *App) saveConfessionalData(kind string, data confessionalFormData) error {
	switch kind {
	case "religio":
		_, err := a.DB.SaveReligioConfessio(data.Religio)
		return err
	case "model":
		_, err := a.DB.SaveModelConfessional(data.Model)
		return err
	case "nivell":
		_, err := a.DB.SaveNivellConfessional(data.Nivell)
		return err
	case "entitat":
		_, err := a.DB.SaveEntitatReligiosa(data.Entitat)
		return err
	case "relacio":
		_, err := a.DB.SaveMunicipiEntitatReligiosa(data.Relacio)
		return err
	case "rel_ent":
		_, err := a.DB.SaveEntitatReligiosaRelacio(data.RelEnt)
		return err
	default:
		return errors.New("tipus confessional no valid")
	}
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
	url := "/territori/confessional/" + section.Slug
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

func (a *App) confessionalCatalogItemIsPublished(kind string, id int) bool {
	switch kind {
	case "religio":
		item, err := a.DB.GetReligioConfessio(id)
		return err == nil && item != nil && item.ModeracioEstat == "publicat" && item.Estat == "actiu"
	case "nivell":
		item, err := a.DB.GetNivellConfessional(id)
		return err == nil && item != nil && item.ModeracioEstat == "publicat" && item.Estat == "actiu"
	default:
		return false
	}
}

func (a *App) suggestConfessionalRelationType(nivellID sql.NullInt64) string {
	if !nivellID.Valid {
		return "relacio"
	}
	nivell, err := a.DB.GetNivellConfessional(int(nivellID.Int64))
	if err != nil || nivell == nil {
		return "relacio"
	}
	if nivell.Codi != "" {
		return nivell.Codi
	}
	return normalizeConfessionalCode(nivell.NomNivell)
}

func religioLabels(items []db.ReligioConfessio) map[int]string {
	labels := map[int]string{}
	for _, item := range items {
		labels[item.ID] = item.Nom
	}
	return labels
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
