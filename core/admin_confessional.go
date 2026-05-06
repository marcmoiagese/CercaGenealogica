package core

import (
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
}

func (a *App) AdminConfessionalList(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKeyAnyScope(w, r, permKeyTerritoriEclesView)
	if !ok {
		return
	}
	religions, _ := a.DB.ListReligioConfessions()
	models, _ := a.DB.ListModelsConfessionals()
	nivells, _ := a.DB.ListNivellsConfessionals()
	entitats, _ := a.DB.ListEntitatsReligioses()
	paisos, _ := a.DB.ListPaisos()
	canCreate := a.HasPermission(user.ID, permKeyTerritoriEclesCreate, PermissionTarget{})
	canEdit := a.HasPermission(user.ID, permKeyTerritoriEclesEdit, PermissionTarget{})
	RenderPrivateTemplate(w, r, "admin-confessional-list.html", map[string]interface{}{
		"Religions":       religions,
		"Models":          models,
		"Nivells":         nivells,
		"Entitats":        entitats,
		"Paisos":          paisos,
		"CanCreate":       canCreate,
		"CanEdit":         canEdit,
		"Notice":          strings.TrimSpace(r.URL.Query().Get("notice")),
		"Error":           strings.TrimSpace(r.URL.Query().Get("error")),
		"ReligionLabels":  religioLabels(religions),
		"ModelLabels":     modelLabels(models),
		"NivellLabels":    nivellConfessionalLabels(nivells),
		"EntitatLabels":   entitatReligiosaLabels(entitats),
		"PaisLabels":      paisLabels(paisos),
		"CanManageArxius": a.canManageAnyDocumentalsModular(user),
		"User":            user,
	})
}

func (a *App) AdminNewConfessional(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriEclesCreate, PermissionTarget{})
	if !ok {
		return
	}
	kind := confessionalKind(r.URL.Query().Get("kind"))
	if kind == "" {
		http.NotFound(w, r)
		return
	}
	data := confessionalFormData{Kind: kind, IsNew: true, ReturnURL: strings.TrimSpace(r.URL.Query().Get("return_to"))}
	switch kind {
	case "religio":
		data.Religio = &db.ReligioConfessio{Estat: "actiu", ModeracioEstat: "publicat"}
	case "model":
		data.Model = &db.ModelConfessional{Estat: "actiu", ModeracioEstat: "publicat"}
	case "nivell":
		data.Nivell = &db.NivellConfessional{Ordre: 1, Estat: "actiu", ModeracioEstat: "publicat"}
	case "entitat":
		data.Entitat = &db.EntitatReligiosa{Estat: "actiu", ModeracioEstat: "publicat"}
	}
	a.renderConfessionalForm(w, r, user, data)
}

func (a *App) AdminEditConfessional(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriEclesEdit, PermissionTarget{})
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
	permKey := permKeyTerritoriEclesCreate
	if id != 0 {
		permKey = permKeyTerritoriEclesEdit
	}
	user, ok := a.requirePermissionKey(w, r, permKey, PermissionTarget{})
	if !ok {
		return
	}
	kind := confessionalKind(r.FormValue("kind"))
	if kind == "" {
		http.NotFound(w, r)
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
		returnURL = "/territori/confessional?notice=saved"
	}
	http.Redirect(w, r, returnURL, http.StatusSeeOther)
}

func (a *App) AdminDeleteConfessional(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/territori/confessional", http.StatusSeeOther)
		return
	}
	if _, ok := a.requirePermissionKey(w, r, permKeyTerritoriEclesEdit, PermissionTarget{}); !ok {
		return
	}
	kind := confessionalKind(r.FormValue("kind"))
	id, _ := strconv.Atoi(r.FormValue("id"))
	if kind == "" || id == 0 {
		http.NotFound(w, r)
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
	}
	if err != nil {
		msg := "delete"
		if errors.Is(err, db.ErrUnsafeDelete) {
			msg = "unsafe_delete"
		}
		http.Redirect(w, r, "/territori/confessional?error="+msg, http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/territori/confessional?notice=deleted", http.StatusSeeOther)
}

func (a *App) renderConfessionalForm(w http.ResponseWriter, r *http.Request, user *db.User, data confessionalFormData) {
	religions, _ := a.DB.ListReligioConfessions()
	models, _ := a.DB.ListModelsConfessionals()
	nivells, _ := a.DB.ListNivellsConfessionals()
	entitats, _ := a.DB.ListEntitatsReligioses()
	paisos, _ := a.DB.ListPaisos()
	RenderPrivateTemplate(w, r, "admin-confessional-form.html", map[string]interface{}{
		"Form":            data,
		"Religions":       religions,
		"Models":          models,
		"Nivells":         nivells,
		"Entitats":        entitats,
		"Paisos":          paisos,
		"CanManageArxius": a.canManageAnyDocumentalsModular(user),
		"User":            user,
	})
}

func (a *App) parseConfessionalForm(kind string, id int, r *http.Request) (confessionalFormData, string) {
	data := confessionalFormData{Kind: kind, IsNew: id == 0, ReturnURL: strings.TrimSpace(r.FormValue("return_to"))}
	estat := normalizeConfessionalEstat(r.FormValue("estat"))
	status := normalizeModerationStatus(r.FormValue("moderation_status"))
	switch kind {
	case "religio":
		item := &db.ReligioConfessio{
			ID:             id,
			Nom:            strings.TrimSpace(r.FormValue("nom")),
			PareID:         sqlNullInt(r.FormValue("pare_id")),
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
			Ordre:               ordre,
			NomNivell:           strings.TrimSpace(r.FormValue("nom_nivell")),
			NomPlural:           strings.TrimSpace(r.FormValue("nom_plural")),
			TipusNivell:         strings.TrimSpace(r.FormValue("tipus_nivell")),
			CodiOficial:         strings.TrimSpace(r.FormValue("codi_oficial")),
			ParentID:            sqlNullInt(r.FormValue("parent_id")),
			AnyInici:            sqlNullInt(r.FormValue("any_inici")),
			AnyFi:               sqlNullInt(r.FormValue("any_fi")),
			Estat:               estat,
			Observacions:        strings.TrimSpace(r.FormValue("observacions")),
			ModeracioEstat:      status,
		}
		data.Nivell = item
		if item.ModelConfessionalID == 0 {
			return data, "Cal indicar el model confessional."
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
			Nom:                  strings.TrimSpace(r.FormValue("nom")),
			ReligioConfessioID:   sqlNullInt(r.FormValue("religio_confessio_id")),
			ModelConfessionalID:  sqlNullInt(r.FormValue("model_confessional_id")),
			NivellConfessionalID: sqlNullInt(r.FormValue("nivell_confessional_id")),
			PaisID:               sqlNullInt(r.FormValue("pais_id")),
			ParentID:             sqlNullInt(r.FormValue("parent_id")),
			TipusEntitat:         strings.TrimSpace(r.FormValue("tipus_entitat")),
			TipusEspecific:       strings.TrimSpace(r.FormValue("tipus_especific")),
			AnyInici:             sqlNullInt(r.FormValue("any_inici")),
			AnyFi:                sqlNullInt(r.FormValue("any_fi")),
			Estat:                estat,
			Web:                  strings.TrimSpace(r.FormValue("web")),
			WebWikipedia:         strings.TrimSpace(r.FormValue("web_wikipedia")),
			Territori:            strings.TrimSpace(r.FormValue("territori")),
			Observacions:         strings.TrimSpace(r.FormValue("observacions")),
			ModeracioEstat:       status,
		}
		data.Entitat = item
		if item.Nom == "" {
			return data, "El nom es obligatori."
		}
		if item.ParentID.Valid && item.ID != 0 && item.ParentID.Int64 == int64(item.ID) {
			return data, "L'entitat no pot ser pare de si mateixa."
		}
	}
	return data, ""
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
	default:
		return errors.New("tipus confessional no valid")
	}
}

func confessionalKind(raw string) string {
	switch strings.TrimSpace(raw) {
	case "religio", "model", "nivell", "entitat":
		return strings.TrimSpace(raw)
	default:
		return ""
	}
}

func extractConfessionalPath(path string) (string, int) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i := 0; i+2 < len(parts); i++ {
		if parts[i] != "confessional" {
			continue
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
	case "pendent", "rebutjat":
		return strings.TrimSpace(raw)
	default:
		return "publicat"
	}
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
