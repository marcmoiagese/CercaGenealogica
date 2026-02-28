package core

import (
	"net/http"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) EspaiPersonaExternalLinksAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/external-links") {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	espaiPersonaID := extractID(r.URL.Path)
	if espaiPersonaID == 0 {
		http.NotFound(w, r)
		return
	}
	persona, err := a.DB.GetEspaiPersona(espaiPersonaID)
	if err != nil || persona == nil || persona.OwnerUserID != user.ID {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLang(r)
	linkedPersonaID := 0
	if rows, err := a.DB.Query(`SELECT target_id FROM espai_coincidencies WHERE owner_user_id = ? AND persona_id = ? AND target_type = 'persona' AND status = 'accepted' LIMIT 1`, user.ID, espaiPersonaID); err == nil && len(rows) > 0 {
		linkedPersonaID = rowInt(rows[0], "target_id")
	}
	if linkedPersonaID == 0 {
		writeJSON(w, buildExternalLinksAPIResponse(lang, espaiPersonaID, []db.ExternalLinkRow{}))
		return
	}
	publicPersona, err := a.DB.GetPersona(linkedPersonaID)
	if err != nil || publicPersona == nil || strings.TrimSpace(publicPersona.ModeracioEstat) != "publicat" {
		writeJSON(w, buildExternalLinksAPIResponse(lang, espaiPersonaID, []db.ExternalLinkRow{}))
		return
	}
	rows, err := a.DB.ExternalLinksListByPersona(linkedPersonaID, "approved")
	if err != nil {
		http.Error(w, "No s'han pogut carregar els enllacos externs", http.StatusInternalServerError)
		return
	}
	writeJSON(w, buildExternalLinksAPIResponse(lang, espaiPersonaID, rows))
}
