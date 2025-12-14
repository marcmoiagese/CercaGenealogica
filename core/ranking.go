package core

import "net/http"

type rankingRow struct {
	Name   string
	Points int
}

// Ranking públic amb usuaris que tenen el perfil públic.
func (a *App) Ranking(w http.ResponseWriter, r *http.Request) {
	lang := ResolveLang(r)
	user, _ := a.VerificarSessio(r)
	rows, err := a.DB.GetRanking(50)
	if err != nil {
		http.Error(w, "Error carregant rànquing", http.StatusInternalServerError)
		return
	}
	var result []rankingRow
	for _, row := range rows {
		u, err := a.DB.GetUserByID(row.UserID)
		if err != nil || u == nil {
			continue
		}
		privacy, _ := a.DB.GetPrivacySettings(row.UserID)
		if privacy != nil && !privacy.ProfilePublic {
			continue
		}
		name := u.Usuari
		if u.Name != "" {
			name = u.Name
			if u.Surname != "" {
				name += " " + u.Surname
			}
		}
		result = append(result, rankingRow{Name: name, Points: row.Total})
	}
	canManageArxius := a.CanManageArxius(user)
	RenderPrivateTemplateLang(w, r, "ranking.html", lang, map[string]interface{}{
		"Ranking":         result,
		"User":            user,
		"CanManageArxius": canManageArxius,
	})
}
