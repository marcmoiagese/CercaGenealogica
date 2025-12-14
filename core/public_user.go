package core

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

// Perfil públic d'un usuari (només si té el perfil marcat com a públic).
func (a *App) PublicUserProfile(w http.ResponseWriter, r *http.Request) {
	lang := ResolveLang(r)
	currentUser, _ := a.VerificarSessio(r)
	idStr := strings.TrimPrefix(r.URL.Path, "/u/")
	userID, err := strconv.Atoi(idStr)
	if err != nil || userID <= 0 {
		http.NotFound(w, r)
		return
	}
	u, err := a.DB.GetUserByID(userID)
	if err != nil || u == nil {
		http.NotFound(w, r)
		return
	}
	privacy, _ := a.DB.GetPrivacySettings(userID)
	if privacy != nil && !privacy.ProfilePublic {
		http.NotFound(w, r)
		return
	}
	showName := privacy == nil || privacy.NomVisibility == "public" || privacy.CognomsVisibility == "public"
	showEmail := privacy == nil || privacy.EmailVisibility == "public"
	showPais := privacy == nil || privacy.PaisVisibility == "public"
	showPoblacio := privacy == nil || privacy.PoblacioVisibility == "public"
	showLanguages := privacy == nil || privacy.PreferredLangVisibility == "public"
	showSpoken := privacy == nil || privacy.SpokenLangsVisibility == "public"
	showPhone := privacy == nil || privacy.PhoneVisibility == "public"
	showActivity := privacy == nil || privacy.ShowActivity
	points, _ := a.DB.GetUserPoints(userID)
	totalPoints := 0
	if points != nil {
		totalPoints = points.Total
	}
	activities := []db.UserActivity{}
	var heatmap []map[string]interface{}
	heatTotal := 0
	if showActivity {
		activities, _ = a.DB.ListUserActivityByUser(userID, db.ActivityFilter{
			Status: "validat",
			Limit:  10,
		})
		heatmap, heatTotal = buildHeatmap(a.DB, userID, lang)
	}
	username := u.Usuari
	var realName string
	if showName && strings.TrimSpace(u.Name) != "" {
		realName = strings.TrimSpace(u.Name)
		if strings.TrimSpace(u.Surname) != "" {
			realName += " " + strings.TrimSpace(u.Surname)
		}
	}
	initial := username
	if strings.TrimSpace(initial) != "" {
		runes := []rune(strings.TrimSpace(initial))
		if len(runes) > 0 {
			initial = strings.ToUpper(string(runes[0]))
		}
	} else if strings.TrimSpace(realName) != "" {
		runes := []rune(strings.TrimSpace(realName))
		if len(runes) > 0 {
			initial = strings.ToUpper(string(runes[0]))
		}
	} else {
		initial = "?"
	}
	var spoken []string
	if showSpoken && strings.TrimSpace(u.SpokenLangs) != "" {
		parts := strings.Split(u.SpokenLangs, ",")
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				spoken = append(spoken, p)
			}
		}
	}

	RenderPrivateTemplateLang(w, r, "user-public.html", lang, map[string]interface{}{
		"ProfileUser":   u,
		"Username":      username,
		"RealName":      realName,
		"ShowRealName":  realName != "",
		"Name":          username,
		"Initial":       initial,
		"Points":        totalPoints,
		"Activities":    activities,
		"ShowActivity":  showActivity,
		"Heatmap":       heatmap,
		"HeatmapTotal":  heatTotal,
		"ShowEmail":     showEmail,
		"ShowPais":      showPais,
		"ShowPoblacio":  showPoblacio,
		"ShowLanguages": showLanguages,
		"ShowSpoken":    showSpoken,
		"SpokenLangs":   spoken,
		"PreferredCode": strings.ToUpper(strings.TrimSpace(u.PreferredLang)),
		"ShowPhone":     showPhone,
		"User":          currentUser,
		"CanManageArxius": a.CanManageArxius(currentUser),
	})
}
