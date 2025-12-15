package core

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type rankingRow struct {
	UserID        int
	Name          string
	Points        int
	Initial       string
	PreferredLang string
	Position      int
	PreferredCode string
}

// Ranking públic amb usuaris que tenen el perfil públic.
func (a *App) Ranking(w http.ResponseWriter, r *http.Request) {
	lang := ResolveLang(r)
	user, _ := a.VerificarSessio(r)
	pageSize := 20
	if szStr := r.URL.Query().Get("page_size"); szStr != "" {
		if v, err := strconv.Atoi(szStr); err == nil && v > 0 {
			pageSize = v
		}
	}
	langFilter := strings.TrimSpace(r.URL.Query().Get("lang"))
	page := 1
	if pStr := r.URL.Query().Get("page"); pStr != "" {
		if v, err := strconv.Atoi(pStr); err == nil && v > 0 {
			page = v
		}
	}
	offset := (page - 1) * pageSize
	filter := db.RankingFilter{
		PreferredLang: langFilter,
		Limit:         pageSize,
		Offset:        offset,
		PublicOnly:    true,
	}
	rows, err := a.DB.GetRanking(filter)
	if err != nil {
		http.Error(w, "Error carregant rànquing", http.StatusInternalServerError)
		return
	}
	total, err := a.DB.CountRanking(filter)
	if err != nil {
		http.Error(w, "Error carregant rànquing", http.StatusInternalServerError)
		return
	}
	totalPages := 1
	if pageSize > 0 && total > 0 {
		totalPages = (total + pageSize - 1) / pageSize
	}
	var result []rankingRow
	currentIndex := offset
	for _, row := range rows {
		u, err := a.DB.GetUserByID(row.UserID)
		if err != nil || u == nil {
			continue
		}
		privacy, _ := a.DB.GetPrivacySettings(row.UserID)
		if privacy != nil && !privacy.ProfilePublic {
			continue
		}
		name := strings.TrimSpace(u.Usuari)
		initial := name
		if strings.TrimSpace(initial) != "" {
			runes := []rune(strings.TrimSpace(initial))
			if len(runes) > 0 {
				initial = strings.ToUpper(string(runes[0]))
			}
		}
		result = append(result, rankingRow{
			UserID:        u.ID,
			Name:          name,
			Points:        row.Total,
			Initial:       initial,
			PreferredLang: u.PreferredLang,
			Position:      currentIndex + 1,
			PreferredCode: strings.ToUpper(strings.TrimSpace(u.PreferredLang)),
		})
		currentIndex++
	}
	canManageArxius := a.CanManageArxius(user)
	RenderPrivateTemplateLang(w, r, "ranking.html", lang, map[string]interface{}{
		"Ranking":         result,
		"Page":            page,
		"PageSize":        pageSize,
		"Total":           total,
		"TotalPages":      totalPages,
		"HasPrev":         page > 1,
		"HasNext":         page < totalPages,
		"PrevPage":        page - 1,
		"NextPage":        page + 1,
		"LangFilter":      langFilter,
		"User":            user,
		"CanManageArxius": canManageArxius,
	})
}
