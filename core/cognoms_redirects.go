package core

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type cognomRedirectDetailView struct {
	FromID    int
	FromLabel string
	Reason    string
	CreatedAt string
	CreatedBy string
}

func (a *App) buildCognomRedirectViews(redirects []db.CognomRedirect) []cognomRedirectDetailView {
	if len(redirects) == 0 {
		return nil
	}
	cognomCache := map[int]string{}
	userCache := map[int]string{}
	resolveCognom := func(id int) string {
		if id <= 0 {
			return ""
		}
		if cached, ok := cognomCache[id]; ok {
			return cached
		}
		if row, err := a.DB.GetCognom(id); err == nil && row != nil {
			cognomCache[id] = strings.TrimSpace(row.Forma)
		} else {
			cognomCache[id] = ""
		}
		return cognomCache[id]
	}
	resolveUser := func(id sql.NullInt64) string {
		if !id.Valid {
			return ""
		}
		uid := int(id.Int64)
		if cached, ok := userCache[uid]; ok {
			return cached
		}
		if row, err := a.DB.GetUserByID(uid); err == nil && row != nil {
			name := strings.TrimSpace(row.Usuari)
			if name == "" {
				name = strings.TrimSpace(strings.TrimSpace(row.Name) + " " + strings.TrimSpace(row.Surname))
			}
			userCache[uid] = name
		} else {
			userCache[uid] = ""
		}
		return userCache[uid]
	}
	out := make([]cognomRedirectDetailView, 0, len(redirects))
	for _, row := range redirects {
		created := ""
		if row.CreatedAt.Valid {
			created = row.CreatedAt.Time.Format("2006-01-02 15:04")
		}
		label := resolveCognom(row.FromCognomID)
		if strings.TrimSpace(label) == "" {
			label = fmt.Sprintf("Cognom %d", row.FromCognomID)
		}
		out = append(out, cognomRedirectDetailView{
			FromID:    row.FromCognomID,
			FromLabel: label,
			Reason:    strings.TrimSpace(row.Reason),
			CreatedAt: created,
			CreatedBy: resolveUser(row.CreatedBy),
		})
	}
	return out
}
