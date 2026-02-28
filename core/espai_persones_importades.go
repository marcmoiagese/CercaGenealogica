package core

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const espaiPeopleDataPerPageDefault = 25

type espaiPersonaDataRowView struct {
	ID              int
	DisplayName     string
	Name            string
	Surname1        string
	Surname2        string
	FullName        string
	Sex             string
	SexLabel        string
	BirthDate       string
	DeathDate       string
	BirthPlace      string
	DeathPlace      string
	Notes           string
	HasMedia        bool
	Visibility      string
	TreeName        string
	ExternalID      string
	LinkedPersonaID int
}

func buildEspaiPeopleDataPageBase(values url.Values) (string, string) {
	query := cloneValues(values)
	query.Del("page")
	query.Del("per_page")
	base := "/espai/persones-importades"
	if len(query) == 0 {
		return base, "?"
	}
	return base + "?" + query.Encode(), "&"
}

func formatEspaiDateDisplay(val string) string {
	val = strings.TrimSpace(val)
	if val == "" {
		return ""
	}
	if formatted := formatDateDisplay(val); formatted != "" {
		return formatted
	}
	return val
}

func espaiSexLabel(lang string, sex string) string {
	sex = strings.ToLower(strings.TrimSpace(sex))
	switch sex {
	case "male":
		label := T(lang, "tree.sex.male")
		if label == "tree.sex.male" {
			return "Home"
		}
		return label
	case "female":
		label := T(lang, "tree.sex.female")
		if label == "tree.sex.female" {
			return "Dona"
		}
		return label
	default:
		label := T(lang, "tree.sex.unknown")
		if label == "tree.sex.unknown" {
			return "Desconegut"
		}
		return label
	}
}

func (a *App) EspaiPersonalPeopleDataPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLang(r)

	filterKeys := []string{"tree", "external_id", "name", "surname1", "surname2", "full_name", "sex", "birth_date", "death_date", "birth_place", "death_place", "notes", "visibility", "has_media", "linked"}
	filterValues := map[string]string{}
	filterMatch := map[string]string{}
	for _, key := range filterKeys {
		paramKey := "f_" + key
		if val := strings.TrimSpace(r.URL.Query().Get(paramKey)); val != "" {
			filterValues[key] = val
			filterMatch[key] = strings.ToLower(val)
		}
	}
	filterOrder := []string{}
	if orderParam := strings.TrimSpace(r.URL.Query().Get("order")); orderParam != "" {
		for _, key := range strings.Split(orderParam, ",") {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			if _, ok := filterMatch[key]; ok {
				filterOrder = append(filterOrder, key)
			}
		}
	}
	if len(filterOrder) == 0 {
		for _, key := range filterKeys {
			if _, ok := filterMatch[key]; ok {
				filterOrder = append(filterOrder, key)
			}
		}
	} else {
		seen := map[string]bool{}
		for _, key := range filterOrder {
			seen[key] = true
		}
		for _, key := range filterKeys {
			if _, ok := filterMatch[key]; ok && !seen[key] {
				filterOrder = append(filterOrder, key)
			}
		}
	}

	sortKey := strings.TrimSpace(r.URL.Query().Get("sort"))
	sortDir := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("dir")))
	if sortDir != "asc" && sortDir != "desc" {
		sortDir = "asc"
	}

	page := parseListPage(r.URL.Query().Get("page"))
	perPage := parseListPerPage(r.URL.Query().Get("per_page"))
	if perPage <= 0 {
		perPage = espaiPeopleDataPerPageDefault
	}

	var hasMedia *bool
	if raw := strings.ToLower(strings.TrimSpace(filterValues["has_media"])); raw != "" {
		val := raw == "1" || raw == "true" || raw == "yes" || raw == "si"
		hasMedia = &val
	}
	var linked *bool
	if raw := strings.ToLower(strings.TrimSpace(filterValues["linked"])); raw != "" {
		val := raw == "1" || raw == "true" || raw == "yes" || raw == "si"
		linked = &val
	}

	dataFilter := db.EspaiPersonaDataFilter{
		Name:       filterValues["name"],
		Surname1:   filterValues["surname1"],
		Surname2:   filterValues["surname2"],
		FullName:   filterValues["full_name"],
		Sex:        filterValues["sex"],
		BirthDate:  filterValues["birth_date"],
		DeathDate:  filterValues["death_date"],
		BirthPlace: filterValues["birth_place"],
		DeathPlace: filterValues["death_place"],
		Notes:      filterValues["notes"],
		Tree:       filterValues["tree"],
		Visibility: filterValues["visibility"],
		HasMedia:   hasMedia,
		Linked:     linked,
		ExternalID: filterValues["external_id"],
		Sort:       sortKey,
		SortDir:    sortDir,
	}

	total, _ := a.DB.CountEspaiPersonesByOwnerDataFilters(user.ID, dataFilter)
	totalPages := 1
	if total > 0 && perPage > 0 {
		totalPages = (total + perPage - 1) / perPage
	}
	if totalPages < 1 {
		totalPages = 1
	}
	if page > totalPages {
		page = totalPages
	}
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * perPage
	if offset < 0 {
		offset = 0
	}
	rows, _ := a.DB.ListEspaiPersonesByOwnerDataFilters(user.ID, dataFilter, perPage, offset)

	linksByPersona := map[int]int{}
	if links, err := a.DB.ListEspaiCoincidenciesByOwner(user.ID); err == nil {
		for _, link := range links {
			if link.PersonaID == 0 || link.TargetID == 0 {
				continue
			}
			if strings.TrimSpace(link.TargetType) != "persona" || strings.TrimSpace(link.Status) != "accepted" {
				continue
			}
			if _, ok := linksByPersona[link.PersonaID]; ok {
				continue
			}
			linksByPersona[link.PersonaID] = link.TargetID
		}
	}

	items := make([]espaiPersonaDataRowView, 0, len(rows))
	for _, row := range rows {
		name := strings.TrimSpace(row.Nom.String)
		surname1 := strings.TrimSpace(row.Cognom1.String)
		surname2 := strings.TrimSpace(row.Cognom2.String)
		fullName := strings.TrimSpace(row.NomComplet.String)
		if fullName == "" {
			fullName = strings.TrimSpace(strings.Join([]string{name, surname1, surname2}, " "))
		}
		displayName := fullName
		if displayName == "" {
			displayName = "?"
		}
		sex := strings.TrimSpace(row.Sexe.String)
		externalID := strings.TrimSpace(row.ExternalID.String)
		if strings.HasPrefix(externalID, "gramps:") {
			externalID = strings.TrimPrefix(externalID, "gramps:")
		}
		visibility := strings.TrimSpace(row.Visibility)
		if visibility == "" {
			visibility = "visible"
		}
		items = append(items, espaiPersonaDataRowView{
			ID:              row.ID,
			DisplayName:     displayName,
			Name:            name,
			Surname1:        surname1,
			Surname2:        surname2,
			FullName:        fullName,
			Sex:             sex,
			SexLabel:        espaiSexLabel(lang, sex),
			BirthDate:       formatEspaiDateDisplay(row.DataNaixement.String),
			DeathDate:       formatEspaiDateDisplay(row.DataDefuncio.String),
			BirthPlace:      strings.TrimSpace(row.LlocNaixement.String),
			DeathPlace:      strings.TrimSpace(row.LlocDefuncio.String),
			Notes:           strings.TrimSpace(row.Notes.String),
			HasMedia:        row.HasMedia,
			Visibility:      visibility,
			TreeName:        strings.TrimSpace(row.TreeName),
			ExternalID:      externalID,
			LinkedPersonaID: linksByPersona[row.ID],
		})
	}

	pageBase, pageSep := buildEspaiPeopleDataPageBase(r.URL.Query())
	pager := espaiPeoplePager{
		Page:       page,
		PerPage:    perPage,
		Total:      total,
		TotalPages: totalPages,
		HasPrev:    page > 1,
		HasNext:    page < totalPages,
		PrevPage:   page - 1,
		NextPage:   page + 1,
		PageBase:   pageBase,
		PageSep:    pageSep,
	}

	RenderPrivateTemplate(w, r, "espai.html", map[string]interface{}{
		"SpaceSection":               "persones_importades",
		"EspaiPeopleData":            items,
		"EspaiPeopleDataPager":       pager,
		"EspaiPeopleDataFilterValues": filterValues,
		"EspaiPeopleDataFilterOrder":  strings.Join(filterOrder, ","),
		"EspaiPeopleDataSort":         sortKey,
		"EspaiPeopleDataSortDir":      sortDir,
		"EspaiPeopleDataPerPage":      perPage,
		"EspaiReturnTo":               r.URL.RequestURI(),
		"UploadError":                 strings.TrimSpace(r.URL.Query().Get("error")),
		"UploadNotice":                strings.TrimSpace(r.URL.Query().Get("notice")),
	})
}
