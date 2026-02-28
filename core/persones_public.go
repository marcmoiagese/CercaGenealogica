package core

import (
	"net/http"
	"strings"
)

// Vista publica de persona (nomes nom, sense dades sensibles).
func (a *App) PersonaPublic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}

	lang := ResolveLang(r)
	id := extractID(r.URL.Path)
	if id == 0 {
		http.Error(w, T(lang, "public.person.not_found"), http.StatusNotFound)
		return
	}
	p, err := a.DB.GetPersona(id)
	status := ""
	if p != nil {
		status = strings.TrimSpace(p.ModeracioEstat)
	}
	if err != nil || p == nil || (status != "" && status != "publicat") {
		http.Error(w, T(lang, "public.person.not_found"), http.StatusNotFound)
		return
	}

	name := strings.TrimSpace(strings.Join([]string{p.Nom, p.Cognom1, p.Cognom2}, " "))
	if name == "" {
		name = strings.TrimSpace(p.NomComplet)
	}
	if name == "" {
		name = T(lang, "tree.unknown.name")
	}

	RenderTemplate(w, r, "persona-public.html", map[string]interface{}{
		"PersonaID":   p.ID,
		"PersonaName": name,
	})
}

// Vista publica de l'arbre (placeholder). De moment només retorna 501.
func (a *App) PersonaPublicArbre(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}

	lang := ResolveLang(r)
	id := extractID(r.URL.Path)
	if id == 0 {
		http.Error(w, T(lang, "public.person.not_found"), http.StatusNotFound)
		return
	}
	p, err := a.DB.GetPersona(id)
	status := ""
	if p != nil {
		status = strings.TrimSpace(p.ModeracioEstat)
	}
	if err != nil || p == nil || (status != "" && status != "publicat") {
		http.Error(w, T(lang, "public.person.not_found"), http.StatusNotFound)
		return
	}

	view := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("view")))
	if view != "familiar" && view != "ventall" {
		view = "pedigree"
	}
	gens := parseTreeGens(r.URL.Query().Get("gens"), treeDefaultGens)

	fullName := strings.TrimSpace(strings.Join([]string{p.Nom, p.Cognom1, p.Cognom2}, " "))
	if fullName == "" {
		fullName = strings.TrimSpace(p.NomComplet)
	}
	if fullName == "" {
		fullName = T(lang, "tree.unknown.name")
	}

	var dataset treeDataset
	if view == "familiar" {
		dataset, err = a.buildFamiliarArbreDataset(p, gens)
	} else {
		dataset, err = a.buildPersonaArbreDataset(p, gens)
	}
	if err != nil {
		http.Error(w, "Error carregant arbre", http.StatusInternalServerError)
		return
	}

	ids := make([]int, 0, len(dataset.FamilyData))
	for _, person := range dataset.FamilyData {
		if person.ID > 0 {
			ids = append(ids, person.ID)
		}
	}
	valid := map[int]bool{}
	if len(ids) > 0 {
		if personsByID, err := a.DB.GetPersonesByIDs(ids); err == nil {
			for _, id := range ids {
				if p := personsByID[id]; p != nil && (strings.TrimSpace(p.ModeracioEstat) == "" || p.ModeracioEstat == "publicat") {
					valid[id] = true
				}
			}
		}
	}

	filteredPeople := make([]treePerson, 0, len(dataset.FamilyData))
	for _, person := range dataset.FamilyData {
		if person.ID <= 0 {
			continue
		}
		if !valid[person.ID] {
			continue
		}
		person.Sex = 2
		person.Birth = ""
		person.BirthPlace = ""
		person.Death = ""
		person.DeathPlace = ""
		person.Occupation = ""
		filteredPeople = append(filteredPeople, person)
	}

	filteredLinks := make([]treeLink, 0, len(dataset.FamilyLinks))
	for _, link := range dataset.FamilyLinks {
		if !valid[link.Child] {
			continue
		}
		father := link.Father
		mother := link.Mother
		if father > 0 && !valid[father] {
			father = 0
		}
		if mother > 0 && !valid[mother] {
			mother = 0
		}
		if father == 0 && mother == 0 {
			continue
		}
		filteredLinks = append(filteredLinks, treeLink{Child: link.Child, Father: father, Mother: mother})
	}

	dataset.FamilyData = filteredPeople
	dataset.FamilyLinks = filteredLinks
	dataset.DatasetStats = treeDatasetStats{People: len(filteredPeople), Links: len(filteredLinks)}

	treeKeys := []string{
		"tree.dataset",
		"tree.visible",
		"tree.error.d3",
		"tree.error.root",
		"tree.error.expand",
		"tree.unknown.name",
		"tree.unknown.person",
		"tree.placeholder.father",
		"tree.placeholder.mother",
		"tree.drawer.section",
		"tree.drawer.empty",
		"tree.drawer.birth",
		"tree.drawer.birth_place",
		"tree.drawer.death",
		"tree.drawer.death_place",
		"tree.drawer.occupation",
		"tree.drawer.sex",
		"tree.drawer.select_person",
		"tree.drawer.segment_hint",
		"tree.drawer.no_extra",
		"tree.drawer.open_profile",
		"tree.sex.male",
		"tree.sex.female",
		"tree.sex.unknown",
		"tree.fan.birth_prefix",
		"tree.fan.death_prefix",
		"tree.controls.view_switch_aria",
		"tree.controls.generations",
		"tree.controls.generation_singular",
		"tree.controls.generation_plural",
		"tree.controls.zoom_in",
		"tree.controls.zoom_out",
		"tree.controls.fit",
		"tree.controls.dataset_title",
		"tree.controls.visible_title",
		"tree.controls.drawer_title",
		"tree.controls.drawer",
		"tree.controls.view_person",
		"tree.aria.fan",
		"tree.aria.tree",
		"tree.drawer.close",
		"tree.drawer.click_hint_node",
		"tree.view.pedigree",
		"tree.view.familiar",
		"tree.view.ventall",
		"tree.title",
	}
	treeI18n := map[string]string{}
	for _, key := range treeKeys {
		treeI18n[key] = T(lang, key)
	}

	RenderTemplate(w, r, "persona-public-arbre.html", map[string]interface{}{
		"Persona":      p,
		"PersonaName":  fullName,
		"View":         view,
		"Gens":         gens,
		"FamilyData":   dataset.FamilyData,
		"FamilyLinks":  dataset.FamilyLinks,
		"RootPersonId": dataset.RootPersonID,
		"DatasetStats": dataset.DatasetStats,
		"TreeI18n":     treeI18n,
	})
}
