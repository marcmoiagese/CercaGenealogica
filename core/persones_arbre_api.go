package core

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	treeDefaultGens = 3
	treeMaxGens     = 7
	treeMaxNodes    = 5000
)

type treePerson struct {
	ID         int    `json:"id"`
	Name       string `json:"name"`
	Sex        int    `json:"sex"`
	Birth      string `json:"birth,omitempty"`
	BirthPlace string `json:"birth_place,omitempty"`
	Death      string `json:"death,omitempty"`
	DeathPlace string `json:"death_place,omitempty"`
	Occupation string `json:"occupation,omitempty"`
	Hidden     bool   `json:"hidden,omitempty"`
}

type treeLink struct {
	Child  int `json:"child"`
	Father int `json:"father,omitempty"`
	Mother int `json:"mother,omitempty"`
}

type treeDatasetStats struct {
	People int `json:"people"`
	Links  int `json:"links"`
}

type treeDataset struct {
	FamilyData   []treePerson     `json:"familyData"`
	FamilyLinks  []treeLink       `json:"familyLinks"`
	RootPersonID int              `json:"rootPersonId"`
	DatasetStats treeDatasetStats `json:"__DATASET_STATS"`
}

type parentPair struct {
	Father int
	Mother int
}

var treeFatherRoles = map[string]struct{}{
	"pare":    {},
	"padre":   {},
	"father":  {},
	"genitor": {},
}

var treeMotherRoles = map[string]struct{}{
	"mare":     {},
	"madre":    {},
	"mother":   {},
	"genitora": {},
}

var treeSubjectRoles = map[string]struct{}{
	"batejat":         {},
	"baptizat":        {},
	"infant":          {},
	"infante":         {},
	"baptism":         {},
	"nascut":          {},
	"nascuda":         {},
	"nacido":          {},
	"nacida":          {},
	"personaprincipal": {},
}

func normalizeTreeToken(val string) string {
	val = strings.ToLower(strings.TrimSpace(val))
	val = strings.ReplaceAll(val, "_", "")
	val = strings.ReplaceAll(val, " ", "")
	val = strings.ReplaceAll(val, "-", "")
	val = strings.ReplaceAll(val, ".", "")
	return val
}

func roleMatchesTree(role string, set map[string]struct{}) bool {
	if role == "" {
		return false
	}
	_, ok := set[normalizeTreeToken(role)]
	return ok
}

func rawPersonName(raw db.TranscripcioPersonaRaw, fallback string) string {
	name := strings.TrimSpace(strings.Join([]string{raw.Nom, raw.Cognom1, raw.Cognom2}, " "))
	if name == "" {
		name = strings.TrimSpace(raw.Rol)
	}
	if name == "" {
		name = fallback
	}
	return name
}

func parseTreeGens(val string, def int) int {
	if def <= 0 {
		def = treeDefaultGens
	}
	if def > treeMaxGens {
		def = treeMaxGens
	}
	if strings.TrimSpace(val) == "" {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(val))
	if err != nil || n <= 0 {
		return def
	}
	if n > treeMaxGens {
		return treeMaxGens
	}
	return n
}

func sexFromRaw(val string) int {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "masculi", "masculí", "home", "m":
		return 0
	case "femeni", "femení", "dona", "f":
		return 1
	default:
		return 2
	}
}

func personaDisplayName(p *db.Persona) string {
	if p == nil {
		return "?"
	}
	name := strings.TrimSpace(strings.Join([]string{p.Nom, p.Cognom1, p.Cognom2}, " "))
	if name == "" {
		name = strings.TrimSpace(p.NomComplet)
	}
	if name == "" {
		name = "?"
	}
	return name
}

func (a *App) fillTreePersonFromRegistres(personaID int, base treePerson) treePerson {
	needBirth := strings.TrimSpace(base.Birth) == ""
	needBirthPlace := strings.TrimSpace(base.BirthPlace) == ""
	needDeath := strings.TrimSpace(base.Death) == ""
	needDeathPlace := strings.TrimSpace(base.DeathPlace) == ""
	needOccupation := strings.TrimSpace(base.Occupation) == ""
	needSex := base.Sex == 2

	if !(needBirth || needBirthPlace || needDeath || needDeathPlace || needOccupation || needSex) {
		return base
	}

	transID := 0
	if bestID, ok, _ := a.DB.FindBestBaptismeTranscripcioForPersona(personaID); ok && bestID > 0 {
		transID = bestID
	} else if registres, err := a.DB.ListRegistresByPersona(personaID, ""); err == nil {
		for _, row := range registres {
			if !roleMatchesTree(row.Rol, treeSubjectRoles) {
				continue
			}
			tipus := normalizeRole(row.TipusActe)
			switch tipus {
			case "baptisme", "bateig", "bautismo", "baptism", "naixement", "naixament", "nacimiento":
				transID = row.RegistreID
			}
			if transID > 0 {
				break
			}
		}
	}
	if transID > 0 {
		attrs, _ := a.DB.ListTranscripcioAtributs(transID)
		reg, _ := a.DB.GetTranscripcioRaw(transID)
		persones, _ := a.DB.ListTranscripcioPersones(transID)
		var raw *db.TranscripcioPersonaRaw
		for i := range persones {
			if persones[i].PersonaID.Valid && int(persones[i].PersonaID.Int64) == personaID {
				raw = &persones[i]
				break
			}
		}
		if needSex && raw != nil {
			base.Sex = sexFromRaw(raw.Sexe)
			needSex = base.Sex == 2
		}
		if needOccupation && raw != nil {
			base.Occupation = strings.TrimSpace(raw.OficiText)
			needOccupation = strings.TrimSpace(base.Occupation) == ""
		}
		if needBirthPlace && raw != nil {
			base.BirthPlace = strings.TrimSpace(raw.MunicipiText)
			needBirthPlace = strings.TrimSpace(base.BirthPlace) == ""
		}
		if needBirth {
			if d := attrValueByKeysRaw(attrs,
				"data_naixement", "datanaixement", "naixement",
				"data_naixament", "datanaixament", "naixament",
				"nascut", "data_nascut", "datanascut",
			); d != "" {
				base.Birth = formatDateDisplay(d)
			}
			if base.Birth == "" {
				if d := attrValueByKeysRaw(attrs,
					"data_bateig", "databateig",
					"data_baptisme", "databaptisme",
					"bateig", "baptisme", "databapt", "data_bapt",
				); d != "" {
					base.Birth = formatDateDisplay(d)
				}
			}
			if base.Birth == "" && reg != nil {
				date := ""
				if reg.DataActeISO.Valid {
					date = strings.TrimSpace(reg.DataActeISO.String)
				}
				if date == "" {
					date = strings.TrimSpace(reg.DataActeText)
				}
				if date != "" {
					base.Birth = formatDateDisplay(date)
				}
			}
			needBirth = strings.TrimSpace(base.Birth) == ""
		}
	}

	if needDeath || needDeathPlace || needOccupation || needSex {
		if registres, err := a.DB.ListRegistresByPersona(personaID, ""); err == nil {
			for _, row := range registres {
				tipus := normalizeRole(row.TipusActe)
				if tipus != "defuncio" && tipus != "obit" {
					continue
				}
				attrs, _ := a.DB.ListTranscripcioAtributs(row.RegistreID)
				reg, _ := a.DB.GetTranscripcioRaw(row.RegistreID)
				persones, _ := a.DB.ListTranscripcioPersones(row.RegistreID)
				var raw *db.TranscripcioPersonaRaw
				for i := range persones {
					if persones[i].PersonaID.Valid && int(persones[i].PersonaID.Int64) == personaID {
						raw = &persones[i]
						break
					}
				}
				if needSex && raw != nil {
					base.Sex = sexFromRaw(raw.Sexe)
					needSex = base.Sex == 2
				}
				if needOccupation && raw != nil && strings.TrimSpace(base.Occupation) == "" {
					base.Occupation = strings.TrimSpace(raw.OficiText)
					needOccupation = strings.TrimSpace(base.Occupation) == ""
				}
				if needDeathPlace && raw != nil {
					base.DeathPlace = strings.TrimSpace(raw.MunicipiText)
					needDeathPlace = strings.TrimSpace(base.DeathPlace) == ""
				}
				if needDeath {
					if d := attrValueByKeysRaw(attrs, "data_defuncio", "datadefuncio", "defuncio"); d != "" {
						base.Death = formatDateDisplay(d)
					}
					if base.Death == "" && reg != nil {
						date := ""
						if reg.DataActeISO.Valid {
							date = strings.TrimSpace(reg.DataActeISO.String)
						}
						if date == "" {
							date = strings.TrimSpace(reg.DataActeText)
						}
						if date != "" {
							base.Death = formatDateDisplay(date)
						}
					}
					needDeath = strings.TrimSpace(base.Death) == ""
				}
				break
			}
		}
	}

	return base
}

func (a *App) loadParentsForPersona(personaID int, cache map[int]parentPair, pseudo map[int]treePerson) (parentPair, error) {
	if cache != nil {
		if pair, ok := cache[personaID]; ok {
			return pair, nil
		}
	}
	pair := parentPair{}
	type candidate struct {
		ID    int
		Bonus int
	}
	var candidates []candidate
	if bestID, ok, err := a.DB.FindBestBaptismeTranscripcioForPersona(personaID); err != nil {
		return pair, err
	} else if ok && bestID > 0 {
		candidates = append(candidates, candidate{ID: bestID, Bonus: 2})
	}
	if registres, err := a.DB.ListRegistresByPersona(personaID, ""); err == nil {
		for _, row := range registres {
			if !roleMatchesTree(row.Rol, treeSubjectRoles) {
				continue
			}
			bonus := 0
			tipus := normalizeRole(row.TipusActe)
			switch tipus {
			case "baptisme", "bateig", "bautismo", "baptism", "naixement", "naixament", "nacimiento":
				bonus = 1
			}
			candidates = append(candidates, candidate{ID: row.RegistreID, Bonus: bonus})
		}
	}
	if len(candidates) == 0 {
		if cache != nil {
			cache[personaID] = pair
		}
		return pair, nil
	}

	seen := map[int]bool{}
	bestScore := -1
	bestPair := parentPair{}

	extractParents := func(transID int) (parentPair, error) {
		persones, err := a.DB.ListTranscripcioPersones(transID)
		if err != nil {
			return parentPair{}, err
		}
		fatherID := 0
		motherID := 0
		for _, p := range persones {
			if fatherID == 0 && roleMatchesTree(p.Rol, treeFatherRoles) {
				if p.PersonaID.Valid && p.PersonaID.Int64 > 0 {
					fatherID = int(p.PersonaID.Int64)
				} else if p.ID > 0 {
					fatherID = -p.ID
					if pseudo != nil {
						sex := sexFromRaw(p.Sexe)
						if sex == 2 {
							sex = 0
						}
						pseudo[fatherID] = treePerson{
							ID:         fatherID,
							Name:       rawPersonName(p, "Pare"),
							Sex:        sex,
							BirthPlace: strings.TrimSpace(p.MunicipiText),
							Occupation: strings.TrimSpace(p.OficiText),
						}
					}
				}
			}
			if motherID == 0 && roleMatchesTree(p.Rol, treeMotherRoles) {
				if p.PersonaID.Valid && p.PersonaID.Int64 > 0 {
					motherID = int(p.PersonaID.Int64)
				} else if p.ID > 0 {
					motherID = -p.ID
					if pseudo != nil {
						sex := sexFromRaw(p.Sexe)
						if sex == 2 {
							sex = 1
						}
						pseudo[motherID] = treePerson{
							ID:         motherID,
							Name:       rawPersonName(p, "Mare"),
							Sex:        sex,
							BirthPlace: strings.TrimSpace(p.MunicipiText),
							Occupation: strings.TrimSpace(p.OficiText),
						}
					}
				}
			}
			if fatherID != 0 && motherID != 0 {
				break
			}
		}
		return parentPair{Father: fatherID, Mother: motherID}, nil
	}

	for _, cand := range candidates {
		if cand.ID <= 0 || seen[cand.ID] {
			continue
		}
		seen[cand.ID] = true
		found, err := extractParents(cand.ID)
		if err != nil {
			return pair, err
		}
		score := cand.Bonus
		if found.Father != 0 {
			score++
		}
		if found.Mother != 0 {
			score++
		}
		if score > bestScore {
			bestScore = score
			bestPair = found
		}
	}

	pair = bestPair
	if cache != nil {
		cache[personaID] = pair
	}
	return pair, nil
}

func (a *App) loadChildrenForPersona(personaID int, pseudo map[int]treePerson) ([]treeLink, error) {
	rows, err := a.DB.ListRegistresByPersona(personaID, "")
	if err != nil {
		return nil, err
	}
	links := []treeLink{}

	for _, row := range rows {
		isFather := roleMatchesTree(row.Rol, treeFatherRoles)
		isMother := roleMatchesTree(row.Rol, treeMotherRoles)
		if !isFather && !isMother {
			continue
		}

		persones, err := a.DB.ListTranscripcioPersones(row.RegistreID)
		if err != nil {
			return nil, err
		}

		fatherID := 0
		motherID := 0
		childIDs := map[int]struct{}{}

		for _, p := range persones {
			if fatherID == 0 && roleMatchesTree(p.Rol, treeFatherRoles) {
				if p.PersonaID.Valid && p.PersonaID.Int64 > 0 {
					fatherID = int(p.PersonaID.Int64)
				} else if p.ID > 0 {
					fatherID = -p.ID
					if pseudo != nil {
						sex := sexFromRaw(p.Sexe)
						if sex == 2 {
							sex = 0
						}
						if _, ok := pseudo[fatherID]; !ok {
							pseudo[fatherID] = treePerson{
								ID:         fatherID,
								Name:       rawPersonName(p, "Pare"),
								Sex:        sex,
								BirthPlace: strings.TrimSpace(p.MunicipiText),
								Occupation: strings.TrimSpace(p.OficiText),
							}
						}
					}
				}
			}
			if motherID == 0 && roleMatchesTree(p.Rol, treeMotherRoles) {
				if p.PersonaID.Valid && p.PersonaID.Int64 > 0 {
					motherID = int(p.PersonaID.Int64)
				} else if p.ID > 0 {
					motherID = -p.ID
					if pseudo != nil {
						sex := sexFromRaw(p.Sexe)
						if sex == 2 {
							sex = 1
						}
						if _, ok := pseudo[motherID]; !ok {
							pseudo[motherID] = treePerson{
								ID:         motherID,
								Name:       rawPersonName(p, "Mare"),
								Sex:        sex,
								BirthPlace: strings.TrimSpace(p.MunicipiText),
								Occupation: strings.TrimSpace(p.OficiText),
							}
						}
					}
				}
			}
			if roleMatchesTree(p.Rol, treeSubjectRoles) {
				childID := 0
				if p.PersonaID.Valid && p.PersonaID.Int64 > 0 {
					childID = int(p.PersonaID.Int64)
				} else if p.ID > 0 {
					childID = -p.ID
					if pseudo != nil {
						sex := sexFromRaw(p.Sexe)
						if _, ok := pseudo[childID]; !ok {
							pseudo[childID] = treePerson{
								ID:         childID,
								Name:       rawPersonName(p, "Infant"),
								Sex:        sex,
								BirthPlace: strings.TrimSpace(p.MunicipiText),
								Occupation: strings.TrimSpace(p.OficiText),
							}
						}
					}
				}
				if childID != 0 {
					childIDs[childID] = struct{}{}
				}
			}
		}

		if isFather {
			fatherID = personaID
		}
		if isMother {
			motherID = personaID
		}

		for childID := range childIDs {
			links = append(links, treeLink{
				Child:  childID,
				Father: fatherID,
				Mother: motherID,
			})
		}
	}

	return links, nil
}

func (a *App) buildPersonaArbreDataset(root *db.Persona, gens int) (treeDataset, error) {
	dataset := treeDataset{}
	if root == nil {
		return dataset, nil
	}

	gens = parseTreeGens("", gens)
	maxDepth := gens - 1
	if maxDepth < 0 {
		maxDepth = 0
	}

	visited := map[int]bool{root.ID: true}
	queue := []struct {
		ID    int
		Depth int
	}{{ID: root.ID, Depth: 0}}

	links := make([]treeLink, 0, gens*2)
	parentCache := map[int]parentPair{}
	pseudoPeople := map[int]treePerson{}

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		if item.Depth >= maxDepth {
			continue
		}

		parents, err := a.loadParentsForPersona(item.ID, parentCache, pseudoPeople)
		if err != nil {
			return dataset, err
		}

		if parents.Father != 0 || parents.Mother != 0 {
			links = append(links, treeLink{
				Child:  item.ID,
				Father: parents.Father,
				Mother: parents.Mother,
			})
		}

		for _, pid := range []int{parents.Father, parents.Mother} {
			if pid <= 0 {
				continue
			}
			if len(visited) >= treeMaxNodes {
				break
			}
			if !visited[pid] {
				visited[pid] = true
				queue = append(queue, struct {
					ID    int
					Depth int
				}{ID: pid, Depth: item.Depth + 1})
			}
		}
		if len(visited) >= treeMaxNodes {
			break
		}
	}

	ids := make([]int, 0, len(visited))
	for id := range visited {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	personsByID, err := a.DB.GetPersonesByIDs(ids)
	if err != nil {
		return dataset, err
	}

	valid := map[int]bool{}
	people := make([]treePerson, 0, len(ids))
	for _, id := range ids {
		p := personsByID[id]
		if p == nil {
			continue
		}

		birth := ""
		if p.DataNaixement.Valid {
			birth = formatDateDisplay(p.DataNaixement.String)
		} else if p.DataBateig.Valid {
			birth = formatDateDisplay(p.DataBateig.String)
		}
		death := ""
		if p.DataDefuncio.Valid {
			death = formatDateDisplay(p.DataDefuncio.String)
		}

		person := treePerson{
			ID:         p.ID,
			Name:       personaDisplayName(p),
			Sex:        2,
			Birth:      birth,
			BirthPlace: strings.TrimSpace(p.MunicipiNaixement),
			Death:      death,
			DeathPlace: strings.TrimSpace(p.MunicipiDefuncio),
			Occupation: strings.TrimSpace(p.Ofici),
		}
		if strings.TrimSpace(person.BirthPlace) == "" {
			person.BirthPlace = strings.TrimSpace(p.Municipi)
		}
		person = a.fillTreePersonFromRegistres(p.ID, person)
		people = append(people, person)
		valid[p.ID] = true
	}
	for _, pseudo := range pseudoPeople {
		if _, exists := valid[pseudo.ID]; exists {
			continue
		}
		people = append(people, pseudo)
		valid[pseudo.ID] = true
	}

	filteredLinks := make([]treeLink, 0, len(links))
	for _, l := range links {
		if !valid[l.Child] {
			continue
		}
		father := l.Father
		mother := l.Mother
		if father > 0 && !valid[father] {
			father = 0
		}
		if mother > 0 && !valid[mother] {
			mother = 0
		}
		if father == 0 && mother == 0 {
			continue
		}
		filteredLinks = append(filteredLinks, treeLink{Child: l.Child, Father: father, Mother: mother})
	}

	dataset = treeDataset{
		FamilyData:   people,
		FamilyLinks:  filteredLinks,
		RootPersonID: root.ID,
		DatasetStats: treeDatasetStats{People: len(people), Links: len(filteredLinks)},
	}

	return dataset, nil
}

func (a *App) buildFamiliarArbreDataset(root *db.Persona, gens int) (treeDataset, error) {
	dataset := treeDataset{}
	if root == nil {
		return dataset, nil
	}

	gens = parseTreeGens("", gens)
	maxDepth := gens - 1
	if maxDepth < 0 {
		maxDepth = 0
	}
	if maxDepth > 3 {
		maxDepth = 3
	}

	type linkKey struct {
		Child  int
		Father int
		Mother int
	}

	peopleIDs := map[int]bool{root.ID: true}
	linksMap := map[linkKey]treeLink{}
	parentCache := map[int]parentPair{}
	pseudoPeople := map[int]treePerson{}
	childrenCache := map[int][]treeLink{}
	primaryParents := map[int]parentPair{}

	addLink := func(link treeLink) {
		if link.Child == 0 {
			return
		}
		if link.Father == 0 && link.Mother == 0 {
			return
		}
		key := linkKey{Child: link.Child, Father: link.Father, Mother: link.Mother}
		if _, ok := linksMap[key]; ok {
			return
		}
		linksMap[key] = link
		if link.Child > 0 {
			peopleIDs[link.Child] = true
		}
		if link.Father > 0 {
			peopleIDs[link.Father] = true
		}
		if link.Mother > 0 {
			peopleIDs[link.Mother] = true
		}
	}

	getChildrenLinks := func(personID int) ([]treeLink, error) {
		if links, ok := childrenCache[personID]; ok {
			return links, nil
		}
		links, err := a.loadChildrenForPersona(personID, pseudoPeople)
		if err != nil {
			return nil, err
		}
		childrenCache[personID] = links
		return links, nil
	}

	visited := map[int]bool{root.ID: true}
	ancestorDepth := map[int]int{root.ID: 0}
	queue := []struct {
		ID    int
		Depth int
	}{{ID: root.ID, Depth: 0}}
	directAncestors := map[int]struct{}{}

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		if item.Depth >= maxDepth {
			continue
		}

		parents, err := a.loadParentsForPersona(item.ID, parentCache, pseudoPeople)
		if err != nil {
			return dataset, err
		}

		if parents.Father != 0 || parents.Mother != 0 {
			addLink(treeLink{
				Child:  item.ID,
				Father: parents.Father,
				Mother: parents.Mother,
			})
			if _, ok := primaryParents[item.ID]; !ok {
				primaryParents[item.ID] = parents
			}
		}

		for _, pid := range []int{parents.Father, parents.Mother} {
			if pid <= 0 {
				continue
			}
			if item.Depth+1 <= 2 {
				directAncestors[pid] = struct{}{}
			}
			if depth, ok := ancestorDepth[pid]; !ok || item.Depth+1 < depth {
				ancestorDepth[pid] = item.Depth + 1
			}
			if !visited[pid] {
				if len(visited)+len(pseudoPeople) >= treeMaxNodes {
					break
				}
				visited[pid] = true
				queue = append(queue, struct {
					ID    int
					Depth int
				}{ID: pid, Depth: item.Depth + 1})
			}
		}
		if len(visited)+len(pseudoPeople) >= treeMaxNodes {
			break
		}
	}

	if links, err := getChildrenLinks(root.ID); err != nil {
		return dataset, err
	} else {
		for _, link := range links {
			if link.Child > 0 && link.Father != 0 && link.Mother != 0 {
				if _, ok := primaryParents[link.Child]; !ok {
					primaryParents[link.Child] = parentPair{Father: link.Father, Mother: link.Mother}
				}
			}
			addLink(link)
		}
	}

	for ancestorID := range directAncestors {
		if ancestorID <= 0 {
			continue
		}
		downVisited := map[int]bool{ancestorID: true}
		downQueue := []struct {
			ID    int
			Depth int
		}{{ID: ancestorID, Depth: 0}}

		for len(downQueue) > 0 {
			item := downQueue[0]
			downQueue = downQueue[1:]
			if item.Depth >= 2 {
				continue
			}
			links, err := getChildrenLinks(item.ID)
			if err != nil {
				return dataset, err
			}
			for _, link := range links {
				if link.Child > 0 && link.Father != 0 && link.Mother != 0 {
					if _, ok := primaryParents[link.Child]; !ok {
						if _, okF := ancestorDepth[link.Father]; okF {
							primaryParents[link.Child] = parentPair{Father: link.Father, Mother: link.Mother}
						} else if _, okM := ancestorDepth[link.Mother]; okM {
							primaryParents[link.Child] = parentPair{Father: link.Father, Mother: link.Mother}
						}
					}
				}
				addLink(link)
				if link.Child > 0 && !downVisited[link.Child] {
					if len(peopleIDs)+len(pseudoPeople) >= treeMaxNodes {
						break
					}
					downVisited[link.Child] = true
					downQueue = append(downQueue, struct {
						ID    int
						Depth int
					}{ID: link.Child, Depth: item.Depth + 1})
				}
			}
			if len(peopleIDs)+len(pseudoPeople) >= treeMaxNodes {
				break
			}
		}
		if len(peopleIDs)+len(pseudoPeople) >= treeMaxNodes {
			break
		}
	}

	ids := make([]int, 0, len(peopleIDs))
	for id := range peopleIDs {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	personsByID, err := a.DB.GetPersonesByIDs(ids)
	if err != nil {
		return dataset, err
	}

	valid := map[int]bool{}
	people := make([]treePerson, 0, len(ids))
	for _, id := range ids {
		p := personsByID[id]
		if p == nil {
			continue
		}

		birth := ""
		if p.DataNaixement.Valid {
			birth = formatDateDisplay(p.DataNaixement.String)
		} else if p.DataBateig.Valid {
			birth = formatDateDisplay(p.DataBateig.String)
		}
		death := ""
		if p.DataDefuncio.Valid {
			death = formatDateDisplay(p.DataDefuncio.String)
		}

		person := treePerson{
			ID:         p.ID,
			Name:       personaDisplayName(p),
			Sex:        2,
			Birth:      birth,
			BirthPlace: strings.TrimSpace(p.MunicipiNaixement),
			Death:      death,
			DeathPlace: strings.TrimSpace(p.MunicipiDefuncio),
			Occupation: strings.TrimSpace(p.Ofici),
		}
		if strings.TrimSpace(person.BirthPlace) == "" {
			person.BirthPlace = strings.TrimSpace(p.Municipi)
		}
		person = a.fillTreePersonFromRegistres(p.ID, person)
		people = append(people, person)
		valid[p.ID] = true
	}
	for _, pseudo := range pseudoPeople {
		if _, exists := valid[pseudo.ID]; exists {
			continue
		}
		people = append(people, pseudo)
		valid[pseudo.ID] = true
	}

	bestByChild := map[int]treeLink{}
	for _, l := range linksMap {
		if !valid[l.Child] {
			continue
		}
		father := l.Father
		mother := l.Mother
		if father > 0 && !valid[father] {
			father = 0
		}
		if mother > 0 && !valid[mother] {
			mother = 0
		}
		if father == 0 && mother == 0 {
			continue
		}
		link := treeLink{Child: l.Child, Father: father, Mother: mother}
		best, ok := bestByChild[link.Child]
		if !ok {
			bestByChild[link.Child] = link
			continue
		}
		score := func(t treeLink) int {
			points := 0
			if pair, ok := primaryParents[t.Child]; ok {
				if pair.Father == t.Father && pair.Mother == t.Mother {
					points += 1000
				}
			}
			if depth, ok := ancestorDepth[t.Father]; ok {
				points += 20 - depth
			}
			if depth, ok := ancestorDepth[t.Mother]; ok {
				points += 20 - depth
			}
			if t.Father != 0 {
				points++
			}
			if t.Mother != 0 {
				points++
			}
			return points
		}
		bestScore := score(best)
		linkScore := score(link)
		if linkScore > bestScore {
			bestByChild[link.Child] = link
			continue
		}
		if linkScore == bestScore {
			if link.Father < best.Father || (link.Father == best.Father && link.Mother < best.Mother) {
				bestByChild[link.Child] = link
			}
		}
	}

	filteredLinks := make([]treeLink, 0, len(bestByChild))
	for _, link := range bestByChild {
		filteredLinks = append(filteredLinks, link)
	}

	sort.Slice(filteredLinks, func(i, j int) bool {
		if filteredLinks[i].Child != filteredLinks[j].Child {
			return filteredLinks[i].Child < filteredLinks[j].Child
		}
		if filteredLinks[i].Father != filteredLinks[j].Father {
			return filteredLinks[i].Father < filteredLinks[j].Father
		}
		return filteredLinks[i].Mother < filteredLinks[j].Mother
	})

	dataset = treeDataset{
		FamilyData:   people,
		FamilyLinks:  filteredLinks,
		RootPersonID: root.ID,
		DatasetStats: treeDatasetStats{People: len(people), Links: len(filteredLinks)},
	}

	return dataset, nil
}

func (a *App) PersonaArbreAPI(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePersonesView(w, r); !ok {
		return
	}
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	if !strings.HasSuffix(r.URL.Path, "/arbre") {
		http.NotFound(w, r)
		return
	}
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	root, err := a.DB.GetPersona(id)
	status := ""
	if root != nil {
		status = strings.TrimSpace(root.ModeracioEstat)
	}
	if err != nil || root == nil || (status != "" && status != "publicat") {
		http.NotFound(w, r)
		return
	}

	gens := parseTreeGens(r.URL.Query().Get("gens"), treeDefaultGens)
	view := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("view")))
	if view != "familiar" && view != "ventall" {
		view = "pedigree"
	}
	var dataset treeDataset
	if view == "familiar" {
		dataset, err = a.buildFamiliarArbreDataset(root, gens)
	} else {
		dataset, err = a.buildPersonaArbreDataset(root, gens)
	}
	if err != nil {
		http.Error(w, "Error carregant arbre", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(dataset)
}

type treeExpandResponse struct {
	People []treePerson     `json:"people"`
	Links  []treeLink       `json:"links"`
	Stats  treeDatasetStats `json:"stats"`
}

func (a *App) ArbreExpandAPI(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePersonesView(w, r); !ok {
		return
	}
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	mode := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("mode")))
	if mode == "" {
		mode = "ancestors"
	}
	if mode != "ancestors" && mode != "children" {
		http.Error(w, "Mode no suportat", http.StatusBadRequest)
		return
	}
	personID, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("person_id")))
	if err != nil || personID <= 0 {
		http.Error(w, "Person ID invalid", http.StatusBadRequest)
		return
	}
	root, err := a.DB.GetPersona(personID)
	status := ""
	if root != nil {
		status = strings.TrimSpace(root.ModeracioEstat)
	}
	if err != nil || root == nil || (status != "" && status != "publicat") {
		http.NotFound(w, r)
		return
	}
	var resp treeExpandResponse
	if mode == "children" {
		pseudoPeople := map[int]treePerson{}
		links, err := a.loadChildrenForPersona(personID, pseudoPeople)
		if err != nil {
			http.Error(w, "Error carregant arbre", http.StatusInternalServerError)
			return
		}
		peopleIDs := map[int]bool{personID: true}
		for _, link := range links {
			if link.Child > 0 {
				peopleIDs[link.Child] = true
			}
			if link.Father > 0 {
				peopleIDs[link.Father] = true
			}
			if link.Mother > 0 {
				peopleIDs[link.Mother] = true
			}
		}
		ids := make([]int, 0, len(peopleIDs))
		for id := range peopleIDs {
			ids = append(ids, id)
		}
		sort.Ints(ids)

		personsByID, err := a.DB.GetPersonesByIDs(ids)
		if err != nil {
			http.Error(w, "Error carregant arbre", http.StatusInternalServerError)
			return
		}

		valid := map[int]bool{}
		people := make([]treePerson, 0, len(ids))
		for _, id := range ids {
			p := personsByID[id]
			if p == nil {
				continue
			}
			birth := ""
			if p.DataNaixement.Valid {
				birth = formatDateDisplay(p.DataNaixement.String)
			} else if p.DataBateig.Valid {
				birth = formatDateDisplay(p.DataBateig.String)
			}
			death := ""
			if p.DataDefuncio.Valid {
				death = formatDateDisplay(p.DataDefuncio.String)
			}
			person := treePerson{
				ID:         p.ID,
				Name:       personaDisplayName(p),
				Sex:        2,
				Birth:      birth,
				BirthPlace: strings.TrimSpace(p.MunicipiNaixement),
				Death:      death,
				DeathPlace: strings.TrimSpace(p.MunicipiDefuncio),
				Occupation: strings.TrimSpace(p.Ofici),
			}
			if strings.TrimSpace(person.BirthPlace) == "" {
				person.BirthPlace = strings.TrimSpace(p.Municipi)
			}
			person = a.fillTreePersonFromRegistres(p.ID, person)
			people = append(people, person)
			valid[p.ID] = true
		}
		for _, pseudo := range pseudoPeople {
			if _, exists := valid[pseudo.ID]; exists {
				continue
			}
			people = append(people, pseudo)
			valid[pseudo.ID] = true
		}

		bestByChild := map[int]treeLink{}
		for _, l := range links {
			if !valid[l.Child] {
				continue
			}
			father := l.Father
			mother := l.Mother
			if father > 0 && !valid[father] {
				father = 0
			}
			if mother > 0 && !valid[mother] {
				mother = 0
			}
			if father == 0 && mother == 0 {
				continue
			}
			link := treeLink{Child: l.Child, Father: father, Mother: mother}
			best, ok := bestByChild[link.Child]
			if !ok {
				bestByChild[link.Child] = link
				continue
			}
			bestScore := 0
			if best.Father != 0 {
				bestScore++
			}
			if best.Mother != 0 {
				bestScore++
			}
			linkScore := 0
			if link.Father != 0 {
				linkScore++
			}
			if link.Mother != 0 {
				linkScore++
			}
			if linkScore > bestScore {
				bestByChild[link.Child] = link
				continue
			}
			if linkScore == bestScore {
				if link.Father < best.Father || (link.Father == best.Father && link.Mother < best.Mother) {
					bestByChild[link.Child] = link
				}
			}
		}

		filteredLinks := make([]treeLink, 0, len(bestByChild))
		for _, link := range bestByChild {
			filteredLinks = append(filteredLinks, link)
		}

		resp = treeExpandResponse{
			People: people,
			Links:  filteredLinks,
			Stats:  treeDatasetStats{People: len(people), Links: len(filteredLinks)},
		}
	} else {
		gens := parseTreeGens(r.URL.Query().Get("gens"), treeDefaultGens)
		dataset, err := a.buildPersonaArbreDataset(root, gens)
		if err != nil {
			http.Error(w, "Error carregant arbre", http.StatusInternalServerError)
			return
		}
		resp = treeExpandResponse{
			People: dataset.FamilyData,
			Links:  dataset.FamilyLinks,
			Stats:  dataset.DatasetStats,
		}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(resp)
}
