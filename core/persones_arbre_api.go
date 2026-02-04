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
	transID := 0
	if bestID, ok, err := a.DB.FindBestBaptismeTranscripcioForPersona(personaID); err != nil {
		return pair, err
	} else if ok && bestID > 0 {
		transID = bestID
	} else if registres, err := a.DB.ListRegistresByPersona(personaID, ""); err == nil {
		for _, row := range registres {
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
	if transID <= 0 {
		if cache != nil {
			cache[personaID] = pair
		}
		return pair, nil
	}
	persones, err := a.DB.ListTranscripcioPersones(transID)
	if err != nil {
		return pair, err
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
	pair = parentPair{Father: fatherID, Mother: motherID}
	if cache != nil {
		cache[personaID] = pair
	}
	return pair, nil
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

func (a *App) PersonaArbreAPI(w http.ResponseWriter, r *http.Request) {
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
	if err != nil || root == nil || root.ModeracioEstat != "publicat" {
		http.NotFound(w, r)
		return
	}

	gens := parseTreeGens(r.URL.Query().Get("gens"), treeDefaultGens)
	dataset, err := a.buildPersonaArbreDataset(root, gens)
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
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	mode := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("mode")))
	if mode == "" {
		mode = "ancestors"
	}
	if mode != "ancestors" {
		http.Error(w, "Mode no suportat", http.StatusBadRequest)
		return
	}
	personID, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("person_id")))
	if err != nil || personID <= 0 {
		http.Error(w, "Person ID invalid", http.StatusBadRequest)
		return
	}
	root, err := a.DB.GetPersona(personID)
	if err != nil || root == nil || root.ModeracioEstat != "publicat" {
		http.NotFound(w, r)
		return
	}
	gens := parseTreeGens(r.URL.Query().Get("gens"), treeDefaultGens)
	dataset, err := a.buildPersonaArbreDataset(root, gens)
	if err != nil {
		http.Error(w, "Error carregant arbre", http.StatusInternalServerError)
		return
	}
	resp := treeExpandResponse{
		People: dataset.FamilyData,
		Links:  dataset.FamilyLinks,
		Stats:  dataset.DatasetStats,
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(resp)
}
