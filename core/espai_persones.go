package core

import (
	"database/sql"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type espaiDocView struct {
	ID     int
	Tipus  string
	Any    string
	Llibre string
	Pagina string
	Estat  string
}

type espaiTimelineEvent struct {
	Type        string
	FilterType  string
	Label       string
	Icon        string
	Date        string
	Title       string
	Source      string
	RegistreID  int
	RegistreAny string
}

type espaiRelationView struct {
	Role            string
	RoleLabel       string
	Name            string
	Municipi        string
	Ofici           string
	RegistreID      int
	RegistreTipus   string
	RegistreAny     string
	Llibre          string
	Linked          bool
	LinkedPersonaID int
}

type espaiAnecdoteView struct {
	ID       int
	Title    string
	Body     string
	Tag      string
	User     string
	Date     string
	Status   string
	Featured bool
}

func espaiRelationLabel(lang string, relType string) string {
	labelFor := func(key, fallback string) string {
		val := T(lang, key)
		if val == "" || val == key {
			return fallback
		}
		return val
	}
	relType = strings.ToLower(strings.TrimSpace(relType))
	switch relType {
	case "father":
		return labelFor("records.detail.father", "Pare")
	case "mother":
		return labelFor("records.detail.mother", "Mare")
	case "spouse":
		return labelFor("records.detail.spouse", "Parella")
	case "parent":
		return labelFor("space.relation.parent", "Parent")
	case "child":
		return labelFor("space.relation.child", "Fill/a")
	case "sibling":
		return labelFor("space.relation.sibling", "Germà/na")
	case "grandparent":
		return labelFor("space.relation.grandparent", "Avi/Àvia")
	default:
		return "Relació"
	}
}

func (a *App) EspaiPersonaHandler(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/arbre") {
		a.EspaiPersonaArbre(w, r)
		return
	}
	a.EspaiPersonaDetall(w, r)
}

func (a *App) EspaiPersonaDetall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	p, err := a.DB.GetEspaiPersona(id)
	if err != nil || p == nil || p.OwnerUserID != user.ID {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLang(r)

	persona := db.Persona{
		ID:                p.ID,
		Nom:               strings.TrimSpace(p.Nom.String),
		Cognom1:           strings.TrimSpace(p.Cognom1.String),
		Cognom2:           strings.TrimSpace(p.Cognom2.String),
		NomComplet:        strings.TrimSpace(p.NomComplet.String),
		MunicipiNaixement: strings.TrimSpace(p.LlocNaixement.String),
		MunicipiDefuncio:  strings.TrimSpace(p.LlocDefuncio.String),
		DataNaixement:     sql.NullString{String: strings.TrimSpace(p.DataNaixement.String), Valid: p.DataNaixement.Valid},
		DataDefuncio:      sql.NullString{String: strings.TrimSpace(p.DataDefuncio.String), Valid: p.DataDefuncio.Valid},
		ModeracioEstat:    "pendent",
		CreatedAt:         p.CreatedAt,
		UpdatedAt:         p.UpdatedAt,
	}

	fullName := strings.TrimSpace(strings.Join([]string{persona.Nom, persona.Cognom1, persona.Cognom2}, " "))
	if fullName == "" {
		fullName = persona.NomComplet
	}
	if fullName == "" {
		fullName = "?"
	}
	initials := ""
	if persona.Nom != "" {
		r := []rune(strings.TrimSpace(persona.Nom))
		if len(r) > 0 {
			initials += strings.ToUpper(string(r[0]))
		}
	}
	if persona.Cognom1 != "" {
		r := []rune(strings.TrimSpace(persona.Cognom1))
		if len(r) > 0 {
			initials += strings.ToUpper(string(r[0]))
		}
	}
	if initials == "" {
		initials = "?"
	}

	birthDate := ""
	if persona.DataNaixement.Valid {
		birthDate = formatDateDisplay(persona.DataNaixement.String)
	}
	deathDate := ""
	if persona.DataDefuncio.Valid {
		deathDate = formatDateDisplay(persona.DataDefuncio.String)
	}
	birthLocation := strings.TrimSpace(persona.MunicipiNaixement)
	deathLocation := strings.TrimSpace(persona.MunicipiDefuncio)
	lifeRange := ""
	if birthDate != "" {
		lifeRange = birthDate
	}
	if deathDate != "" {
		if lifeRange != "" {
			lifeRange += " – " + deathDate
		} else {
			lifeRange = deathDate
		}
	}
	birthLabel := birthDate
	if birthLocation != "" {
		if birthLabel != "" {
			birthLabel += " · " + birthLocation
		} else {
			birthLabel = birthLocation
		}
	}
	deathLabel := deathDate
	if deathLocation != "" {
		if deathLabel != "" {
			deathLabel += " · " + deathLocation
		} else {
			deathLabel = deathLocation
		}
	}

	lastUpdated := ""
	if persona.UpdatedAt.Valid {
		lastUpdated = persona.UpdatedAt.Time.Format("2006-01-02")
	} else if persona.CreatedAt.Valid {
		lastUpdated = persona.CreatedAt.Time.Format("2006-01-02")
	}

	totalFields := 0
	filledFields := 0
	addField := func(val string) {
		totalFields++
		if strings.TrimSpace(val) != "" {
			filledFields++
		}
	}
	addField(persona.Nom)
	addField(persona.Cognom1)
	addField(persona.Cognom2)
	addField(birthLocation)
	addField(deathLocation)
	totalFields += 2
	if birthDate != "" {
		filledFields++
	}
	if deathDate != "" {
		filledFields++
	}
	completesa := 0
	if totalFields > 0 {
		completesa = int(float64(filledFields) / float64(totalFields) * 100)
		if completesa > 100 {
			completesa = 100
		}
	}

	fieldNeedsLink := map[string]bool{
		"data_naixement":     false,
		"data_bateig":        false,
		"data_defuncio":      false,
		"municipi_naixement": false,
		"municipi_defuncio":  false,
	}

	relacions := []espaiRelationView{}
	if rels, err := a.DB.ListEspaiRelacionsByArbre(p.ArbreID); err == nil {
		type parentLink struct {
			ParentID int
			RelType  string
		}
		parentLinks := map[int][]parentLink{}
		childrenByParent := map[int]map[int]struct{}{}
		spousesByPerson := map[int]map[int]struct{}{}
		siblingsByPerson := map[int]map[int]struct{}{}
		addChild := func(parentID, childID int) {
			if parentID == 0 || childID == 0 {
				return
			}
			set := childrenByParent[parentID]
			if set == nil {
				set = map[int]struct{}{}
				childrenByParent[parentID] = set
			}
			set[childID] = struct{}{}
		}
		addParent := func(childID, parentID int, relType string) {
			if childID == 0 || parentID == 0 {
				return
			}
			relType = strings.ToLower(strings.TrimSpace(relType))
			parentLinks[childID] = append(parentLinks[childID], parentLink{ParentID: parentID, RelType: relType})
		}
		addPair := func(store map[int]map[int]struct{}, aID, bID int) {
			if aID == 0 || bID == 0 {
				return
			}
			set := store[aID]
			if set == nil {
				set = map[int]struct{}{}
				store[aID] = set
			}
			set[bID] = struct{}{}
		}

		for _, rel := range rels {
			relType := strings.ToLower(strings.TrimSpace(rel.RelationType))
			switch relType {
			case "father", "mother", "parent":
				addParent(rel.PersonaID, rel.RelatedPersonaID, relType)
				addChild(rel.RelatedPersonaID, rel.PersonaID)
			case "child":
				addParent(rel.RelatedPersonaID, rel.PersonaID, "parent")
				addChild(rel.PersonaID, rel.RelatedPersonaID)
			case "spouse":
				addPair(spousesByPerson, rel.PersonaID, rel.RelatedPersonaID)
				addPair(spousesByPerson, rel.RelatedPersonaID, rel.PersonaID)
			case "sibling":
				addPair(siblingsByPerson, rel.PersonaID, rel.RelatedPersonaID)
				addPair(siblingsByPerson, rel.RelatedPersonaID, rel.PersonaID)
			}
		}

		personCache := map[int]*db.EspaiPersona{}
		getPerson := func(id int) *db.EspaiPersona {
			if v, ok := personCache[id]; ok {
				return v
			}
			person, err := a.DB.GetEspaiPersona(id)
			if err != nil || person == nil {
				personCache[id] = nil
				return nil
			}
			personCache[id] = person
			return person
		}
		seen := map[string]bool{}
		addRelation := func(relatedID int, relType string) {
			if relatedID == 0 {
				return
			}
			key := relType + ":" + strconv.Itoa(relatedID)
			if seen[key] {
				return
			}
			seen[key] = true
			other := getPerson(relatedID)
			if other == nil {
				return
			}
			name := espaiPersonaDisplayNameWithFallback(*other, T(lang, "tree.unknown.name"))
			roleLabel := espaiRelationLabel(lang, relType)
			relacions = append(relacions, espaiRelationView{
				Role:            relType,
				RoleLabel:       roleLabel,
				Name:            name,
				Municipi:        strings.TrimSpace(other.LlocNaixement.String),
				Linked:          false,
				LinkedPersonaID: relatedID,
			})
		}

		parents := parentLinks[p.ID]
		sort.SliceStable(parents, func(i, j int) bool {
			rank := func(relType string) int {
				switch relType {
				case "father":
					return 0
				case "mother":
					return 1
				case "parent":
					return 2
				default:
					return 3
				}
			}
			return rank(parents[i].RelType) < rank(parents[j].RelType)
		})
		for _, pl := range parents {
			addRelation(pl.ParentID, pl.RelType)
		}

		if spouses := spousesByPerson[p.ID]; spouses != nil {
			for spouseID := range spouses {
				addRelation(spouseID, "spouse")
			}
		}

		if children := childrenByParent[p.ID]; children != nil {
			for childID := range children {
				if childID == p.ID {
					continue
				}
				addRelation(childID, "child")
			}
		}

		siblingSet := map[int]struct{}{}
		if siblings := siblingsByPerson[p.ID]; siblings != nil {
			for siblingID := range siblings {
				if siblingID == p.ID {
					continue
				}
				siblingSet[siblingID] = struct{}{}
			}
		}
		for _, pl := range parents {
			if siblings := childrenByParent[pl.ParentID]; siblings != nil {
				for siblingID := range siblings {
					if siblingID == p.ID {
						continue
					}
					siblingSet[siblingID] = struct{}{}
				}
			}
		}
		for siblingID := range siblingSet {
			addRelation(siblingID, "sibling")
		}

		grandSet := map[int]struct{}{}
		for _, pl := range parents {
			for _, gp := range parentLinks[pl.ParentID] {
				if gp.ParentID == 0 || gp.ParentID == p.ID {
					continue
				}
				grandSet[gp.ParentID] = struct{}{}
			}
		}
		for gpID := range grandSet {
			addRelation(gpID, "grandparent")
		}
	}

	RenderPrivateTemplate(w, r, "persona-detall.html", map[string]interface{}{
		"Persona":              persona,
		"NomComplet":           fullName,
		"Initials":             initials,
		"BirthDate":            birthDate,
		"BaptismDate":          "",
		"DeathDate":            deathDate,
		"LifeRange":            lifeRange,
		"BirthLabel":           birthLabel,
		"DeathLabel":           deathLabel,
		"BirthLocation":        birthLocation,
		"DeathLocation":        deathLocation,
		"LastUpdated":          lastUpdated,
		"Completesa":           completesa,
		"QualitatFonts":        0,
		"FieldNeedsLink":       fieldNeedsLink,
		"CanEditPersona":       false,
		"CanLinkPersonaFields": false,
		"DocRegistres":         []espaiDocView{},
		"DocTotal":             0,
		"OriginMunicipi":       "",
		"OriginDefuncio":       "",
		"OriginLlibre":         "",
		"OriginPagina":         "",
		"OriginRegistreID":     0,
		"OriginAny":            "",
		"Relacions":            relacions,
		"TimelineEvents":       []espaiTimelineEvent{},
		"Anecdotes":            []espaiAnecdoteView{},
		"TipusOptions":         transcripcioTipusActe,
		"User":                 user,
		"MarkType":             "",
		"MarkPublic":           false,
		"MarkOwn":              false,
		"WikiPending":          false,
		"Tab":                  "detall",
		"PersonaBasePath":      "/espai/persones",
		"IsEspaiProfile":       true,
	})
}

func (a *App) EspaiPersonaArbre(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	if user == nil {
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
	lang := ResolveLang(r)
	p, err := a.DB.GetEspaiPersona(id)
	if err != nil || p == nil || p.OwnerUserID != user.ID {
		http.NotFound(w, r)
		return
	}

	view := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("view")))
	if view != "familiar" && view != "ventall" {
		view = "pedigree"
	}
	gens := parseTreeGens(r.URL.Query().Get("gens"), treeDefaultGens)

	fullName := espaiPersonaDisplayNameWithFallback(*p, T(lang, "tree.unknown.name"))
	if fullName == "" {
		fullName = "?"
	}

	dataset, err := a.buildEspaiArbreDataset(p.ArbreID, p.ID, lang, false)
	if err != nil || dataset.RootPersonID == 0 {
		http.Error(w, "Error carregant arbre", http.StatusInternalServerError)
		return
	}

	treeI18n := treeI18nMap(lang)
	RenderPrivateTemplate(w, r, "persona-arbre.html", map[string]interface{}{
		"Persona":        db.Persona{ID: p.ID},
		"PersonaName":    fullName,
		"View":           view,
		"Gens":           gens,
		"FamilyData":     dataset.FamilyData,
		"FamilyLinks":    dataset.FamilyLinks,
		"RootPersonId":   dataset.RootPersonID,
		"DatasetStats":   dataset.DatasetStats,
		"TreeI18n":       treeI18n,
		"TreeProfileBase": "/espai/persones",
		"TreeExpandDisabled": true,
	})
}
