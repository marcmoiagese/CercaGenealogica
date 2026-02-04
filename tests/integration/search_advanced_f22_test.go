package integration

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestAdvancedSearchFiltersAndFuzzy(t *testing.T) {
	apps := newAppsForAllDBs(t)
	for _, appDB := range apps {
		appDB := appDB
		t.Run(appDB.Label, func(t *testing.T) {
			app := appDB.App
			database := appDB.DB

			user, _ := createF7UserWithSession(t, database)

			paisID, err := database.CreatePais(&db.Pais{CodiISO2: "TS", CodiISO3: "TST"})
			if err != nil {
				t.Fatalf("CreatePais ha fallat: %v", err)
			}
			nivellID, err := database.CreateNivell(&db.NivellAdministratiu{
				PaisID:          paisID,
				Nivel:           1,
				NomNivell:       "Nivell Test",
				TipusNivell:     "Regio",
				Estat:           "actiu",
				ModeracioEstat:  "publicat",
				CreatedBy:       sql.NullInt64{Int64: int64(user.ID), Valid: true},
			})
			if err != nil {
				t.Fatalf("CreateNivell ha fallat: %v", err)
			}

			mun1 := &db.Municipi{
				Nom:            "Municipi One",
				Tipus:          "municipi",
				Estat:          "actiu",
				ModeracioEstat: "publicat",
				CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
			}
			mun1.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellID), Valid: true}
			mun1ID, err := database.CreateMunicipi(mun1)
			if err != nil {
				t.Fatalf("CreateMunicipi mun1 ha fallat: %v", err)
			}
			mun2 := &db.Municipi{
				Nom:            "Municipi Two",
				Tipus:          "municipi",
				Estat:          "actiu",
				ModeracioEstat: "publicat",
				CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
			}
			mun2.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellID), Valid: true}
			mun2ID, err := database.CreateMunicipi(mun2)
			if err != nil {
				t.Fatalf("CreateMunicipi mun2 ha fallat: %v", err)
			}

			closureFor := func(munID int) []db.AdminClosureEntry {
				return []db.AdminClosureEntry{
					{DescendantMunicipiID: munID, AncestorType: "municipi", AncestorID: munID},
					{DescendantMunicipiID: munID, AncestorType: "nivell", AncestorID: nivellID},
					{DescendantMunicipiID: munID, AncestorType: "pais", AncestorID: paisID},
				}
			}
			if err := database.ReplaceAdminClosure(mun1ID, closureFor(mun1ID)); err != nil {
				t.Fatalf("ReplaceAdminClosure mun1 ha fallat: %v", err)
			}
			if err := database.ReplaceAdminClosure(mun2ID, closureFor(mun2ID)); err != nil {
				t.Fatalf("ReplaceAdminClosure mun2 ha fallat: %v", err)
			}

			eclesID, err := database.CreateArquebisbat(&db.Arquebisbat{
				Nom:            "Bisbat Test",
				TipusEntitat:   "bisbat",
				ModeracioEstat: "publicat",
				CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
			})
			if err != nil {
				t.Fatalf("CreateArquebisbat ha fallat: %v", err)
			}
			arxiuID, err := database.CreateArxiu(&db.Arxiu{
				Nom:            "Arxiu Test",
				Tipus:          "diocesa",
				ModeracioEstat: "publicat",
				CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
			})
			if err != nil {
				t.Fatalf("CreateArxiu ha fallat: %v", err)
			}
			llibreID, err := database.CreateLlibre(&db.Llibre{
				ArquebisbatID:  eclesID,
				MunicipiID:     mun1ID,
				Titol:          "Llibre Test",
				ModeracioEstat: "publicat",
				CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
			})
			if err != nil {
				t.Fatalf("CreateLlibre ha fallat: %v", err)
			}

			canonID, err := database.UpsertCognom("Moia", core.NormalizeCognomKey("Moia"), "test", "", nil)
			if err != nil {
				t.Fatalf("UpsertCognom ha fallat: %v", err)
			}
			_, err = database.CreateCognomVariant(&db.CognomVariant{
				CognomID:       canonID,
				Variant:        "Moya",
				Key:            core.NormalizeCognomKey("Moya"),
				ModeracioEstat: "publicat",
				CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
			})
			if err != nil {
				t.Fatalf("CreateCognomVariant ha fallat: %v", err)
			}

			personExact := &db.Persona{
				Nom:            "Joan",
				Cognom1:        "Moia",
				Municipi:       mun1.Nom,
				ModeracioEstat: "publicat",
				CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
				UpdatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
			}
			personExactID, err := database.CreatePersona(personExact)
			if err != nil {
				t.Fatalf("CreatePersona exact ha fallat: %v", err)
			}
			personPhonetic := &db.Persona{
				Nom:            "Juan",
				Cognom1:        "Moya",
				Municipi:       mun1.Nom,
				ModeracioEstat: "publicat",
				CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
				UpdatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
			}
			personPhoneticID, err := database.CreatePersona(personPhonetic)
			if err != nil {
				t.Fatalf("CreatePersona phonetic ha fallat: %v", err)
			}
			personFilter1 := &db.Persona{
				Nom:            "Pere",
				Cognom1:        "Soler",
				Municipi:       mun1.Nom,
				ModeracioEstat: "publicat",
				CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
				UpdatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
			}
			personFilter1ID, err := database.CreatePersona(personFilter1)
			if err != nil {
				t.Fatalf("CreatePersona filter1 ha fallat: %v", err)
			}
			personFilter2 := &db.Persona{
				Nom:            "Pere",
				Cognom1:        "Soler",
				Municipi:       mun2.Nom,
				ModeracioEstat: "publicat",
				CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
				UpdatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
			}
			personFilter2ID, err := database.CreatePersona(personFilter2)
			if err != nil {
				t.Fatalf("CreatePersona filter2 ha fallat: %v", err)
			}

			registre := &db.TranscripcioRaw{
				LlibreID:       llibreID,
				TipusActe:      "baptisme",
				DataActeEstat:  "clar",
				DataActeISO:    sql.NullString{String: "1900-01-01", Valid: true},
				AnyDoc:         sql.NullInt64{Int64: 1900, Valid: true},
				ModeracioEstat: "publicat",
				CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
			}
			registreID, err := database.CreateTranscripcioRaw(registre)
			if err != nil {
				t.Fatalf("CreateTranscripcioRaw ha fallat: %v", err)
			}
			_, _ = database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
				TranscripcioID: registreID,
				Rol:            "batejat",
				Nom:            "Joan",
				Cognom1:        "Moia",
			})

			upsertPersonaDoc(t, database, personExactID, mun1ID, "joan moia", "joan", "moia", "joan moia")
			upsertPersonaDoc(t, database, personPhoneticID, mun1ID, "", "", "", "joan moia")
			upsertPersonaDoc(t, database, personFilter1ID, mun1ID, "pere soler", "pere", "soler", "pere soler")
			upsertPersonaDoc(t, database, personFilter2ID, mun2ID, "pere soler", "pere", "soler", "pere soler")
			upsertRegistreDoc(t, database, registreID, mun1ID, llibreID, arxiuID, eclesID, "joan moia")

			resp := callSearchAPI(t, app, "/api/search?q=pere+soler&entity=persona&ancestor_type=municipi&ancestor_id="+strconv.Itoa(mun1ID))
			items := readItems(resp)
			if len(items) != 1 {
				t.Fatalf("esperava 1 resultat filtrat per municipi, got %d", len(items))
			}
			if id := itemID(items[0]); id != personFilter1ID {
				t.Fatalf("resultat filtrat incorrecte, got %d", id)
			}

			resp = callSearchAPI(t, app, "/api/search?q=joan&entity=registre_raw&entitat_eclesiastica_id="+strconv.Itoa(eclesID))
			items = readItems(resp)
			if len(items) != 1 {
				t.Fatalf("esperava 1 registre per entitat, got %d", len(items))
			}

			resp = callSearchAPI(t, app, "/api/search?q=Moya&entity=persona")
			items = readItems(resp)
			if !containsID(items, personExactID) {
				t.Fatalf("variant cognom no retorna persona canònica")
			}

			resp = callSearchAPI(t, app, "/api/search?q=joan+moia&entity=persona")
			items = readItems(resp)
			if len(items) < 2 {
				t.Fatalf("esperava mínim 2 resultats (exacte + fonètic)")
			}
			if first := itemID(items[0]); first != personExactID {
				t.Fatalf("esperava exacte primer, got %d", first)
			}

			resp = callSearchAPI(t, app, "/api/search?q=pere+soler&entity=persona&page_size=1")
			items = readItems(resp)
			if len(items) != 1 {
				t.Fatalf("esperava 1 resultat a la primera pàgina, got %d", len(items))
			}
			if pages := int(resp["total_pages"].(float64)); pages != 2 {
				t.Fatalf("esperava 2 pàgines, got %d", pages)
			}

			resp = callSearchAPI(t, app, "/api/search?q=joan&from=not-a-date")
			if total := int(resp["total"].(float64)); total == 0 {
				t.Fatalf("esperava resultats amb data invàlida")
			}
		})
	}
}

func upsertPersonaDoc(t *testing.T, database db.DB, personaID, munID int, fullNorm, nomNorm, cognomsNorm, tokens string) {
	t.Helper()
	toks := strings.TrimSpace(tokens)
	phonetic := ""
	if toks != "" {
		phonetic = strings.Join(phoneticTokensFrom(toks), " ")
	}
	doc := &db.SearchDoc{
		EntityType:        "persona",
		EntityID:          personaID,
		Published:         true,
		MunicipiID:        sql.NullInt64{Int64: int64(munID), Valid: munID > 0},
		PersonNomNorm:     nomNorm,
		PersonCognomsNorm: cognomsNorm,
		PersonFullNorm:    fullNorm,
		PersonTokensNorm:  toks,
		CognomsTokensNorm: cognomsNorm,
		PersonPhonetic:    phonetic,
		CognomsPhonetic:   phonetic,
		CognomsCanon:      cognomsNorm,
	}
	if err := database.UpsertSearchDoc(doc); err != nil {
		t.Fatalf("UpsertSearchDoc persona ha fallat: %v", err)
	}
}

func upsertRegistreDoc(t *testing.T, database db.DB, registreID, munID, llibreID, arxiuID, eclesID int, fullNorm string) {
	t.Helper()
	toks := strings.TrimSpace(fullNorm)
	phonetic := ""
	if toks != "" {
		phonetic = strings.Join(phoneticTokensFrom(toks), " ")
	}
	doc := &db.SearchDoc{
		EntityType:            "registre_raw",
		EntityID:              registreID,
		Published:             true,
		MunicipiID:            sql.NullInt64{Int64: int64(munID), Valid: munID > 0},
		ArxiuID:               sql.NullInt64{Int64: int64(arxiuID), Valid: arxiuID > 0},
		LlibreID:              sql.NullInt64{Int64: int64(llibreID), Valid: llibreID > 0},
		EntitatEclesiasticaID: sql.NullInt64{Int64: int64(eclesID), Valid: eclesID > 0},
		PersonFullNorm:        fullNorm,
		PersonTokensNorm:      toks,
		CognomsTokensNorm:     fullNorm,
		PersonPhonetic:        phonetic,
		CognomsPhonetic:       phonetic,
		CognomsCanon:          fullNorm,
	}
	if err := database.UpsertSearchDoc(doc); err != nil {
		t.Fatalf("UpsertSearchDoc registre ha fallat: %v", err)
	}
}

func callSearchAPI(t *testing.T, app *core.App, path string) map[string]interface{} {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rr := httptest.NewRecorder()
	app.SearchAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("SearchAPI retorn %d", rr.Code)
	}
	var payload map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&payload); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	return payload
}

func readItems(payload map[string]interface{}) []map[string]interface{} {
	rawItems, ok := payload["items"].([]interface{})
	if !ok {
		return nil
	}
	items := make([]map[string]interface{}, 0, len(rawItems))
	for _, raw := range rawItems {
		if row, ok := raw.(map[string]interface{}); ok {
			items = append(items, row)
		}
	}
	return items
}

func itemID(item map[string]interface{}) int {
	if item == nil {
		return 0
	}
	if raw, ok := item["entity_id"].(float64); ok {
		return int(raw)
	}
	return 0
}

func containsID(items []map[string]interface{}, id int) bool {
	for _, item := range items {
		if itemID(item) == id {
			return true
		}
	}
	return false
}

func phoneticTokensFrom(text string) []string {
	parts := strings.Fields(strings.ToLower(text))
	seen := map[string]struct{}{}
	out := []string{}
	for _, part := range parts {
		code := soundexTest(part)
		if code == "" {
			continue
		}
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		out = append(out, code)
	}
	return out
}

func soundexTest(token string) string {
	token = strings.ToUpper(stripNonLettersTest(token))
	if token == "" {
		return ""
	}
	first := token[:1]
	var lastDigit string
	var b strings.Builder
	b.WriteString(first)
	for i := 1; i < len(token); i++ {
		d := soundexDigitTest(token[i])
		if d == "" {
			lastDigit = ""
			continue
		}
		if d == lastDigit {
			continue
		}
		b.WriteString(d)
		lastDigit = d
		if b.Len() >= 4 {
			break
		}
	}
	for b.Len() < 4 {
		b.WriteByte('0')
	}
	return b.String()
}

func soundexDigitTest(ch byte) string {
	switch ch {
	case 'B', 'F', 'P', 'V':
		return "1"
	case 'C', 'G', 'J', 'K', 'Q', 'S', 'X', 'Z':
		return "2"
	case 'D', 'T':
		return "3"
	case 'L':
		return "4"
	case 'M', 'N':
		return "5"
	case 'R':
		return "6"
	default:
		return ""
	}
}

func stripNonLettersTest(val string) string {
	var b strings.Builder
	for _, r := range val {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
			b.WriteRune(r)
		}
	}
	return b.String()
}
