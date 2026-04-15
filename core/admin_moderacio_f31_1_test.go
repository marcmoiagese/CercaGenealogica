package core

import (
	"database/sql"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestApplyModeracioBulkRegistreDerivedSideEffectsDeletesSearchDocsF311(t *testing.T) {
	app, database := newModeracioBulkDiagnosticsApp(t)

	const registreID = 77
	if err := database.UpsertSearchDoc(&db.SearchDoc{
		EntityType:      "registre_raw",
		EntityID:        registreID,
		Published:       true,
		PersonFullNorm:  "joan pujo",
		PersonNomNorm:   "joan",
		CognomsCanon:    "pujol",
		PersonPhonetic:  "JN",
		CognomsPhonetic: "PJL",
	}); err != nil {
		t.Fatalf("UpsertSearchDoc ha fallat: %v", err)
	}

	app.applyModeracioBulkRegistreDerivedSideEffects(
		[]moderacioBulkRegistreState{{
			Reg: db.TranscripcioRaw{ID: registreID, ModeracioEstat: "publicat"},
		}},
		map[int]struct{}{registreID: {}},
		"rebutjat",
		map[moderacioBulkRegistreDemoKey][]int{},
		map[int][]int{},
		nil,
	)

	doc, err := database.GetSearchDoc("registre_raw", registreID)
	if err == nil && doc != nil {
		t.Fatalf("search_doc de registre %d no s'ha eliminat", registreID)
	}
}

func TestModeracioBulkMeasuredDurationAtLeastF316(t *testing.T) {
	got := measuredDurationAtLeast(0, 2*time.Millisecond, 3*time.Millisecond)
	if got != 5*time.Millisecond {
		t.Fatalf("durada coherent incorrecta: got %s", got)
	}
	got = measuredDurationAtLeast(10*time.Millisecond, 2*time.Millisecond, 3*time.Millisecond)
	if got != 10*time.Millisecond {
		t.Fatalf("durada elapsed hauria de prevaldre si és superior: got %s", got)
	}
}

func TestModeracioBulkRegistreSearchBuildUsesWarmupDocCacheF316(t *testing.T) {
	app, database := newModeracioBulkDiagnosticsApp(t)
	searchCtx := newModeracioBulkRegistreSearchContext()
	registre := db.TranscripcioRaw{
		ID:             31601,
		ModeracioEstat: "pendent",
		DataActeISO:    sql.NullString{String: "1901-01-01", Valid: true},
		AnyDoc:         sql.NullInt64{Int64: 1901, Valid: true},
	}
	state := moderacioBulkRegistreState{
		Reg: registre,
		Persones: []db.TranscripcioPersonaRaw{{
			TranscripcioID: registre.ID,
			Rol:            "batejat",
			Nom:            "Joan",
			Cognom1:        "Serra",
		}},
	}
	successSet := map[int]struct{}{registre.ID: {}}

	warmupStats := app.warmModeracioBulkRegistreSearchCache([]moderacioBulkRegistreState{state}, successSet, "publicat", searchCtx)
	if warmupStats.Docs != 1 {
		t.Fatalf("warmup hauria de construir 1 search doc, got %d", warmupStats.Docs)
	}
	if len(searchCtx.SearchDocCache) != 1 {
		t.Fatalf("warmup hauria de deixar 1 search doc cachejat, got %d", len(searchCtx.SearchDocCache))
	}

	metrics := app.applyModeracioBulkRegistreDerivedSideEffects(
		[]moderacioBulkRegistreState{state},
		successSet,
		"publicat",
		map[moderacioBulkRegistreDemoKey][]int{},
		map[int][]int{},
		searchCtx,
	)
	if metrics.SearchDocCacheHits != 1 || metrics.SearchDocCacheMisses != 0 {
		t.Fatalf("cache de search doc inesperada: hits=%d misses=%d warmup_cognom_hits=%d warmup_cognom_misses=%d", metrics.SearchDocCacheHits, metrics.SearchDocCacheMisses, warmupStats.Hits, warmupStats.Misses)
	}
	if metrics.SearchDocsUpserts != 1 {
		t.Fatalf("search_docs upserts esperat 1, got %d", metrics.SearchDocsUpserts)
	}
	if metrics.SearchDur < metrics.SearchBuildDur+metrics.SearchUpsertDur+metrics.SearchDeleteDur {
		t.Fatalf("SearchDur incoherent: total=%s build=%s upsert=%s delete=%s", metrics.SearchDur, metrics.SearchBuildDur, metrics.SearchUpsertDur, metrics.SearchDeleteDur)
	}
	doc, err := database.GetSearchDoc("registre_raw", registre.ID)
	if err != nil || doc == nil {
		t.Fatalf("search_doc no creat: doc=%v err=%v", doc, err)
	}
}

func TestNomCognomContribMetricsF319(t *testing.T) {
	registre := db.TranscripcioRaw{
		TipusActe: "baptisme",
		AnyDoc:    sql.NullInt64{Int64: 1901, Valid: true},
	}
	persones := []db.TranscripcioPersonaRaw{
		{Rol: "batejat", Nom: "Joan", Cognom1: "Pujol", Cognom2: "Serra"},
		{Rol: "testimoni", Nom: "Pere", Cognom1: "Ferrer"},
	}
	metrics := nomCognomContribMetrics{Cache: newNomCognomContribCache()}
	contrib := calcNomCognomContribsWithMetrics(registre, persones, &metrics)

	if contrib.AnyDoc != 1901 {
		t.Fatalf("AnyDoc inesperat: %d", contrib.AnyDoc)
	}
	if contrib.NomCounts[NormalizeNomKey("Joan")] != 1 {
		t.Fatalf("nom esperat no agregat: %#v", contrib.NomCounts)
	}
	if contrib.CognomCounts[NormalizeCognomKey("Pujol")] != 1 || contrib.CognomCounts[NormalizeCognomKey("Serra")] != 1 {
		t.Fatalf("cognoms esperats no agregats: %#v", contrib.CognomCounts)
	}
	if metrics.Persones != 2 || metrics.Matched != 1 || metrics.NomValues != 1 || metrics.CognomValues != 2 {
		t.Fatalf("mètriques de contribució inesperades: %#v", metrics)
	}
	secondMetrics := nomCognomContribMetrics{Cache: metrics.Cache}
	_ = calcNomCognomContribsWithMetrics(registre, persones, &secondMetrics)
	if secondMetrics.NomCacheHits != 1 || secondMetrics.CognomCacheHits != 2 {
		t.Fatalf("cache de contribució no reutilitzada: %#v", secondMetrics)
	}
}
