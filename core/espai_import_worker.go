package core

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	espaiImportWorkerDefaultPollSeconds = 5
	espaiImportWorkerDefaultBatch       = 10
)

type espaiImportWorkerConfig struct {
	PollInterval time.Duration
	BatchSize    int
}

type espaiImportWorkerState struct {
	mu      sync.Mutex
	running map[int]int
	active  map[int]struct{}
}

var espaiImportWorker = &espaiImportWorkerState{
	running: map[int]int{},
	active:  map[int]struct{}{},
}

func (a *App) espaiImportWorkerConfig() espaiImportWorkerConfig {
	pollSeconds := parseIntDefault(a.Config["ESP_IMPORT_WORKER_POLL_SECONDS"], espaiImportWorkerDefaultPollSeconds)
	if pollSeconds <= 0 {
		pollSeconds = espaiImportWorkerDefaultPollSeconds
	}
	batch := parseIntDefault(a.Config["ESP_IMPORT_WORKER_BATCH"], espaiImportWorkerDefaultBatch)
	if batch <= 0 {
		batch = espaiImportWorkerDefaultBatch
	}
	return espaiImportWorkerConfig{
		PollInterval: time.Duration(pollSeconds) * time.Second,
		BatchSize:    batch,
	}
}

func (a *App) StartEspaiImportWorker() {
	cfg := a.espaiImportWorkerConfig()
	if cfg.PollInterval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(cfg.PollInterval)
		defer ticker.Stop()
		a.dispatchEspaiImports(cfg)
		for range ticker.C {
			a.dispatchEspaiImports(cfg)
		}
	}()
}

func (a *App) dispatchEspaiImports(cfg espaiImportWorkerConfig) {
	if a == nil || a.DB == nil {
		return
	}
	imports, err := a.DB.ListEspaiImportsByStatus("queued", cfg.BatchSize)
	if err != nil || len(imports) == 0 {
		return
	}
	for _, imp := range imports {
		limit := a.importWorkerLimitForUser(imp.OwnerUserID)
		if !espaiImportWorker.tryStart(imp.OwnerUserID, imp.ID, limit) {
			continue
		}
		impCopy := imp
		go a.runEspaiImportJob(&impCopy)
	}
}

func (w *espaiImportWorkerState) tryStart(ownerID, importID, limit int) bool {
	if ownerID <= 0 || importID <= 0 {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.active[importID]; ok {
		return false
	}
	if limit > 0 && w.running[ownerID] >= limit {
		return false
	}
	w.active[importID] = struct{}{}
	w.running[ownerID]++
	return true
}

func (w *espaiImportWorkerState) finish(ownerID, importID int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if ownerID > 0 {
		if w.running[ownerID] > 0 {
			w.running[ownerID]--
		}
		if w.running[ownerID] <= 0 {
			delete(w.running, ownerID)
		}
	}
	delete(w.active, importID)
}

func (a *App) runEspaiImportJob(imp *db.EspaiImport) {
	if imp == nil {
		return
	}
	defer espaiImportWorker.finish(imp.OwnerUserID, imp.ID)

	err := a.executeEspaiImportJob(imp)
	if err != nil {
		_ = a.DB.UpdateEspaiImportStatus(imp.ID, "error", err.Error(), "")
	}
}

func (a *App) executeEspaiImportJob(imp *db.EspaiImport) error {
	importType := strings.TrimSpace(imp.ImportType)
	switch importType {
	case "gedcom":
		return a.executeEspaiGedcomImport(imp)
	case "gramps":
		return a.executeEspaiGrampsImport(imp)
	default:
		return fmt.Errorf("import type no suportat: %s", importType)
	}
}

func (a *App) executeEspaiGedcomImport(imp *db.EspaiImport) error {
	if imp == nil {
		return errors.New("import record missing")
	}
	if !imp.FontID.Valid {
		return errors.New(T("cat", "space.gedcom.error.missing_font"))
	}
	font, err := a.DB.GetEspaiFontImportacio(int(imp.FontID.Int64))
	if err != nil || font == nil {
		return errors.New(T("cat", "space.gedcom.error.missing_font"))
	}
	if !font.StoragePath.Valid || strings.TrimSpace(font.StoragePath.String) == "" {
		return errors.New(T("cat", "space.gedcom.error.missing_file"))
	}
	if _, err := os.Stat(font.StoragePath.String); err != nil {
		return errors.New(T("cat", "space.gedcom.error.missing_file"))
	}
	mode := strings.TrimSpace(imp.ImportMode)
	if strings.EqualFold(mode, "merge") {
		if err := a.processGedcomImportMerge(imp, font.StoragePath.String); err != nil {
			return err
		}
		return nil
	}
	if err := a.processGedcomImport(imp, font.StoragePath.String); err != nil {
		return err
	}
	return nil
}

func (a *App) executeEspaiGrampsImport(imp *db.EspaiImport) error {
	if imp == nil {
		return errors.New("import record missing")
	}
	integ, err := a.findGrampsIntegrationForImport(imp.OwnerUserID, imp.ArbreID)
	if err != nil {
		return err
	}
	if integ == nil {
		return errors.New(T("cat", "space.gramps.error.not_found"))
	}
	_ = a.DB.UpdateEspaiImportStatus(imp.ID, "parsing", "", "")
	if err := a.syncGrampsIntegration(context.Background(), integ, true); err != nil {
		return err
	}
	return a.DB.UpdateEspaiImportStatus(imp.ID, "done", "", "")
}

func (a *App) findGrampsIntegrationForImport(ownerID, arbreID int) (*db.EspaiIntegracioGramps, error) {
	if ownerID <= 0 || arbreID <= 0 {
		return nil, errors.New(T("cat", "space.gramps.error.not_found"))
	}
	integracions, err := a.DB.ListEspaiIntegracionsGrampsByOwner(ownerID)
	if err != nil {
		return nil, err
	}
	for i := range integracions {
		if integracions[i].ArbreID == arbreID {
			return &integracions[i], nil
		}
	}
	return nil, nil
}

func (a *App) queueGrampsImport(ownerID, arbreID int) (*db.EspaiImport, error) {
	if ownerID <= 0 || arbreID <= 0 {
		return nil, errors.New(T("cat", "space.gramps.error.not_found"))
	}
	importRec := &db.EspaiImport{
		OwnerUserID: ownerID,
		ArbreID:     arbreID,
		ImportType:  "gramps",
		ImportMode:  "sync",
		Status:      "queued",
	}
	if _, err := a.DB.CreateEspaiImport(importRec); err != nil {
		return nil, err
	}
	return importRec, nil
}
