package core

import (
	"path/filepath"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) queueMediaDeepZoom(cfg mediaConfig, albumPublicID string, item db.MediaItem) {
	if a == nil || a.DB == nil {
		return
	}
	go a.buildMediaDeepZoom(cfg, albumPublicID, item)
}

func (a *App) buildMediaDeepZoom(cfg mediaConfig, albumPublicID string, item db.MediaItem) {
	originalPath := filepath.Join(cfg.Root, filepath.FromSlash(item.StorageKeyOriginal))
	outputDir := filepath.Join(cfg.Root, albumPublicID, item.PublicID, "dz")

	if _, _, _, err := BuildDeepZoom(originalPath, outputDir, 256, "jpg"); err != nil {
		Errorf("Error generant DeepZoom per %s: %v", item.PublicID, err)
		_ = a.DB.UpdateMediaItemDerivativesStatus(item.ID, "failed")
		return
	}

	if err := a.DB.UpdateMediaItemDerivativesStatus(item.ID, "ready"); err != nil {
		Errorf("Error actualitzant derivatives_status per %s: %v", item.PublicID, err)
	}
}
