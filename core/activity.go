package core

import (
	"context"
	"database/sql"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	rulePersonaCreate   = "persona_create"
	rulePersonaUpdate   = "persona_update"
	rulePaginaIndex     = "llibre_pagina_index"
	ruleModeracioApprove = "moderacio_approve"
	ruleModeracioReject  = "moderacio_reject"
	ruleArxiuCreate      = "arxiu_create"
	ruleArxiuUpdate      = "arxiu_update"
	ruleLlibreCreate     = "llibre_create"
	ruleLlibreUpdate     = "llibre_update"
	ruleNivellCreate     = "nivell_create"
	ruleNivellUpdate     = "nivell_update"
	ruleMunicipiCreate   = "municipi_create"
	ruleMunicipiUpdate   = "municipi_update"
	ruleEclesiasticCreate = "eclesiastic_create"
	ruleEclesiasticUpdate = "eclesiastic_update"
)

// RegisterUserActivity crea una entrada d'activitat i, si està validada, suma punts.
func (a *App) RegisterUserActivity(ctx context.Context, userID int, ruleCode, action, objectType string, objectID *int, status string, moderatedBy *int, details string) (int, error) {
	var (
		points int
		ruleID sql.NullInt64
	)
	if ruleCode != "" {
		if r, err := a.DB.GetPointsRuleByCode(ruleCode); err == nil && r != nil && r.Active {
			points = r.Points
			ruleID = sql.NullInt64{Int64: int64(r.ID), Valid: true}
		}
	}
	var obj sql.NullInt64
	if objectID != nil {
		obj = sql.NullInt64{Int64: int64(*objectID), Valid: true}
	}
	var mod sql.NullInt64
	if moderatedBy != nil {
		mod = sql.NullInt64{Int64: int64(*moderatedBy), Valid: true}
	}
	if status == "" {
		status = "validat"
	}
	act := &db.UserActivity{
		UserID:      userID,
		RuleID:      ruleID,
		Action:      action,
		ObjectType:  objectType,
		ObjectID:    obj,
		Points:      points,
		Status:      status,
		ModeratedBy: mod,
		Details:     details,
		CreatedAt:   time.Now(),
	}
	id, err := a.DB.InsertUserActivity(act)
	if err != nil {
		return 0, err
	}
	if status == "validat" && points != 0 {
		if err := a.DB.AddPointsToUser(userID, points); err != nil {
			return id, err
		}
	}
	_ = ctx
	return id, nil
}

// ValidateActivity canvia l'estat d'una activitat pendent a validat i aplica punts si cal.
func (a *App) ValidateActivity(activityID int, moderatorID int) error {
	act, err := a.DB.GetUserActivity(activityID)
	if err != nil {
		return err
	}
	if act.Status == "validat" {
		return nil
	}
	if err := a.DB.UpdateUserActivityStatus(activityID, "validat", &moderatorID); err != nil {
		return err
	}
	if act.Points != 0 {
		if err := a.DB.AddPointsToUser(act.UserID, act.Points); err != nil {
			return err
		}
	}
	return nil
}

// CancelActivity marca una activitat com a anul·lada (no suma punts).
func (a *App) CancelActivity(activityID int, moderatorID int) error {
	return a.DB.UpdateUserActivityStatus(activityID, "anulat", &moderatorID)
}
