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
	ruleLlibrePageStatsUpdate = "llibre_page_stats_update"
	ruleMunicipiMapaSubmit  = "municipi_mapa_submit"
	ruleMunicipiMapaApprove = "municipi_mapa_approve"
	ruleMunicipiMapaReject  = "municipi_mapa_reject"
	ruleMunicipiHistoriaGeneralSubmit = "municipi_historia_general_submit"
	ruleMunicipiHistoriaFetSubmit     = "municipi_historia_fet_submit"
	ruleMunicipiAnecdotaPublicada      = "municipi_anecdota_publicada"
)

const (
	antiAbuseBurstWindow   = 5 * time.Minute
	antiAbuseBurstThreshold = 30
	antiAbuseRejectWindow  = 24 * time.Hour
	antiAbuseRejectThreshold = 10
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
	trigger := AchievementTrigger{
		ActivityID: id,
		RuleCode:   ruleCode,
		Action:     action,
		ObjectType: objectType,
		Status:     act.Status,
		CreatedAt:  act.CreatedAt,
	}
	if objectID != nil {
		trigger.ObjectID = *objectID
	}
	a.EvaluateAchievementsForUser(ctx, userID, trigger)
	a.logAntiAbuseSignals(userID, act.CreatedAt)
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
	ruleCode := ""
	if act.RuleID.Valid {
		if rule, err := a.DB.GetPointsRule(int(act.RuleID.Int64)); err == nil && rule != nil {
			ruleCode = rule.Code
		}
	}
	objID := 0
	if act.ObjectID.Valid {
		objID = int(act.ObjectID.Int64)
	}
	trigger := AchievementTrigger{
		ActivityID: act.ID,
		RuleCode:   ruleCode,
		Action:     act.Action,
		ObjectType: act.ObjectType,
		ObjectID:   objID,
		Status:     "validat",
		CreatedAt:  act.CreatedAt,
	}
	a.EvaluateAchievementsForUser(context.Background(), act.UserID, trigger)
	return nil
}

// CancelActivity marca una activitat com a anul·lada (no suma punts).
func (a *App) CancelActivity(activityID int, moderatorID int) error {
	act, err := a.DB.GetUserActivity(activityID)
	if err != nil {
		return err
	}
	if act.Status == "anulat" {
		return nil
	}
	if err := a.DB.UpdateUserActivityStatus(activityID, "anulat", &moderatorID); err != nil {
		return err
	}
	a.logAntiAbuseSignals(act.UserID, time.Now())
	return nil
}

func (a *App) logAntiAbuseSignals(userID int, now time.Time) {
	if userID <= 0 {
		return
	}
	burstFilter := db.AchievementActivityFilter{
		UserID: userID,
		From:   now.Add(-antiAbuseBurstWindow),
		To:     now,
	}
	burstCount, err := a.DB.CountUserActivities(burstFilter)
	if err == nil && burstCount >= antiAbuseBurstThreshold {
		Infof("Antiabuse burst user=%d count=%d window=%s", userID, burstCount, antiAbuseBurstWindow.String())
	}
	rejectFilter := db.AchievementActivityFilter{
		UserID:   userID,
		Statuses: []string{"anulat"},
		From:     now.Add(-antiAbuseRejectWindow),
		To:       now,
	}
	rejectCount, err := a.DB.CountUserActivities(rejectFilter)
	if err == nil && rejectCount >= antiAbuseRejectThreshold {
		Infof("Antiabuse rejects user=%d count=%d window=%s", userID, rejectCount, antiAbuseRejectWindow.String())
	}
}
