package core

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	mediaGrantHoursDefault      = 24
	mediaPointsBaseDefault      = 10
	mediaPointsFactorDefault    = 2
	mediaPointsPerCreditDefault = 10
)

var errInsufficientCredits = errors.New("insufficient credits")

type mediaCreditsConfig struct {
	GrantDuration   time.Duration
	PointsBase      int
	PointsFactor    int
	PointsPerCredit int
}

func (a *App) mediaCreditsConfig() mediaCreditsConfig {
	raw := map[string]string{}
	if a != nil && a.Config != nil {
		raw = a.Config
	}
	grantHours := parseIntDefault(raw["MEDIA_GRANT_HOURS"], mediaGrantHoursDefault)
	if grantHours <= 0 {
		grantHours = mediaGrantHoursDefault
	}
	pointsBase := parseIntDefault(raw["MEDIA_POINTS_BASE"], mediaPointsBaseDefault)
	if pointsBase < 0 {
		pointsBase = 0
	}
	pointsFactor := parseIntDefault(raw["MEDIA_POINTS_K"], mediaPointsFactorDefault)
	if pointsFactor < 0 {
		pointsFactor = 0
	}
	pointsPerCredit := parseIntDefault(raw["MEDIA_POINTS_PER_CREDIT"], mediaPointsPerCreditDefault)
	if pointsPerCredit <= 0 {
		pointsPerCredit = mediaPointsPerCreditDefault
	}
	return mediaCreditsConfig{
		GrantDuration:   time.Duration(grantHours) * time.Hour,
		PointsBase:      pointsBase,
		PointsFactor:    pointsFactor,
		PointsPerCredit: pointsPerCredit,
	}
}

func mediaCreditCost(album *db.MediaAlbum, item *db.MediaItem) int {
	if item != nil && item.CreditCost > 0 {
		return item.CreditCost
	}
	if album != nil && album.CreditCost > 0 {
		return album.CreditCost
	}
	return 0
}

func (a *App) mediaPointsForDifficulty(difficulty int) int {
	if difficulty < 0 {
		difficulty = 0
	}
	if difficulty > 100 {
		difficulty = 100
	}
	cfg := a.mediaCreditsConfig()
	return cfg.PointsBase + (difficulty * cfg.PointsFactor)
}

func generateGrantToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func (a *App) mediaEnsureAccessGrant(user *db.User, item *db.MediaItem, cost int) (*db.MediaAccessGrant, error) {
	if user == nil || item == nil {
		return nil, errors.New("missing user or item")
	}
	existing, err := a.DB.GetActiveMediaAccessGrant(user.ID, item.ID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return existing, nil
	}
	if cost < 0 {
		cost = 0
	}
	if cost > 0 {
		balance, err := a.DB.GetUserCreditsBalance(user.ID)
		if err != nil {
			return nil, err
		}
		if balance < cost {
			return nil, errInsufficientCredits
		}
		entry := &db.UserCreditsLedgerEntry{
			UserID:  user.ID,
			Delta:   -cost,
			Reason:  "spend_view_item",
			RefType: sql.NullString{String: "media_item", Valid: true},
			RefID:   sql.NullInt64{Int64: int64(item.ID), Valid: true},
		}
		if _, err := a.DB.InsertUserCreditsLedger(entry); err != nil {
			return nil, err
		}
	}
	token, err := generateGrantToken()
	if err != nil {
		return nil, err
	}
	cfg := a.mediaCreditsConfig()
	grant := &db.MediaAccessGrant{
		UserID:       user.ID,
		MediaItemID:  item.ID,
		GrantToken:   token,
		ExpiresAt:    time.Now().Add(cfg.GrantDuration),
		CreditsSpent: cost,
	}
	if id, err := a.DB.CreateMediaAccessGrant(grant); err != nil {
		if cost > 0 {
			refund := &db.UserCreditsLedgerEntry{
				UserID:  user.ID,
				Delta:   cost,
				Reason:  "refund",
				RefType: sql.NullString{String: "media_item", Valid: true},
				RefID:   sql.NullInt64{Int64: int64(item.ID), Valid: true},
			}
			_, _ = a.DB.InsertUserCreditsLedger(refund)
		}
		return nil, err
	} else {
		grant.ID = id
	}
	_, _ = a.DB.InsertMediaAccessLog(&db.MediaAccessLog{
		UserID:       user.ID,
		MediaItemID:  item.ID,
		AccessType:   "view",
		CreditsSpent: cost,
	})
	return grant, nil
}

func (a *App) mediaValidateGrantToken(user *db.User, item *db.MediaItem, token string) bool {
	if user == nil || item == nil {
		return false
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return false
	}
	grant, err := a.DB.GetMediaAccessGrantByToken(token)
	if err != nil || grant == nil {
		return false
	}
	if grant.UserID != user.ID || grant.MediaItemID != item.ID {
		return false
	}
	return true
}

func (a *App) recordUserPoints(userID int, points int, action, objectType string, objectID int, moderatedBy *int, details string) error {
	if userID <= 0 || points == 0 {
		return nil
	}
	obj := sql.NullInt64{}
	if objectID > 0 {
		obj = sql.NullInt64{Int64: int64(objectID), Valid: true}
	}
	mod := sql.NullInt64{}
	if moderatedBy != nil && *moderatedBy > 0 {
		mod = sql.NullInt64{Int64: int64(*moderatedBy), Valid: true}
	}
	act := &db.UserActivity{
		UserID:      userID,
		RuleID:      sql.NullInt64{},
		Action:      action,
		ObjectType:  objectType,
		ObjectID:    obj,
		Points:      points,
		Status:      "validat",
		ModeratedBy: mod,
		Details:     details,
		CreatedAt:   time.Now(),
	}
	if _, err := a.DB.InsertUserActivity(act); err != nil {
		return err
	}
	return a.DB.AddPointsToUser(userID, points)
}

func (a *App) ConvertPointsToCredits(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	if user == nil {
		user, _ = a.VerificarSessio(r)
	}
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	lang := resolveUserLang(r, user)
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/perfil?tab=activitat&error="+url.QueryEscape(T(lang, "profile.credits.convert.invalid")), http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Redirect(w, r, "/perfil?tab=activitat&error="+url.QueryEscape(T(lang, "error.csrf")), http.StatusSeeOther)
		return
	}
	points := parseIntDefault(r.FormValue("points"), 0)
	cfg := a.mediaCreditsConfig()
	if points <= 0 {
		http.Redirect(w, r, "/perfil?tab=activitat&error="+url.QueryEscape(T(lang, "profile.credits.convert.invalid")), http.StatusSeeOther)
		return
	}
	if points < cfg.PointsPerCredit {
		http.Redirect(w, r, "/perfil?tab=activitat&error="+url.QueryEscape(T(lang, "profile.credits.convert.invalid")), http.StatusSeeOther)
		return
	}
	if cfg.PointsPerCredit > 0 && points%cfg.PointsPerCredit != 0 {
		msg := fmt.Sprintf(T(lang, "profile.credits.convert.multiple"), cfg.PointsPerCredit)
		http.Redirect(w, r, "/perfil?tab=activitat&error="+url.QueryEscape(msg), http.StatusSeeOther)
		return
	}
	credits := points / cfg.PointsPerCredit
	if credits <= 0 {
		http.Redirect(w, r, "/perfil?tab=activitat&error="+url.QueryEscape(T(lang, "profile.credits.convert.invalid")), http.StatusSeeOther)
		return
	}
	userPoints, err := a.DB.GetUserPoints(user.ID)
	totalPoints := 0
	if err == nil && userPoints != nil {
		totalPoints = userPoints.Total
	}
	if totalPoints < points {
		http.Redirect(w, r, "/perfil?tab=activitat&error="+url.QueryEscape(T(lang, "profile.credits.convert.insufficient")), http.StatusSeeOther)
		return
	}
	entry := &db.UserCreditsLedgerEntry{
		UserID:  user.ID,
		Delta:   credits,
		Reason:  "earn_from_points",
		RefType: sql.NullString{String: "points", Valid: true},
	}
	if _, err := a.DB.InsertUserCreditsLedger(entry); err != nil {
		Errorf("Error afegint credits per usuari %d: %v", user.ID, err)
		http.Redirect(w, r, "/perfil?tab=activitat&error="+url.QueryEscape(T(lang, "profile.credits.convert.invalid")), http.StatusSeeOther)
		return
	}
	details := fmt.Sprintf("credits=%d rate=%d", credits, cfg.PointsPerCredit)
	if err := a.recordUserPoints(user.ID, -points, "convert_credits", "credits", 0, nil, details); err != nil {
		Errorf("Error restant punts per conversio usuari %d: %v", user.ID, err)
		refund := &db.UserCreditsLedgerEntry{
			UserID:  user.ID,
			Delta:   -credits,
			Reason:  "refund",
			RefType: sql.NullString{String: "points", Valid: true},
		}
		_, _ = a.DB.InsertUserCreditsLedger(refund)
		http.Redirect(w, r, "/perfil?tab=activitat&error="+url.QueryEscape(T(lang, "profile.credits.convert.invalid")), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/perfil?tab=activitat&success="+url.QueryEscape(T(lang, "profile.credits.convert.success")), http.StatusSeeOther)
}
