package core

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type espaiGroupSummary struct {
	Members   int
	Trees     int
	Conflicts int
}

type espaiGroupMemberView struct {
	ID        int
	UserID    int
	Name      string
	Email     string
	Role      string
	Status    string
	JoinedAt  sql.NullTime
	CreatedAt sql.NullTime
}

type espaiGroupTreeView struct {
	ID        int
	TreeID    int
	Name      string
	OwnerID   int
	OwnerName string
	Status    string
}

type espaiGroupChangeView struct {
	ID        int
	Action    string
	ActorID   sql.NullInt64
	ActorName string
	Object    string
	Payload   string
	PayloadPretty string
	CreatedAt sql.NullTime
}

type espaiGroupPersonOption struct {
	ID    int
	Label string
}

var groupRoleRank = map[string]int{
	"viewer": 0,
	"member": 1,
	"admin":  2,
	"owner":  3,
}

func (a *App) EspaiPersonalGrupsPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}

	groups, _ := a.DB.ListEspaiGrupsByUser(user.ID)
	summaries := map[int]espaiGroupSummary{}
	for _, g := range groups {
		members, _ := a.DB.ListEspaiGrupMembres(g.ID)
		trees, _ := a.DB.ListEspaiGrupArbres(g.ID)
		conflicts, _ := a.DB.ListEspaiGrupConflictes(g.ID)
		summary := espaiGroupSummary{Members: len(members), Trees: len(trees)}
		for _, c := range conflicts {
			if strings.TrimSpace(c.Status) == "pending" {
				summary.Conflicts++
			}
		}
		summaries[g.ID] = summary
	}

	spaceState := "ready"
	if len(groups) == 0 {
		spaceState = "empty"
	}

	groupID := parseFormInt(r.URL.Query().Get("group_id"))
	var selected *db.EspaiGrup
	var member *db.EspaiGrupMembre
	var memberViews []espaiGroupMemberView
	var treeViews []espaiGroupTreeView
	var conflictViews []db.EspaiGrupConflicte
	var changeViews []espaiGroupChangeView
	var selectedChange *espaiGroupChangeView
	var availableTrees []db.EspaiArbre

	if groupID > 0 {
		selected, member = a.loadEspaiGrupAccess(user.ID, groupID)
		if selected != nil && member != nil {
			memberViews = a.buildGroupMembersView(selected.ID)
			treeViews = a.buildGroupTreesView(selected.ID)
			conflictViews = a.buildGroupConflictsView(selected.ID, strings.TrimSpace(r.URL.Query().Get("conflict_status")))
			changeViews = a.buildGroupChangesView(selected.ID, r.URL.Query())
			changeID := parseFormInt(r.URL.Query().Get("change_id"))
			if changeID > 0 {
				for i := range changeViews {
					if changeViews[i].ID == changeID {
						selectedChange = &changeViews[i]
						break
					}
				}
			}
			availableTrees = a.availableGroupTrees(user.ID, selected.ID)
		}
	}

	RenderPrivateTemplate(w, r, "espai.html", map[string]interface{}{
		"SpaceSection":     "grups",
		"SpaceState":       spaceState,
		"GroupList":        groups,
		"GroupSummaries":   summaries,
		"SelectedGroup":    selected,
		"SelectedMember":   member,
		"GroupMembers":     memberViews,
		"GroupTrees":       treeViews,
		"GroupConflicts":   conflictViews,
		"GroupChanges":     changeViews,
		"SelectedChange":   selectedChange,
		"AvailableTrees":   availableTrees,
		"UploadError":      strings.TrimSpace(r.URL.Query().Get("error")),
		"UploadNotice":     strings.TrimSpace(r.URL.Query().Get("notice")),
		"ConflictFilter":   strings.TrimSpace(r.URL.Query().Get("conflict_status")),
		"ChangeActor":      strings.TrimSpace(r.URL.Query().Get("actor_id")),
		"ChangeAction":     strings.TrimSpace(r.URL.Query().Get("action")),
		"ChangeFrom":       strings.TrimSpace(r.URL.Query().Get("from")),
		"ChangeTo":         strings.TrimSpace(r.URL.Query().Get("to")),
		"CurrentUserID":    user.ID,
	})
}

func (a *App) EspaiGrupsCreate(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	desc := strings.TrimSpace(r.FormValue("description"))
	if name == "" {
		http.Redirect(w, r, "/espai/grups?error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.name_required")), http.StatusSeeOther)
		return
	}
	group := &db.EspaiGrup{
		OwnerUserID: user.ID,
		Nom:         name,
		Descripcio:  sqlNullString(desc),
		Status:      "active",
	}
	if _, err := a.DB.CreateEspaiGrup(group); err != nil {
		http.Redirect(w, r, "/espai/grups?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	joinedAt := sql.NullTime{Time: time.Now(), Valid: true}
	_, _ = a.DB.AddEspaiGrupMembre(&db.EspaiGrupMembre{
		GrupID:   group.ID,
		UserID:   user.ID,
		Role:     "owner",
		Status:   "active",
		JoinedAt: joinedAt,
	})
	a.logEspaiGroupChange(group.ID, user.ID, "group_created", "group", group.ID, map[string]interface{}{"name": name})
	http.Redirect(w, r, "/espai/grups?group_id="+intToStr(group.ID)+"&notice="+urlQueryEscape(T(ResolveLang(r), "space.groups.notice.created")), http.StatusSeeOther)
}

func (a *App) EspaiGrupsInvite(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	groupID := parseFormInt(r.FormValue("group_id"))
	email := strings.TrimSpace(r.FormValue("email"))
	role := strings.TrimSpace(r.FormValue("role"))
	if groupID == 0 || email == "" {
		http.Redirect(w, r, "/espai/grups?error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.invite_missing")), http.StatusSeeOther)
		return
	}
	group, member := a.loadEspaiGrupAccess(user.ID, groupID)
	if group == nil || !groupRoleAllows(member, "admin") {
		http.Redirect(w, r, "/espai/grups?error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.not_allowed")), http.StatusSeeOther)
		return
	}
	if !isValidGroupRole(role) {
		role = "member"
	}
	targetUser, err := a.DB.GetUserByEmail(email)
	if err != nil || targetUser == nil {
		http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID)+"&error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.user_not_found")), http.StatusSeeOther)
		return
	}
	existing, _ := a.DB.GetEspaiGrupMembre(groupID, targetUser.ID)
	if existing != nil && existing.ID > 0 {
		existing.Role = role
		existing.Status = "invited"
		existing.JoinedAt = sql.NullTime{}
		_ = a.DB.UpdateEspaiGrupMembre(existing)
	} else {
		_, _ = a.DB.AddEspaiGrupMembre(&db.EspaiGrupMembre{
			GrupID: groupID,
			UserID: targetUser.ID,
			Role:   role,
			Status: "invited",
		})
	}
	a.logEspaiGroupChange(group.ID, user.ID, "member_invited", "user", targetUser.ID, map[string]interface{}{"email": email, "role": role})
	http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID)+"&notice="+urlQueryEscape(T(ResolveLang(r), "space.groups.notice.invited")), http.StatusSeeOther)
}

func (a *App) EspaiGrupsAcceptInvite(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	groupID := parseFormInt(r.FormValue("group_id"))
	member, err := a.DB.GetEspaiGrupMembre(groupID, user.ID)
	if err != nil || member == nil || member.Status != "invited" {
		http.Redirect(w, r, "/espai/grups?error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.invite_not_found")), http.StatusSeeOther)
		return
	}
	member.Status = "active"
	member.JoinedAt = sql.NullTime{Time: time.Now(), Valid: true}
	_ = a.DB.UpdateEspaiGrupMembre(member)
	a.logEspaiGroupChange(groupID, user.ID, "member_accepted", "user", user.ID, nil)
	http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID)+"&notice="+urlQueryEscape(T(ResolveLang(r), "space.groups.notice.invite_accepted")), http.StatusSeeOther)
}

func (a *App) EspaiGrupsDeclineInvite(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	groupID := parseFormInt(r.FormValue("group_id"))
	member, err := a.DB.GetEspaiGrupMembre(groupID, user.ID)
	if err != nil || member == nil || member.Status != "invited" {
		http.Redirect(w, r, "/espai/grups?error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.invite_not_found")), http.StatusSeeOther)
		return
	}
	member.Status = "removed"
	member.JoinedAt = sql.NullTime{}
	_ = a.DB.UpdateEspaiGrupMembre(member)
	a.logEspaiGroupChange(groupID, user.ID, "member_declined", "user", user.ID, nil)
	http.Redirect(w, r, "/espai/grups?notice="+urlQueryEscape(T(ResolveLang(r), "space.groups.notice.invite_declined")), http.StatusSeeOther)
}

func (a *App) EspaiGrupsUpdateMember(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	groupID := parseFormInt(r.FormValue("group_id"))
	targetID := parseFormInt(r.FormValue("user_id"))
	role := strings.TrimSpace(r.FormValue("role"))
	action := strings.TrimSpace(r.FormValue("action"))
	if groupID == 0 || targetID == 0 {
		http.Redirect(w, r, "/espai/grups?error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.member_not_found")), http.StatusSeeOther)
		return
	}
	group, member := a.loadEspaiGrupAccess(user.ID, groupID)
	if group == nil || !groupRoleAllows(member, "admin") {
		http.Redirect(w, r, "/espai/grups?error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.not_allowed")), http.StatusSeeOther)
		return
	}
	targetMember, err := a.DB.GetEspaiGrupMembre(groupID, targetID)
	if err != nil || targetMember == nil {
		http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID)+"&error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.member_not_found")), http.StatusSeeOther)
		return
	}
	if action == "remove" {
		if strings.TrimSpace(targetMember.Role) == "owner" && member.Role != "owner" {
			http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID)+"&error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.owner_only")), http.StatusSeeOther)
			return
		}
		targetMember.Status = "removed"
		_ = a.DB.UpdateEspaiGrupMembre(targetMember)
		a.logEspaiGroupChange(group.ID, user.ID, "member_removed", "user", targetID, nil)
		http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID)+"&notice="+urlQueryEscape(T(ResolveLang(r), "space.groups.notice.member_removed")), http.StatusSeeOther)
		return
	}
	if strings.TrimSpace(role) != "" {
		if !isValidGroupRole(role) {
			role = targetMember.Role
		}
		if targetMember.Role == "owner" && role != "owner" {
			http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID)+"&error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.owner_only")), http.StatusSeeOther)
			return
		}
		if member.Role != "owner" && role == "owner" {
			http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID)+"&error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.owner_only")), http.StatusSeeOther)
			return
		}
		targetMember.Role = role
		_ = a.DB.UpdateEspaiGrupMembre(targetMember)
		a.logEspaiGroupChange(group.ID, user.ID, "member_role", "user", targetID, map[string]interface{}{"role": role})
		http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID)+"&notice="+urlQueryEscape(T(ResolveLang(r), "space.groups.notice.member_updated")), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID), http.StatusSeeOther)
}

func (a *App) EspaiGrupsAddTree(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	groupID := parseFormInt(r.FormValue("group_id"))
	treeID := parseFormInt(r.FormValue("tree_id"))
	if groupID == 0 || treeID == 0 {
		http.Redirect(w, r, "/espai/grups?error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.tree_missing")), http.StatusSeeOther)
		return
	}
	group, member := a.loadEspaiGrupAccess(user.ID, groupID)
	if group == nil || !groupRoleAllows(member, "member") {
		http.Redirect(w, r, "/espai/grups?error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.not_allowed")), http.StatusSeeOther)
		return
	}
	tree, err := a.DB.GetEspaiArbre(treeID)
	if err != nil || tree == nil || tree.OwnerUserID != user.ID {
		http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID)+"&error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.tree_not_found")), http.StatusSeeOther)
		return
	}
	if _, err := a.DB.AddEspaiGrupArbre(&db.EspaiGrupArbre{GrupID: groupID, ArbreID: treeID, Status: "active"}); err != nil {
		_ = a.DB.UpdateEspaiGrupArbreStatus(groupID, treeID, "active")
	}
	a.logEspaiGroupChange(group.ID, user.ID, "tree_linked", "tree", treeID, map[string]interface{}{"name": tree.Nom})
	_, _ = a.rebuildGroupConflicts(groupID)
	http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID)+"&notice="+urlQueryEscape(T(ResolveLang(r), "space.groups.notice.tree_linked")), http.StatusSeeOther)
}

func (a *App) EspaiGrupsRemoveTree(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	groupID := parseFormInt(r.FormValue("group_id"))
	treeID := parseFormInt(r.FormValue("tree_id"))
	if groupID == 0 || treeID == 0 {
		http.Redirect(w, r, "/espai/grups?error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.tree_missing")), http.StatusSeeOther)
		return
	}
	group, member := a.loadEspaiGrupAccess(user.ID, groupID)
	if group == nil || member == nil || strings.TrimSpace(member.Status) != "active" {
		http.Redirect(w, r, "/espai/grups?error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.not_allowed")), http.StatusSeeOther)
		return
	}
	tree, err := a.DB.GetEspaiArbre(treeID)
	if err != nil || tree == nil {
		http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID)+"&error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.tree_not_found")), http.StatusSeeOther)
		return
	}
	if !groupRoleAllows(member, "admin") && tree.OwnerUserID != user.ID {
		http.Redirect(w, r, "/espai/grups?error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.not_allowed")), http.StatusSeeOther)
		return
	}
	_ = a.DB.UpdateEspaiGrupArbreStatus(groupID, treeID, "removed")
	a.logEspaiGroupChange(group.ID, user.ID, "tree_unlinked", "tree", treeID, map[string]interface{}{"name": tree.Nom})
	http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID)+"&notice="+urlQueryEscape(T(ResolveLang(r), "space.groups.notice.tree_removed")), http.StatusSeeOther)
}

func (a *App) EspaiGrupsRebuildConflicts(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	groupID := parseFormInt(r.FormValue("group_id"))
	group, member := a.loadEspaiGrupAccess(user.ID, groupID)
	if group == nil || !groupRoleAllows(member, "admin") {
		http.Redirect(w, r, "/espai/grups?error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.not_allowed")), http.StatusSeeOther)
		return
	}
	count, _ := a.rebuildGroupConflicts(groupID)
	a.logEspaiGroupChange(group.ID, user.ID, "conflicts_rebuild", "group", groupID, map[string]interface{}{"count": count})
	http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID)+"&notice="+urlQueryEscape(T(ResolveLang(r), "space.groups.notice.conflicts_rebuilt")), http.StatusSeeOther)
}

func (a *App) EspaiGrupsResolveConflict(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	groupID := parseFormInt(r.FormValue("group_id"))
	conflictID := parseFormInt(r.FormValue("conflict_id"))
	group, member := a.loadEspaiGrupAccess(user.ID, groupID)
	if group == nil || !groupRoleAllows(member, "admin") {
		http.Redirect(w, r, "/espai/grups?error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.not_allowed")), http.StatusSeeOther)
		return
	}
	_ = a.DB.UpdateEspaiGrupConflicteStatus(conflictID, "resolved", &user.ID)
	a.logEspaiGroupChange(group.ID, user.ID, "conflict_resolved", "conflict", conflictID, nil)
	http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID)+"&notice="+urlQueryEscape(T(ResolveLang(r), "space.groups.notice.conflict_resolved")), http.StatusSeeOther)
}

func (a *App) EspaiGrupsTreeView(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	groupID := parseFormInt(r.URL.Query().Get("group_id"))
	group, member := a.loadEspaiGrupAccess(user.ID, groupID)
	if group == nil || member == nil || member.Status != "active" {
		http.Redirect(w, r, "/espai/grups?error="+urlQueryEscape(T(ResolveLang(r), "space.groups.error.not_allowed")), http.StatusSeeOther)
		return
	}
	rootID := parseFormInt(r.URL.Query().Get("root_id"))
	dataset, options, err := a.buildGroupTreeDataset(group.ID, user.ID, rootID, ResolveLang(r))
	if err != nil {
		http.Redirect(w, r, "/espai/grups?group_id="+intToStr(groupID)+"&error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	view := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("view")))
	if view != "familiar" && view != "ventall" {
		view = "pedigree"
	}
	gens := parseTreeGens(r.URL.Query().Get("gens"), treeDefaultGens)
	treeI18n := treeI18nMap(ResolveLang(r))

	RenderPrivateTemplate(w, r, "espai-grup-arbre.html", map[string]interface{}{
		"Group":        group,
		"GroupID":      group.ID,
		"GroupName":    group.Nom,
		"View":         view,
		"Gens":         gens,
		"FamilyData":   dataset.FamilyData,
		"FamilyLinks":  dataset.FamilyLinks,
		"RootPersonId": dataset.RootPersonID,
		"DatasetStats": dataset.DatasetStats,
		"TreeI18n":     treeI18n,
		"RootOptions":  options,
	})
}

func (a *App) loadEspaiGrupAccess(userID, groupID int) (*db.EspaiGrup, *db.EspaiGrupMembre) {
	group, err := a.DB.GetEspaiGrup(groupID)
	if err != nil || group == nil {
		return nil, nil
	}
	member, err := a.DB.GetEspaiGrupMembre(groupID, userID)
	if err != nil || member == nil || member.Status == "removed" {
		return nil, nil
	}
	return group, member
}

func groupRoleAllows(member *db.EspaiGrupMembre, min string) bool {
	if member == nil {
		return false
	}
	if strings.TrimSpace(member.Status) != "active" {
		return false
	}
	return groupRoleRank[strings.TrimSpace(member.Role)] >= groupRoleRank[min]
}

func isValidGroupRole(role string) bool {
	_, ok := groupRoleRank[strings.TrimSpace(role)]
	return ok
}

func (a *App) buildGroupMembersView(groupID int) []espaiGroupMemberView {
	members, _ := a.DB.ListEspaiGrupMembres(groupID)
	views := make([]espaiGroupMemberView, 0, len(members))
	for _, m := range members {
		if m.Status == "removed" {
			continue
		}
		user, _ := a.DB.GetUserByID(m.UserID)
		view := espaiGroupMemberView{
			ID:        m.ID,
			UserID:    m.UserID,
			Name:      groupUserDisplayName(user),
			Email:     "",
			Role:      m.Role,
			Status:    m.Status,
			JoinedAt:  m.JoinedAt,
			CreatedAt: m.CreatedAt,
		}
		if user != nil {
			view.Email = user.Email
		}
		views = append(views, view)
	}
	sort.Slice(views, func(i, j int) bool { return views[i].UserID < views[j].UserID })
	return views
}

func (a *App) buildGroupTreesView(groupID int) []espaiGroupTreeView {
	links, _ := a.DB.ListEspaiGrupArbres(groupID)
	out := []espaiGroupTreeView{}
	for _, link := range links {
		if link.Status != "active" {
			continue
		}
		tree, err := a.DB.GetEspaiArbre(link.ArbreID)
		if err != nil || tree == nil {
			continue
		}
		owner, _ := a.DB.GetUserByID(tree.OwnerUserID)
		out = append(out, espaiGroupTreeView{
			ID:        link.ID,
			TreeID:    link.ArbreID,
			Name:      tree.Nom,
			OwnerID:   tree.OwnerUserID,
			OwnerName: groupUserDisplayName(owner),
			Status:    link.Status,
		})
	}
	return out
}

func (a *App) buildGroupConflictsView(groupID int, status string) []db.EspaiGrupConflicte {
	conflicts, _ := a.DB.ListEspaiGrupConflictes(groupID)
	if status == "" {
		return conflicts
	}
	filtered := []db.EspaiGrupConflicte{}
	for _, c := range conflicts {
		if strings.TrimSpace(c.Status) == status {
			filtered = append(filtered, c)
		}
	}
	return filtered
}

func (a *App) buildGroupChangesView(groupID int, q url.Values) []espaiGroupChangeView {
	actor := strings.TrimSpace(q.Get("actor_id"))
	action := strings.TrimSpace(q.Get("action"))
	fromDate := parseDateParam(q.Get("from"))
	toDate := parseDateParam(q.Get("to"))
	actorID := 0
	if actor != "" {
		actorID = parseFormInt(actor)
	}

	changes, _ := a.DB.ListEspaiGrupCanvis(groupID, 50)
	out := []espaiGroupChangeView{}
	for _, c := range changes {
		if actor != "" {
			if !c.ActorID.Valid || int(c.ActorID.Int64) != actorID {
				continue
			}
		}
		if action != "" && strings.TrimSpace(c.Action) != action {
			continue
		}
		if fromDate != nil && c.CreatedAt.Valid && c.CreatedAt.Time.Before(*fromDate) {
			continue
		}
		if toDate != nil && c.CreatedAt.Valid && c.CreatedAt.Time.After(*toDate) {
			continue
		}
		actorName := "-"
		if c.ActorID.Valid {
			if user, _ := a.DB.GetUserByID(int(c.ActorID.Int64)); user != nil {
				actorName = groupUserDisplayName(user)
			}
		}
		object := strings.TrimSpace(c.ObjectType.String)
		if object == "" {
			object = "-"
		}
		payload := ""
		payloadPretty := ""
		if c.PayloadJSON.Valid {
			payload = c.PayloadJSON.String
			payloadPretty = prettyJSON(payload)
		}
		out = append(out, espaiGroupChangeView{
			ID:        c.ID,
			Action:    c.Action,
			ActorID:   c.ActorID,
			ActorName: actorName,
			Object:    object,
			Payload:   payload,
			PayloadPretty: payloadPretty,
			CreatedAt: c.CreatedAt,
		})
	}
	return out
}

func parseDateParam(val string) *time.Time {
	val = strings.TrimSpace(val)
	if val == "" {
		return nil
	}
	if t, err := time.Parse("2006-01-02", val); err == nil {
		return &t
	}
	return nil
}

func prettyJSON(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var out bytes.Buffer
	if err := json.Indent(&out, []byte(raw), "", "  "); err != nil {
		return raw
	}
	return out.String()
}

func (a *App) availableGroupTrees(userID, groupID int) []db.EspaiArbre {
	all, _ := a.DB.ListEspaiArbresByOwner(userID)
	linked, _ := a.DB.ListEspaiGrupArbres(groupID)
	linkedSet := map[int]struct{}{}
	for _, l := range linked {
		if l.Status == "active" {
			linkedSet[l.ArbreID] = struct{}{}
		}
	}
	out := []db.EspaiArbre{}
	for _, t := range all {
		if _, ok := linkedSet[t.ID]; ok {
			continue
		}
		out = append(out, t)
	}
	return out
}

func (a *App) buildGroupTreeDataset(groupID int, userID int, rootID int, lang string) (treeDataset, []espaiGroupPersonOption, error) {
	dataset := treeDataset{}
	links, _ := a.DB.ListEspaiGrupArbres(groupID)
	if len(links) == 0 {
		return dataset, nil, errors.New(T(lang, "space.groups.error.tree_empty"))
	}

	personsByID := map[int]db.EspaiPersona{}
	personOptions := []espaiGroupPersonOption{}
	relations := []db.EspaiRelacio{}

	for _, link := range links {
		if link.Status != "active" {
			continue
		}
		tree, err := a.DB.GetEspaiArbre(link.ArbreID)
		if err != nil || tree == nil {
			continue
		}
		persones, _ := a.DB.ListEspaiPersonesByArbre(tree.ID)
		for _, p := range persones {
			if strings.TrimSpace(p.Status) != "" && strings.TrimSpace(p.Status) != "active" {
				continue
			}
			visibility := strings.TrimSpace(p.Visibility)
			if visibility == "" {
				visibility = "visible"
			}
			if visibility == "hidden" && tree.OwnerUserID != userID {
				continue
			}
			personsByID[p.ID] = p
			label := espaiPersonaDisplayName(p)
			if label == "-" {
				label = T(lang, "tree.unknown.name")
			}
			personOptions = append(personOptions, espaiGroupPersonOption{
				ID:    p.ID,
				Label: label + " · " + tree.Nom,
			})
		}
		rels, _ := a.DB.ListEspaiRelacionsByArbre(tree.ID)
		relations = append(relations, rels...)
	}

	if len(personsByID) == 0 {
		return dataset, nil, errors.New(T(lang, "space.groups.error.tree_empty"))
	}
	if rootID == 0 || personsByID[rootID].ID == 0 {
		rootID = pickFirstVisibleID(personsByID)
	}

	people := make([]treePerson, 0, len(personsByID))
	for _, p := range personsByID {
		people = append(people, treePerson{
			ID:   p.ID,
			Name: espaiPersonaDisplayName(p),
			Sex:  espaiSexToTree(p.Sexe),
		})
	}
	sort.Slice(people, func(i, j int) bool { return people[i].ID < people[j].ID })
	sort.Slice(personOptions, func(i, j int) bool { return personOptions[i].Label < personOptions[j].Label })

	parentMap := map[int]parentPair{}
	for _, rel := range relations {
		if personsByID[rel.PersonaID].ID == 0 || personsByID[rel.RelatedPersonaID].ID == 0 {
			continue
		}
		relType := strings.ToLower(strings.TrimSpace(rel.RelationType))
		switch relType {
		case "father", "mother", "parent":
			childID := rel.PersonaID
			parentID := rel.RelatedPersonaID
			pair := parentMap[childID]
			if relType == "father" {
				pair.Father = parentID
			} else if relType == "mother" {
				pair.Mother = parentID
			} else {
				if pair.Father == 0 {
					pair.Father = parentID
				} else if pair.Mother == 0 {
					pair.Mother = parentID
				}
			}
			parentMap[childID] = pair
		case "child":
			parentID := rel.PersonaID
			childID := rel.RelatedPersonaID
			sex := espaiSexToTree(personsByID[parentID].Sexe)
			pair := parentMap[childID]
			if sex == 1 {
				if pair.Mother == 0 {
					pair.Mother = parentID
				} else if pair.Father == 0 {
					pair.Father = parentID
				}
			} else {
				if pair.Father == 0 {
					pair.Father = parentID
				} else if pair.Mother == 0 {
					pair.Mother = parentID
				}
			}
			parentMap[childID] = pair
		}
	}
	linkSet := map[int]struct{}{}
	linksOut := make([]treeLink, 0, len(parentMap))
	for childID, pair := range parentMap {
		if pair.Father == 0 && pair.Mother == 0 {
			continue
		}
		linksOut = append(linksOut, treeLink{Child: childID, Father: pair.Father, Mother: pair.Mother})
		linkSet[childID] = struct{}{}
	}
	for id := range personsByID {
		if _, ok := linkSet[id]; ok {
			continue
		}
		linksOut = append(linksOut, treeLink{Child: id})
	}
	sort.Slice(linksOut, func(i, j int) bool { return linksOut[i].Child < linksOut[j].Child })

	dataset = treeDataset{
		FamilyData:   people,
		FamilyLinks:  linksOut,
		RootPersonID: rootID,
		DatasetStats: treeDatasetStats{People: len(people), Links: len(linksOut)},
	}
	return dataset, personOptions, nil
}

func (a *App) rebuildGroupConflicts(groupID int) (int, error) {
	personsByKey := map[string][]db.EspaiPersona{}
	links, _ := a.DB.ListEspaiGrupArbres(groupID)
	for _, link := range links {
		if link.Status != "active" {
			continue
		}
		persones, _ := a.DB.ListEspaiPersonesByArbre(link.ArbreID)
		for _, p := range persones {
			if strings.TrimSpace(p.Status) != "" && strings.TrimSpace(p.Status) != "active" {
				continue
			}
			key := groupPersonKey(p)
			if key == "" {
				continue
			}
			personsByKey[key] = append(personsByKey[key], p)
		}
	}
	if len(personsByKey) == 0 {
		return 0, nil
	}
	existing := map[string]bool{}
	conflicts, _ := a.DB.ListEspaiGrupConflictes(groupID)
	for _, c := range conflicts {
		if c.Summary.Valid {
			existing[c.Summary.String] = true
		}
	}
	created := 0
	for key, list := range personsByKey {
		if len(list) < 2 {
			continue
		}
		summary := "Possible duplicat: " + key
		if existing[summary] {
			continue
		}
		ids := []int{}
		for _, p := range list {
			ids = append(ids, p.ID)
		}
		payload, _ := json.Marshal(map[string]interface{}{
			"persona_ids": ids,
			"key":         key,
		})
		_, _ = a.DB.CreateEspaiGrupConflicte(&db.EspaiGrupConflicte{
			GrupID:       groupID,
			ArbreID:      list[0].ArbreID,
			ConflictType: "persona",
			Status:       "pending",
			Summary:      sqlNullString(summary),
			DetailsJSON:  sqlNullString(string(payload)),
		})
		created++
	}
	if created > 0 {
		a.notifyEspaiGroupConflicts(groupID, created)
	}
	return created, nil
}

func groupPersonKey(p db.EspaiPersona) string {
	name := strings.ToLower(strings.TrimSpace(espaiPersonaDisplayName(p)))
	if name == "-" {
		return ""
	}
	year := 0
	if p.DataNaixement.Valid {
		year = extractYear(p.DataNaixement.String)
	}
	if year == 0 && p.DataDefuncio.Valid {
		year = extractYear(p.DataDefuncio.String)
	}
	parts := []string{normalizeGroupToken(name)}
	if year > 0 {
		parts = append(parts, intToStr(year))
	}
	return strings.Join(parts, " ")
}

func normalizeGroupToken(val string) string {
	val = strings.ToLower(strings.TrimSpace(val))
	val = strings.ReplaceAll(val, "_", "")
	val = strings.ReplaceAll(val, "-", "")
	val = strings.ReplaceAll(val, ".", "")
	val = strings.ReplaceAll(val, ",", "")
	return val
}

func groupUserDisplayName(u *db.User) string {
	if u == nil {
		return "-"
	}
	name := strings.TrimSpace(strings.Join([]string{strings.TrimSpace(u.Name), strings.TrimSpace(u.Surname)}, " "))
	if name == "" {
		name = strings.TrimSpace(u.Usuari)
	}
	if name == "" {
		name = strings.TrimSpace(u.Email)
	}
	if name == "" {
		return "-"
	}
	return name
}

func (a *App) logEspaiGroupChange(groupID int, actorID int, action string, objectType string, objectID int, payload map[string]interface{}) {
	var payloadJSON sql.NullString
	if payload != nil {
		if raw, err := json.Marshal(payload); err == nil {
			payloadJSON = sql.NullString{String: string(raw), Valid: true}
		}
	}
	objType := sql.NullString{}
	if strings.TrimSpace(objectType) != "" {
		objType = sqlNullString(objectType)
	}
	objID := sql.NullInt64{}
	if objectID > 0 {
		objID = sql.NullInt64{Int64: int64(objectID), Valid: true}
	}
	actor := sql.NullInt64{Int64: int64(actorID), Valid: actorID > 0}
	_, _ = a.DB.CreateEspaiGrupCanvi(&db.EspaiGrupCanvi{
		GrupID:      groupID,
		ActorID:     actor,
		Action:      action,
		ObjectType:  objType,
		ObjectID:    objID,
		PayloadJSON: payloadJSON,
	})
}

func intToStr(val int) string {
	return strconv.Itoa(val)
}
