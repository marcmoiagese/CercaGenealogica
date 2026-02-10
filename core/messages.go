package core

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	dmMessageMaxLen     = 2000
	dmMessagePreviewLen = 140
	dmThreadListLimit   = 50
	dmThreadMsgLimit    = 60
	dmMessageRateLimit  = 0.05
	dmMessageRateBurst  = 30
	dmEmailSnippetMax   = 120
	dmFolderInboxToken  = "__inbox__"
)

type dmThreadView struct {
	ThreadID      int
	OtherUserID   int
	OtherLabel    string
	LastMessage   string
	LastMessageAt string
	Unread        bool
	Archived      bool
	Folder        string
	ThreadURL     string
	IsActive      bool
}

type dmMessageView struct {
	Body      string
	Sender    string
	SenderID  int
	CreatedAt string
	IsOwn     bool
}

func (a *App) requireMessagesView(w http.ResponseWriter, r *http.Request) (*db.User, bool) {
	return a.requirePermissionKeyAnyScope(w, r, permKeyMessagesView)
}

func parseDMFolderFilter(raw string) (string, *string) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	if trimmed == dmFolderInboxToken {
		empty := ""
		return trimmed, &empty
	}
	return trimmed, &trimmed
}

func buildFolderQuery(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	return "?folder=" + url.QueryEscape(raw)
}

func (a *App) buildDMThreadViews(userID int, filter db.DMThreadListFilter, activeThreadID int, folderParam string) []dmThreadView {
	threads, err := a.DB.ListDMThreadsForUser(userID, filter)
	if err != nil {
		Errorf("Error carregant inbox missatges: %v", err)
		threads = []db.DMThreadListItem{}
	}
	folderQuery := buildFolderQuery(folderParam)
	threadViews := make([]dmThreadView, 0, len(threads))
	for _, thread := range threads {
		if blocked, _ := a.DB.IsUserBlocked(userID, thread.OtherUserID); blocked {
			if filter.Archived == nil || !*filter.Archived || !thread.Archived {
				continue
			}
		}
		otherUser, _ := a.DB.GetUserByID(thread.OtherUserID)
		otherLabel := formatDMUserLabel(otherUser)
		lastMessage := strings.TrimSpace(stripMessageMarkup(thread.LastMessageBody))
		if lastMessage == "" {
			lastMessage = "—"
		}
		if utf8.RuneCountInString(lastMessage) > dmMessagePreviewLen {
			lastMessage = truncateRunes(lastMessage, dmMessagePreviewLen) + "…"
		}
		lastAt := ""
		if thread.LastMessageAt.Valid {
			lastAt = thread.LastMessageAt.Time.Format("02/01/2006 15:04")
		} else if thread.ThreadCreatedAt.Valid {
			lastAt = thread.ThreadCreatedAt.Time.Format("02/01/2006 15:04")
		}
		threadViews = append(threadViews, dmThreadView{
			ThreadID:      thread.ThreadID,
			OtherUserID:   thread.OtherUserID,
			OtherLabel:    otherLabel,
			LastMessage:   lastMessage,
			LastMessageAt: lastAt,
			Unread:        thread.Unread,
			Archived:      thread.Archived,
			Folder:        thread.Folder,
			ThreadURL:     fmt.Sprintf("/missatges/fil/%d%s", thread.ThreadID, folderQuery),
			IsActive:      activeThreadID > 0 && thread.ThreadID == activeThreadID,
		})
	}
	return threadViews
}

func (a *App) MessagesInbox(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireMessagesView(w, r)
	if !ok || user == nil {
		return
	}
	lang := resolveUserLang(r, user)

	archived := parseFormBool(r.URL.Query().Get("archived"))
	folderParam, folderFilter := parseDMFolderFilter(r.URL.Query().Get("folder"))
	folderName := folderParam
	if folderParam == dmFolderInboxToken {
		folderName = ""
	}
	deleted := false
	filter := db.DMThreadListFilter{
		Archived: &archived,
		Deleted:  &deleted,
		Folder:   folderFilter,
		Limit:    dmThreadListLimit,
	}
	threadViews := a.buildDMThreadViews(user.ID, filter, 0, folderParam)
	folders := []string{}
	if list, err := a.DB.ListDMThreadFolders(user.ID); err == nil {
		folders = list
	} else {
		Errorf("Error carregant carpetes missatges: %v", err)
	}

	var newMessageUser *db.User
	newMessageLabel := ""
	newMessageBlocked := ""
	toID := parseFormInt(r.URL.Query().Get("to"))
	if toID > 0 && toID != user.ID {
		if u, err := a.DB.GetUserByID(toID); err == nil && u != nil {
			newMessageUser = u
			newMessageLabel = formatDMUserLabel(u)
			if ok, reasonKey := a.canUserSendDM(user.ID, u.ID); !ok {
				newMessageBlocked = T(lang, reasonKey)
			}
		}
	}
	if toID == user.ID {
		newMessageBlocked = T(lang, "messages.contact.disabled.self")
	}
	inboxBaseURL := "/missatges"
	folderPrefix := "?"
	if archived {
		inboxBaseURL = "/missatges?archived=1"
		folderPrefix = "&"
	}

	data := map[string]interface{}{
		"Threads":             threadViews,
		"ArchivedView":        archived,
		"Folders":             folders,
		"CurrentFolder":       folderParam,
		"CurrentFolderName":   folderName,
		"FolderInboxToken":    dmFolderInboxToken,
		"InboxBaseURL":        inboxBaseURL,
		"InboxFolderPrefix":   folderPrefix,
		"NewMessageUser":      newMessageUser,
		"NewMessageUserLabel": newMessageLabel,
		"NewMessageBlocked":   newMessageBlocked,
		"MessageMaxLen":       dmMessageMaxLen,
	}
	RenderPrivateTemplateLang(w, r, "messages-inbox.html", lang, data)
}

func (a *App) MessagesThread(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireMessagesView(w, r)
	if !ok || user == nil {
		return
	}
	lang := resolveUserLang(r, user)

	threadID := extractID(r.URL.Path)
	thread, otherID, state, err := a.loadDMThreadState(user.ID, threadID)
	if err != nil || thread == nil {
		http.NotFound(w, r)
		return
	}
	otherUser, _ := a.DB.GetUserByID(otherID)
	otherLabel := formatDMUserLabel(otherUser)
	folderParam, folderFilter := parseDMFolderFilter(r.URL.Query().Get("folder"))
	folderName := folderParam
	if folderParam == dmFolderInboxToken {
		folderName = ""
	}
	deleted := false
	threadFilter := db.DMThreadListFilter{
		Deleted: &deleted,
		Folder:  folderFilter,
		Limit:   dmThreadListLimit,
	}
	threadViews := a.buildDMThreadViews(user.ID, threadFilter, threadID, folderParam)
	folders := []string{}
	if list, err := a.DB.ListDMThreadFolders(user.ID); err == nil {
		folders = list
	} else {
		Errorf("Error carregant carpetes missatges: %v", err)
	}

	msgs, err := a.DB.ListDMMessages(threadID, dmThreadMsgLimit, 0)
	if err != nil {
		Errorf("Error carregant missatges thread %d: %v", threadID, err)
		msgs = []db.DMMessage{}
	}
	messageViews := []dmMessageView{}
	refreshUnread := false
	if len(msgs) > 0 {
		lastMsgID := msgs[0].ID
		if lastMsgID > 0 {
			_ = a.DB.MarkDMThreadRead(threadID, user.ID, lastMsgID)
			if state != nil && state.Unread {
				refreshUnread = true
			}
		}
		for i := len(msgs) - 1; i >= 0; i-- {
			msg := msgs[i]
			isOwn := msg.SenderID == user.ID
			senderLabel := otherLabel
			senderID := msg.SenderID
			if isOwn {
				senderLabel = T(lang, "messages.you")
				senderID = user.ID
			}
			createdAt := ""
			if msg.CreatedAt.Valid {
				createdAt = msg.CreatedAt.Time.Format("02/01/2006 15:04")
			}
			messageViews = append(messageViews, dmMessageView{
				Body:      strings.TrimSpace(msg.Body),
				Sender:    senderLabel,
				SenderID:  senderID,
				CreatedAt: createdAt,
				IsOwn:     isOwn,
			})
		}
	}

	canSend := true
	sendBlocked := ""
	if ok, reasonKey := a.canUserSendDM(user.ID, otherID); !ok {
		canSend = false
		sendBlocked = T(lang, reasonKey)
	}
	viewerBlocked := false
	if blocked, _ := a.DB.IsUserBlocked(user.ID, otherID); blocked {
		viewerBlocked = true
	}

	data := map[string]interface{}{
		"ThreadID":      threadID,
		"OtherUser":     otherUser,
		"OtherLabel":    otherLabel,
		"Messages":      messageViews,
		"Threads":       threadViews,
		"Folders":       folders,
		"CurrentFolder": folderParam,
		"CurrentFolderName": folderName,
		"FolderInboxToken": dmFolderInboxToken,
		"CanSend":       canSend,
		"SendBlocked":   sendBlocked,
		"ViewerBlocked": viewerBlocked,
		"Archived":      state.Archived,
		"MessageMaxLen": dmMessageMaxLen,
	}
	if refreshUnread {
		if count, err := a.DB.CountDMUnread(user.ID); err == nil {
			data["UnreadMessagesCount"] = count
		} else {
			Errorf("Error comptant missatges pendents per usuari %d: %v", user.ID, err)
		}
	}
	RenderPrivateTemplateLang(w, r, "messages-thread.html", lang, data)
}

func (a *App) MessagesSetFolder(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireMessagesView(w, r)
	if !ok || user == nil {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	threadID := extractID(r.URL.Path)
	if threadID <= 0 {
		http.NotFound(w, r)
		return
	}
	folder := strings.TrimSpace(r.FormValue("folder"))
	folder = strings.ReplaceAll(folder, "\n", " ")
	folder = strings.ReplaceAll(folder, "\r", " ")
	folder = strings.Join(strings.Fields(folder), " ")
	if utf8.RuneCountInString(folder) > 60 {
		folder = truncateRunes(folder, 60)
	}
	if folder == dmFolderInboxToken {
		folder = ""
	}
	if err := a.DB.SetDMThreadFolder(threadID, user.ID, folder); err != nil {
		Errorf("Error assignant carpeta thread %d: %v", threadID, err)
		http.Error(w, "No s'ha pogut desar la carpeta", http.StatusInternalServerError)
		return
	}
	redirect := strings.TrimSpace(r.FormValue("return"))
	if redirect == "" {
		redirect = fmt.Sprintf("/missatges/fil/%d", threadID)
	}
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}

func (a *App) MessagesNew(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireMessagesView(w, r)
	if !ok || user == nil {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !allowRouteLimit(r, "/missatges/enviar", dmMessageRateLimit, dmMessageRateBurst) {
		http.Error(w, "Massa peticions", http.StatusTooManyRequests)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	recipientID := parseFormInt(r.FormValue("recipient_id"))
	if recipientID <= 0 {
		http.Error(w, "recipient invalid", http.StatusBadRequest)
		return
	}
	recipient, err := a.DB.GetUserByID(recipientID)
	if err != nil || recipient == nil {
		http.NotFound(w, r)
		return
	}
	if ok, _ := a.canUserSendDM(user.ID, recipientID); !ok {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	body, err := sanitizeDMBody(r.FormValue("body"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	thread, err := a.DB.GetOrCreateDMThread(user.ID, recipientID)
	if err != nil || thread == nil {
		http.Error(w, "No s'ha pogut crear la conversa", http.StatusInternalServerError)
		return
	}
	msgID, err := a.DB.CreateDMMessage(thread.ID, user.ID, body)
	if err != nil {
		http.Error(w, "No s'ha pogut enviar el missatge", http.StatusInternalServerError)
		return
	}
	_ = a.DB.UpdateDMThreadLastMessage(thread.ID, msgID, time.Now())
	_ = a.DB.MarkDMThreadRead(thread.ID, user.ID, msgID)
	a.maybeSendDMNotification(recipient, user, thread.ID, body)
	http.Redirect(w, r, fmt.Sprintf("/missatges/fil/%d", thread.ID), http.StatusSeeOther)
}

func (a *App) MessagesSend(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireMessagesView(w, r)
	if !ok || user == nil {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !allowRouteLimit(r, "/missatges/enviar", dmMessageRateLimit, dmMessageRateBurst) {
		http.Error(w, "Massa peticions", http.StatusTooManyRequests)
		return
	}
	threadID := extractID(r.URL.Path)
	thread, otherID, _, err := a.loadDMThreadState(user.ID, threadID)
	if err != nil || thread == nil {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	if ok, _ := a.canUserSendDM(user.ID, otherID); !ok {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	body, err := sanitizeDMBody(r.FormValue("body"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	msgID, err := a.DB.CreateDMMessage(threadID, user.ID, body)
	if err != nil {
		http.Error(w, "No s'ha pogut enviar el missatge", http.StatusInternalServerError)
		return
	}
	_ = a.DB.UpdateDMThreadLastMessage(threadID, msgID, time.Now())
	_ = a.DB.MarkDMThreadRead(threadID, user.ID, msgID)
	recipient, _ := a.DB.GetUserByID(otherID)
	a.maybeSendDMNotification(recipient, user, threadID, body)
	http.Redirect(w, r, fmt.Sprintf("/missatges/fil/%d", threadID), http.StatusSeeOther)
}

func (a *App) MessagesArchive(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireMessagesView(w, r)
	if !ok || user == nil {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	threadID := extractID(r.URL.Path)
	thread, _, _, err := a.loadDMThreadState(user.ID, threadID)
	if err != nil || thread == nil {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	archived := true
	if raw := strings.TrimSpace(r.FormValue("archived")); raw != "" {
		archived = parseFormBool(raw)
	}
	if err := a.DB.SetDMThreadArchived(threadID, user.ID, archived); err != nil {
		http.Error(w, "No s'ha pogut arxivar", http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/missatges/fil/%d", threadID), http.StatusSeeOther)
}

func (a *App) MessagesDelete(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireMessagesView(w, r)
	if !ok || user == nil {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	threadID := extractID(r.URL.Path)
	thread, _, _, err := a.loadDMThreadState(user.ID, threadID)
	if err != nil || thread == nil {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	if err := a.DB.SoftDeleteDMThread(threadID, user.ID); err != nil {
		http.Error(w, "No s'ha pogut esborrar", http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/missatges", http.StatusSeeOther)
}

func (a *App) MessagesBlock(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireMessagesView(w, r)
	if !ok || user == nil {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	threadID := extractID(r.URL.Path)
	thread, otherID, _, err := a.loadDMThreadState(user.ID, threadID)
	if err != nil || thread == nil {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	shouldBlock := parseFormBool(r.FormValue("blocked"))
	if shouldBlock {
		if err := a.DB.AddUserBlock(user.ID, otherID); err != nil {
			http.Error(w, "No s'ha pogut bloquejar", http.StatusInternalServerError)
			return
		}
		_ = a.DB.SetDMThreadArchived(threadID, user.ID, true)
	} else {
		if err := a.DB.RemoveUserBlock(user.ID, otherID); err != nil {
			http.Error(w, "No s'ha pogut desbloquejar", http.StatusInternalServerError)
			return
		}
	}
	http.Redirect(w, r, fmt.Sprintf("/missatges/fil/%d", threadID), http.StatusSeeOther)
}

func (a *App) UserBlock(w http.ResponseWriter, r *http.Request) {
	user, _ := a.VerificarSessio(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	targetID := parseFormInt(r.FormValue("user_id"))
	if targetID <= 0 || targetID == user.ID {
		http.Error(w, "invalid user", http.StatusBadRequest)
		return
	}
	shouldBlock := parseFormBool(r.FormValue("blocked"))
	if shouldBlock {
		if err := a.DB.AddUserBlock(user.ID, targetID); err != nil {
			http.Error(w, "No s'ha pogut bloquejar", http.StatusInternalServerError)
			return
		}
		if thread, err := a.DB.GetDMThreadByUsers(user.ID, targetID); err == nil && thread != nil {
			_ = a.DB.SetDMThreadArchived(thread.ID, user.ID, true)
		}
	} else {
		if err := a.DB.RemoveUserBlock(user.ID, targetID); err != nil {
			http.Error(w, "No s'ha pogut desbloquejar", http.StatusInternalServerError)
			return
		}
	}
	returnTo := safeReturnTo(r.FormValue("return_to"), fmt.Sprintf("/u/%d", targetID))
	http.Redirect(w, r, returnTo, http.StatusSeeOther)
}

func (a *App) loadDMThreadState(userID, threadID int) (*db.DMThread, int, *db.DMThreadListItem, error) {
	if userID <= 0 || threadID <= 0 {
		return nil, 0, nil, fmt.Errorf("invalid ids")
	}
	thread, err := a.DB.GetDMThreadByID(threadID)
	if err != nil || thread == nil {
		return nil, 0, nil, fmt.Errorf("thread not found")
	}
	otherID := 0
	if thread.UserLowID == userID {
		otherID = thread.UserHighID
	} else if thread.UserHighID == userID {
		otherID = thread.UserLowID
	} else {
		return nil, 0, nil, fmt.Errorf("not participant")
	}
	deleted := false
	filter := db.DMThreadListFilter{
		ThreadID: threadID,
		Deleted:  &deleted,
		Limit:    1,
	}
	items, err := a.DB.ListDMThreadsForUser(userID, filter)
	if err != nil || len(items) == 0 {
		return nil, 0, nil, fmt.Errorf("thread state not found")
	}
	return thread, otherID, &items[0], nil
}

func (a *App) canUserSendDM(senderID, recipientID int) (bool, string) {
	if senderID <= 0 || recipientID <= 0 {
		return false, "messages.contact.disabled.viewer"
	}
	if senderID == recipientID {
		return false, "messages.contact.disabled.self"
	}
	if blocked, _ := a.DB.IsUserBlocked(senderID, recipientID); blocked {
		return false, "messages.contact.blocked.viewer"
	}
	if blocked, _ := a.DB.IsUserBlocked(recipientID, senderID); blocked {
		return false, "messages.contact.blocked.target"
	}
	if privacy, _ := a.DB.GetPrivacySettings(senderID); privacy != nil && !privacy.AllowContact {
		return false, "messages.contact.disabled.viewer"
	}
	if privacy, _ := a.DB.GetPrivacySettings(recipientID); privacy != nil && !privacy.AllowContact {
		return false, "messages.contact.disabled.target"
	}
	return true, ""
}

func (a *App) maybeSendDMNotification(recipient *db.User, sender *db.User, threadID int, body string) {
	if recipient == nil || sender == nil {
		return
	}
	if !a.Mail.Enabled {
		return
	}
	if strings.TrimSpace(recipient.Email) == "" {
		return
	}
	privacy, _ := a.DB.GetPrivacySettings(recipient.ID)
	if privacy != nil {
		if !privacy.NotifyEmail || !privacy.AllowContact {
			return
		}
	}
	lang := resolveUserLang(nil, recipient)
	senderLabel := formatDMUserLabel(sender)
	threadURL := fmt.Sprintf("http://localhost:8080/missatges/fil/%d", threadID)
	snippet := buildDMEmailSnippet(body)
	subject := fmt.Sprintf(T(lang, "email.dm.subject"), senderLabel)
	bodyText := ""
	if snippet != "" {
		bodyText = fmt.Sprintf(T(lang, "email.dm.body.snippet"), senderLabel, threadURL, snippet)
	} else {
		bodyText = fmt.Sprintf(T(lang, "email.dm.body"), senderLabel, threadURL)
	}
	if err := a.Mail.Send(recipient.Email, subject, bodyText); err != nil {
		Errorf("No s'ha pogut enviar el correu de missatge a %s: %v", recipient.Email, err)
	}
}

func sanitizeDMBody(body string) (string, error) {
	trimmed := strings.TrimSpace(body)
	if trimmed == "" {
		return "", fmt.Errorf("missatge buit")
	}
	if utf8.RuneCountInString(trimmed) > dmMessageMaxLen {
		return "", fmt.Errorf("missatge massa llarg")
	}
	return trimmed, nil
}

func formatDMUserLabel(u *db.User) string {
	if u == nil {
		return "—"
	}
	username := strings.TrimSpace(u.Usuari)
	if username != "" {
		return "@" + username
	}
	name := strings.TrimSpace(strings.TrimSpace(u.Name) + " " + strings.TrimSpace(u.Surname))
	if name != "" {
		return name
	}
	return fmt.Sprintf("Usuari %d", u.ID)
}

func truncateRunes(val string, max int) string {
	if max <= 0 {
		return ""
	}
	if utf8.RuneCountInString(val) <= max {
		return val
	}
	runes := []rune(val)
	if len(runes) <= max {
		return val
	}
	return string(runes[:max])
}

func buildDMEmailSnippet(body string) string {
	snippet := strings.TrimSpace(body)
	if snippet == "" {
		return ""
	}
	snippet = strings.ReplaceAll(snippet, "\n", " ")
	snippet = strings.ReplaceAll(snippet, "\r", " ")
	snippet = strings.Join(strings.Fields(snippet), " ")
	if utf8.RuneCountInString(snippet) > dmEmailSnippetMax {
		trunc := dmEmailSnippetMax
		if trunc > 1 {
			trunc--
		}
		snippet = truncateRunes(snippet, trunc) + "…"
	}
	return snippet
}
