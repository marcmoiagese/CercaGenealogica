package core

import (
	"fmt"
	"strings"
)

type DBOperationLog struct {
	Component string
	Op        string
	Object    string
	ObjectID  int
	UserID    int
	Engine    string
	Err       error
}

func (a *App) dbEngineName() string {
	if a == nil || a.DB == nil {
		return "unknown"
	}
	if engine := strings.TrimSpace(a.DB.Engine()); engine != "" {
		return engine
	}
	return "unknown"
}

func LogDBOperationError(entry DBOperationLog) {
	if entry.Err == nil {
		return
	}
	component := strings.TrimSpace(entry.Component)
	if component == "" {
		component = "unknown"
	}
	op := strings.TrimSpace(entry.Op)
	if op == "" {
		op = "unknown"
	}
	object := strings.TrimSpace(entry.Object)
	if object == "" {
		object = "-"
	}
	engine := strings.TrimSpace(entry.Engine)
	if engine == "" {
		engine = "unknown"
	}
	Errorf("db operation failed component=%s op=%s object=%s object_id=%d user_id=%d engine=%s err=%q",
		component, op, object, entry.ObjectID, entry.UserID, engine, fmt.Sprint(entry.Err))
}

func (a *App) logDBOperationError(entry DBOperationLog) {
	if entry.Engine == "" {
		entry.Engine = a.dbEngineName()
	}
	LogDBOperationError(entry)
}
