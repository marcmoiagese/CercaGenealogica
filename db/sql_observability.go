package db

import (
	"errors"
	"fmt"
	"strings"
)

type SQLErrorContext struct {
	Engine    string
	Component string
	Op        string
	Object    string
	ObjectID  int
}

type SQLOpError struct {
	Context SQLErrorContext
	Err     error
}

func (e *SQLOpError) Error() string {
	if e == nil {
		return ""
	}
	ctx := normalizeSQLErrorContext(e.Context)
	return fmt.Sprintf("sql op failed engine=%s component=%s op=%s object=%s object_id=%d: %v",
		ctx.Engine, ctx.Component, ctx.Op, ctx.Object, ctx.ObjectID, e.Err)
}

func (e *SQLOpError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func WrapSQLError(ctx SQLErrorContext, err error) error {
	if err == nil {
		return nil
	}
	var opErr *SQLOpError
	if errors.As(err, &opErr) {
		return err
	}
	ctx = normalizeSQLErrorContext(ctx)
	logErrorf("sql op failed engine=%s component=%s op=%s object=%s object_id=%d err=%q",
		ctx.Engine, ctx.Component, ctx.Op, ctx.Object, ctx.ObjectID, fmt.Sprint(err))
	return &SQLOpError{Context: ctx, Err: err}
}

func (h sqlHelper) wrapSQLError(component, op, object string, objectID int, err error) error {
	return WrapSQLError(SQLErrorContext{
		Engine:    h.style,
		Component: component,
		Op:        op,
		Object:    object,
		ObjectID:  objectID,
	}, err)
}

func normalizeSQLErrorContext(ctx SQLErrorContext) SQLErrorContext {
	ctx.Engine = strings.TrimSpace(ctx.Engine)
	if ctx.Engine == "" {
		ctx.Engine = "unknown"
	}
	ctx.Component = strings.TrimSpace(ctx.Component)
	if ctx.Component == "" {
		ctx.Component = "db"
	}
	ctx.Op = strings.TrimSpace(ctx.Op)
	if ctx.Op == "" {
		ctx.Op = "unknown"
	}
	ctx.Object = strings.TrimSpace(ctx.Object)
	if ctx.Object == "" {
		ctx.Object = "-"
	}
	return ctx
}
