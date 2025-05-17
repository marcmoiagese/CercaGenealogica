package handlers

import (
	"net/http"
)

func StaticHandler() http.Handler {
	return http.StripPrefix("/static/", http.FileServer(http.Dir("web/static")))
}
