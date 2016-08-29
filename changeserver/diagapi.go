package main

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func (s *server) initDiagAPI(prefix string, router *httprouter.Router) {
	router.HandlerFunc("GET", prefix+"/health", s.handleGetHealth)
	router.HandlerFunc("PUT", prefix+"/health", s.handleSetHealth)
	router.HandlerFunc("POST", prefix+"/health", s.handleSetHealth)
}

func (s *server) handleGetHealth(resp http.ResponseWriter, req *http.Request) {
	rawReason := s.markdownReason.Load()

	if rawReason == nil {
		resp.Write([]byte("OK"))
	} else {
		markdownReason := rawReason.(*string)
		resp.WriteHeader(http.StatusServiceUnavailable)
		resp.Write([]byte(*markdownReason))
	}
}

func (s *server) handleSetHealth(resp http.ResponseWriter, req *http.Request) {
	// TODO based on json or form encoded, set the markdown reason.
}
