package main

import (
	"net/http"
	"runtime"
	"strconv"

	"github.com/golang/gddo/httputil"
	"github.com/julienschmidt/httprouter"
)

func (s *server) initDiagAPI(prefix string, router *httprouter.Router) {
	router.HandlerFunc("GET", prefix+"/health", s.handleGetHealth)
	router.HandlerFunc("PUT", prefix+"/health", s.handleSetHealth)
	router.HandlerFunc("POST", prefix+"/health", s.handleSetHealth)
	router.HandlerFunc("GET", prefix+"/diagnostics/stack", s.handleGetStack)
}

func (s *server) handleGetHealth(resp http.ResponseWriter, req *http.Request) {
	rawReason := s.markdownReason.Load()

	if rawReason == nil {
		resp.Write([]byte("OK"))
	} else {
		markdownReason := rawReason.(*string)
		if *markdownReason == "" {
			resp.Write([]byte("OK"))
		} else {
			resp.WriteHeader(http.StatusServiceUnavailable)
			resp.Write([]byte(*markdownReason))
		}
	}
}

func (s *server) handleSetHealth(resp http.ResponseWriter, req *http.Request) {
	enc := httputil.NegotiateContentEncoding(req, []string{formContent})
	if enc == "" {
		resp.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}

	err := req.ParseForm()
	if err != nil {
		sendError(resp, req, http.StatusBadRequest, err.Error())
		return
	}

	upStr := req.Form.Get("up")
	if upStr == "" {
		sendError(resp, req, http.StatusBadRequest, "\"up\" parameter must be set")
	}
	up, err := strconv.ParseBool(upStr)
	if err != nil {
		up = false
	}

	var reason string
	if up {
		reason = ""
	} else {
		reason = req.Form.Get("reason")
		if reason == "" {
			reason = "Marked down"
		}
	}

	s.markdownReason.Store(&reason)
}

func (s *server) handleGetStack(
	resp http.ResponseWriter, req *http.Request) {
	stackBufLen := 64
	for {
		stackBuf := make([]byte, stackBufLen)
		stackLen := runtime.Stack(stackBuf, true)
		if stackLen == len(stackBuf) {
			// Must be truncated
			stackBufLen *= 2
		} else {
			resp.Header().Set("Content-Type", textContent)
			resp.Write(stackBuf[:stackLen])
			return
		}
	}
}
