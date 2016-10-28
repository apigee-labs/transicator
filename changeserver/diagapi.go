package main

import (
	"net/http"
	"runtime"

	"github.com/julienschmidt/httprouter"
)

func (s *server) initDiagAPI(prefix string, router *httprouter.Router) {
	router.HandlerFunc("GET", prefix+"/diagnostics/stack", s.handleGetStack)
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
