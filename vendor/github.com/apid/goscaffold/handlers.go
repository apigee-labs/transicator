// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package goscaffold

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/pprof"
)

/*
requestHandler handles all requests and stops them if we are marked down.
*/
type requestHandler struct {
	s     *HTTPScaffold
	child http.Handler
}

func (h *requestHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	startErr := h.s.tracker.start()
	if startErr == nil {
		h.child.ServeHTTP(resp, req)
		h.s.tracker.end()
	} else {
		writeUnavailable(resp, req, NotReady, startErr)
	}
}

/*
managementHandler adds support for health checks and diagnostics.
*/
type managementHandler struct {
	s     *HTTPScaffold
	mux   *http.ServeMux
	child http.Handler
}

func (h *managementHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	handler, pattern := h.mux.Handler(req)
	if pattern == "" && h.child != nil {
		// Fall through for stuff that's not a management call
		h.child.ServeHTTP(resp, req)
	} else {
		// Handler may be one of ours, or a built-in not found handler
		handler.ServeHTTP(resp, req)
	}
}

func (s *HTTPScaffold) createManagementHandler() *managementHandler {
	h := &managementHandler{
		s:   s,
		mux: http.NewServeMux(),
	}

	// Manually register paths from "pprof" package because we are
	// not using a standard HTTP handler here.
	h.mux.HandleFunc("/debug/pprof/", pprof.Index)
	h.mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	h.mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	h.mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	h.mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	if s.healthPath != "" {
		h.mux.HandleFunc(s.healthPath, s.handleHealth)
	}
	if s.readyPath != "" {
		h.mux.HandleFunc(s.readyPath, s.handleReady)
	}
	if s.markdownPath != "" {
		h.mux.HandleFunc(s.markdownPath, s.handleMarkdown)
	}
	return h
}

func (s *HTTPScaffold) callHealthCheck() (HealthStatus, error) {
	if s.healthCheck == nil {
		return OK, nil
	}
	status, err := s.healthCheck()
	if status == OK {
		return OK, nil
	}
	if err == nil {
		return status, errors.New(status.String())
	}
	return status, err
}

/*
handleHealth only fails if the user's health check function tells us.
*/
func (s *HTTPScaffold) handleHealth(resp http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	status, healthErr := s.callHealthCheck()

	if status == Failed {
		writeUnavailable(resp, req, status, healthErr)
	} else {
		resp.WriteHeader(http.StatusOK)
	}
}

/*
handleReady fails if we are marked down and also if the user's health function
tells us.
*/
func (s *HTTPScaffold) handleReady(resp http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	status, healthErr := s.callHealthCheck()
	if status == OK {
		healthErr = s.tracker.markedDown()
		if healthErr != nil {
			status = NotReady
		}
	}

	if status == OK {
		resp.WriteHeader(http.StatusOK)
	} else {
		writeUnavailable(resp, req, status, healthErr)
	}
}

/*
handleMarkdown handles a request to mark down the server.
*/
func (s *HTTPScaffold) handleMarkdown(resp http.ResponseWriter, req *http.Request) {
	if req.Method != s.markdownMethod {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	req.Body.Close()
	s.tracker.markDown()
	if s.markdownHandler != nil {
		s.markdownHandler()
	}
}

func writeUnavailable(
	resp http.ResponseWriter, req *http.Request,
	stat HealthStatus, err error) {

	mt := SelectMediaType(req, []string{"text/plain", "application/json"})

	resp.WriteHeader(http.StatusServiceUnavailable)
	switch mt {
	case "application/json":
		re := map[string]string{
			"status": stat.String(),
			"reason": err.Error(),
		}
		buf, _ := json.Marshal(&re)
		resp.Header().Set("Content-Type", mt)
		resp.Write(buf)
	default:
		resp.Header().Set("Content-Type", "text/plain")
		resp.Write([]byte(err.Error()))
	}
}
