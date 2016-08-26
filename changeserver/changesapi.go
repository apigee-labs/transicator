package main

import (
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/gddo/httputil"
	"github.com/julienschmidt/httprouter"
)

const (
	defaultLimit = 100
)

func (s *server) initChangesAPI(prefix string, router *httprouter.Router) {
	router.HandlerFunc("GET", prefix+"/changes", s.handleGetChanges)
}

func (s *server) handleGetChanges(resp http.ResponseWriter, req *http.Request) {
	enc := httputil.NegotiateContentEncoding(req, []string{jsonContent})
	if enc == "" {
		resp.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}

	q := req.URL.Query()

	since, err := getInt64Param(q, "since", 0)
	if err != nil {
		sendError(resp, req, http.StatusBadRequest, "Invalid since parameter")
		return
	}

	index, err := getInt32Param(q, "index", 0)
	if err != nil {
		sendError(resp, req, http.StatusBadRequest, "Invalid index parameter")
		return
	}

	limit, err := getUintParam(q, "limit", defaultLimit)
	if err != nil {
		sendError(resp, req, http.StatusBadRequest, "Invalid limit parameter")
		return
	}

	tag := q.Get("tag")

	log.Debugf("Receiving changes: tag = %s since = %d index = %d limit = %d",
		tag, since, index, limit)
	entries, err := s.db.GetEntries(tag, since+1, index, limit)
	if err != nil {
		sendError(resp, req, http.StatusInternalServerError, err.Error())
		return
	}
	log.Debugf("Received %d changes", len(entries))

	bod := "["
	for i, e := range entries {
		if i > 0 {
			bod += ","
		}
		bod += string(e)
	}
	bod += "]"

	resp.Header().Set("Content-Type", jsonContent)
	resp.Write([]byte(bod))
}
