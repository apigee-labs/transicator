package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/30x/transicator/common"
	"github.com/30x/transicator/replication"
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
	if s.isMarkedDown() {
		sendError(resp, req, http.StatusServiceUnavailable, "Marked down")
		return
	}

	enc := httputil.NegotiateContentEncoding(req, []string{jsonContent})
	if enc == "" {
		resp.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}

	q := req.URL.Query()

	limit, err := getIntParam(q, "limit", defaultLimit)
	if err != nil {
		sendError(resp, req, http.StatusBadRequest, "Invalid limit parameter")
		return
	}

	block, err := getIntParam(q, "block", 0)
	if err != nil {
		sendError(resp, req, http.StatusBadRequest, "Invalid block parameter")
		return
	}

	scopes := q["scope"]
	if len(scopes) == 0 {
		// If no scope specified, replace with the empty scope
		scopes = []string{""}
	}

	var sinceSeq common.Sequence
	since := q.Get("since")
	if since == "" {
		sinceSeq = common.Sequence{}
	} else {
		sinceSeq, err = common.ParseSequence(since)
		if err != nil {
			sendError(resp, req, http.StatusBadRequest, fmt.Sprintf("Invalid since value %s", since))
			return
		}
	}

	var snapshotFilter func([]byte) bool
	snapStr := q.Get("snapshot")
	if snapStr != "" {
		snapshot, err := replication.MakeSnapshot(snapStr)
		if err != nil {
			sendError(resp, req, http.StatusBadRequest, fmt.Sprintf("Invalid snapshot %s", snapStr))
			return
		}
		snapshotFilter = makeSnapshotFilter(snapshot)
	}

	// Need to advance past a single "since" value
	sinceSeq.Index++

	log.Debugf("Receiving changes: scopes = %v since = %s limit = %d block = %d",
		scopes, sinceSeq, limit, block)
	entries, err := s.db.GetMultiEntries(scopes, int64(sinceSeq.LSN),
		int32(sinceSeq.Index), limit, snapshotFilter)
	if err != nil {
		sendError(resp, req, http.StatusInternalServerError, err.Error())
		return
	}
	log.Debugf("Received %d changes", len(entries))

	if len(entries) == 0 && block > 0 {
		log.Debugf("Blocking for up to %d seconds", block)
		newIndex := s.tracker.timedWait(sinceSeq, time.Duration(block)*time.Second, scopes)
		if newIndex.Compare(sinceSeq) > 0 {
			entries, err = s.db.GetMultiEntries(scopes, int64(sinceSeq.LSN),
				int32(sinceSeq.Index), limit, snapshotFilter)
			if err != nil {
				sendError(resp, req, http.StatusInternalServerError, err.Error())
				return
			}
		}
		log.Debugf("Received %d changes after blocking", len(entries))
	}

	changeList := common.ChangeList{}

	for _, e := range entries {
		change, _ := common.UnmarshalChange(e)
		// Database doesn't have value of "Sequence" in it
		change.Sequence = change.GetSequence().String()
		changeList.Changes = append(changeList.Changes, *change)
	}

	resp.Header().Set("Content-Type", jsonContent)
	resp.Write(changeList.Marshal())
}

func makeSnapshotFilter(ss *replication.Snapshot) func([]byte) bool {
	return func(buf []byte) bool {
		change, err := common.UnmarshalChange(buf)
		if err == nil {
			return !ss.Contains(uint32(change.TransactionID))
		}
		return false
	}
}
