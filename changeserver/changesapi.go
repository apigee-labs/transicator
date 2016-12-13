/*
Copyright 2016 The Transicator Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/30x/goscaffold"
	log "github.com/Sirupsen/logrus"
	"github.com/apigee-labs/transicator/common"
	"github.com/apigee-labs/transicator/replication"
	"github.com/julienschmidt/httprouter"
)

const (
	defaultLimit = 100
)

func (s *server) initChangesAPI(prefix string, router *httprouter.Router) {
	router.HandlerFunc("GET", prefix+"/changes", s.handleGetChanges)
}

func (s *server) handleGetChanges(resp http.ResponseWriter, req *http.Request) {
	enc := goscaffold.SelectMediaType(req, []string{jsonContent, protoContent})
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

	selectors := q["scope"]
	if len(selectors) == 0 {
		selectors = []string{""}
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
		var snapshot *replication.Snapshot
		snapshot, err = replication.MakeSnapshot(snapStr)
		if err != nil {
			sendError(resp, req, http.StatusBadRequest, fmt.Sprintf("Invalid snapshot %s", snapStr))
			return
		}
		snapshotFilter = makeSnapshotFilter(snapshot)
	}

	// Need to advance past a single "since" value
	sinceSeq.Index++

	log.Debugf("Receiving changes: selectors = %v since = %s limit = %d block = %d",
		selectors, sinceSeq, limit, block)
	entries, firstSeq, lastSeq, err := s.db.Scan(
		selectors, sinceSeq.LSN, sinceSeq.Index, limit, snapshotFilter)
	if err != nil {
		sendError(resp, req, http.StatusInternalServerError, err.Error())
		return
	}
	log.Debugf("Received %d changes", len(entries))

	if len(entries) == 0 && block > 0 {
		// Query -- which was consistent at the "snapshot" level -- didn't
		// return anything. Wait until something is put in the database and try again.
		waitSeq := lastSeq
		waitSeq.Index++

		log.Debugf("Blocking at %s for up to %d seconds", waitSeq, block)
		newIndex := s.tracker.timedWait(waitSeq, time.Duration(block)*time.Second, selectors)
		if newIndex.Compare(sinceSeq) > 0 {
			entries, firstSeq, lastSeq, err = s.db.Scan(
				selectors, sinceSeq.LSN, sinceSeq.Index, limit, snapshotFilter)
			if err != nil {
				sendError(resp, req, http.StatusInternalServerError, err.Error())
				return
			}
		}
		log.Debugf("Received %d changes after blocking", len(entries))
	}

	changeList := common.ChangeList{
		FirstSequence: firstSeq.String(),
		LastSequence:  lastSeq.String(),
	}

	for _, e := range entries {
		change, err := decodeChangeProto(e)
		if err != nil {
			sendError(resp, req, http.StatusInternalServerError,
				fmt.Sprintf("Invalid data in database: %s", err))
		}
		// Database doesn't have value of "Sequence" in it
		change.Sequence = change.GetSequence().String()
		changeList.Changes = append(changeList.Changes, *change)
	}

	switch enc {
	case jsonContent:
		resp.Header().Set("Content-Type", jsonContent)
		resp.Write(changeList.Marshal())
	case protoContent:
		resp.Header().Set("Content-Type", protoContent)
		resp.Write(changeList.MarshalProto())
	default:
		panic("Got to an unsupported media type")
	}
}

func makeSnapshotFilter(ss *replication.Snapshot) func([]byte) bool {
	return func(buf []byte) bool {
		txid, err := decodeChangeTXID(buf)
		if err == nil {
			return !ss.Contains(txid)
		}
		return false
	}
}
