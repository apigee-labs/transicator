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
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/30x/goscaffold"
	log "github.com/Sirupsen/logrus"
	"github.com/apigee-labs/transicator/common"
	"github.com/apigee-labs/transicator/replication"
	"github.com/julienschmidt/httprouter"
)

const (
	defaultLimit = 100
	changeSelectorValidChars = "^[0-9a-z_-]+$"
)

var emptySequence = common.Sequence{}
var lowestPossibleSequence = common.MakeSequence(0, 1)
var reChangeSelector = regexp.MustCompile(changeSelectorValidChars)

func (s *server) initChangesAPI(prefix string, router *httprouter.Router) {
	router.HandlerFunc("GET", prefix+"/changes", s.handleGetChanges)
}

func (s *server) handleGetChanges(resp http.ResponseWriter, req *http.Request) {
	enc := goscaffold.SelectMediaType(req, []string{jsonContent, protoContent})
	if enc == "" {
		sendAPIError(unsupportedFormat, "", resp, req)
		return
	}

	q := req.URL.Query()

	limit, err := getIntParam(q, "limit", defaultLimit)
	if err != nil {
		sendAPIError(invalidParameter, "limit", resp, req)
		return
	}

	block, err := getIntParam(q, "block", 0)
	if err != nil {
		sendAPIError(invalidParameter, "block", resp, req)
		return
	}

	scopes, err := getCheckChangeSelectorParams(req)
	if err != nil {
		sendAPIError(invalidParameter, err.Error(), resp, req)
		return
	}
	if len(scopes) == 0 {
		// If no scope specified, replace with the empty scope
		scopes = []string{""}
	}

	var sinceSeq common.Sequence
	since := q.Get("since")
	if since == "" {
		sinceSeq = emptySequence
	} else {
		sinceSeq, err = common.ParseSequence(since)
		if err != nil {
			sendAPIError(invalidParameter, "since", resp, req)
			return
		}
	}

	var snapshotFilter func([]byte) bool
	snapStr := q.Get("snapshot")
	if snapStr != "" {
		var snapshot *replication.Snapshot
		snapshot, err = replication.MakeSnapshot(snapStr)
		if err != nil {
			sendAPIError(invalidParameter, "snapshot", resp, req)
			return
		}
		snapshotFilter = makeSnapshotFilter(snapshot)
	}

	// Need to advance past a single "since" value
	sinceSeq.Index++

	firstSeq, lastSeq, entries, success :=
		s.receiveChanges(scopes, sinceSeq, limit, snapshotFilter, resp, req)
	if !success {
		return
	}

	if len(entries) == 0 && block > 0 {
		// Query -- which was consistent at the "snapshot" level -- didn't
		// return anything. Wait until something is put in the database and try again.
		waitSeq := lastSeq
		waitSeq.Index++

		log.Debugf("Blocking at %s for up to %d seconds", waitSeq, block)
		newIndex := s.tracker.timedWait(waitSeq, time.Duration(block)*time.Second, scopes)

		if newIndex.Compare(sinceSeq) > 0 {
			firstSeq, lastSeq, entries, success =
				s.receiveChanges(scopes, sinceSeq, limit, snapshotFilter, resp, req)
			if !success {
				return
			}
		}
	}

	changeList := common.ChangeList{
		FirstSequence: firstSeq.String(),
		LastSequence:  lastSeq.String(),
	}

	for _, e := range entries {
		change, err := decodeChangeProto(e)
		if err != nil {
			sendAPIError(serverError,
				fmt.Sprintf("Invalid data in database: %s", err), resp, req)
		}
		// Database doesn't have value of "Sequence" in it
		change.Sequence = change.GetSequence().String()
		changeList.Changes = append(changeList.Changes, *change)
	}

	// Important to return an intermediate sequence if we ran up against the limit
	if len(entries) == limit && limit > 0 {
		changeList.LastSequence = changeList.Changes[len(entries)-1].Sequence
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

func (s *server) receiveChanges(
	scopes []string, sinceSeq common.Sequence,
	limit int, filter func([]byte) bool,
	resp http.ResponseWriter, req *http.Request) (firstSeq, lastSeq common.Sequence, entries [][]byte, success bool) {

	log.Debugf("Receiving changes: scopes = %v since = %s limit = %d",
		scopes, sinceSeq, limit)

	var err error
	entries, firstSeq, lastSeq, err = s.db.Scan(
		scopes, sinceSeq.LSN, sinceSeq.Index, limit, filter)

	if err != nil {
		sendAPIError(serverError, err.Error(), resp, req)
		return
	}
	if sinceSeq.Compare(firstSeq) < 0 && sinceSeq.Compare(lowestPossibleSequence) > 0 {
		// "since" parameter specified and too old. Need to return an error.
		log.Debugf("since value of %s is too old compared to %s\n",
			sinceSeq, firstSeq)
		sendAPIError(snapshotOld, "", resp, req)
		return
	}

	log.Debugf("Received %d changes", len(entries))
	success = true
	return
}

/*
getChangeSelectorParams combines all 'scope' query
params into one slice after checking for valid characters.
 */
func getCheckChangeSelectorParams(r *http.Request) ([]string, error) {
	scopes := r.URL.Query()["scope"]
	for _, s := range scopes {
		if !reChangeSelector.MatchString(s) {
			return nil, errors.New("Invalid char in scope param")
		}
	}
	return scopes, nil
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
