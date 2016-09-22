package main

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"

	"github.com/30x/transicator/common"
	"github.com/30x/transicator/replication"
	"github.com/30x/transicator/storage"
	"github.com/golang/gddo/httputil"
	"github.com/julienschmidt/httprouter"
)

const (
	jsonContent     = "application/json"
	formContent     = "application/www-form-urlencoded"
	textContent     = "text/plain"
	lastSequenceKey = "_ls"
	scopeField      = "_apid_scope"
)

type server struct {
	db             *storage.DB
	repl           *replication.Replicator
	tracker        *changeTracker
	markdownReason atomic.Value
	markedDown     int32
	stopChan       chan chan<- bool
}

type errMsg struct {
	Error string `json:"error"`
}

func startChangeServer(mux *http.ServeMux, dbDir, pgURL, pgSlot, urlPrefix string) (*server, error) {
	success := false

	db, err := storage.OpenDB(dbDir, defaultCacheSize)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			db.Close()
		}
	}()

	fcBuf, err := db.GetMetadata(lastSequenceKey)
	if err != nil {
		return nil, err
	}

	var firstChange common.Sequence
	if fcBuf == nil {
		firstChange = common.Sequence{}
	} else {
		firstChange, err = common.ParseSequenceBytes(fcBuf)
		if err != nil {
			return nil, err
		}
	}

	repl, err := replication.Start(pgURL, pgSlot)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			repl.Stop()
		}
	}()

	success = true

	s := &server{
		db:             db,
		repl:           repl,
		tracker:        createTracker(),
		markdownReason: atomic.Value{},
		stopChan:       make(chan chan<- bool, 1),
	}

	router := httprouter.New()
	mux.Handle("/", router)

	s.initChangesAPI(urlPrefix, router)
	s.initDiagAPI(urlPrefix, router)
	go s.runReplication(firstChange)

	return s, nil
}

func (s *server) stop() {
	stopped := make(chan bool, 1)
	s.stopChan <- stopped
	<-stopped
	s.tracker.close()
	s.db.Close()
}

func (s *server) delete() error {
	return s.db.Delete()
}

func (s *server) isMarkedDown() bool {
	md := atomic.LoadInt32(&s.markedDown)
	return md != 0
}

func getIntParam(q url.Values, key string, dflt int) (int, error) {
	qs := q.Get(key)
	if qs == "" {
		return dflt, nil
	}
	v, err := strconv.ParseInt(qs, 10, 32)
	if err != nil {
		return 0, err
	}
	return int(v), nil
}

func sendError(resp http.ResponseWriter, req *http.Request, code int, msg string) {
	ct := httputil.NegotiateContentType(req, []string{jsonContent, textContent}, jsonContent)

	switch ct {
	case jsonContent:
		em := &errMsg{
			Error: msg,
		}
		eb, _ := json.Marshal(em)
		resp.Header().Set("Content-Type", jsonContent)
		resp.WriteHeader(code)
		resp.Write(eb)

	default:
		resp.Header().Set("Content-Type", textContent)
		resp.WriteHeader(code)
		resp.Write([]byte(msg))
	}
}
