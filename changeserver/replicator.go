package main

import (
	"github.com/30x/transicator/replication"
	log "github.com/Sirupsen/logrus"
)

func (s *server) runReplication() {
	for {
		select {
		case change := <-s.repl.Changes():
			s.handleChange(change)
		case stopped := <-s.stopChan:
			s.repl.Stop()
			stopped <- true
			return
		}
	}
}

func (s *server) handleChange(c replication.Change) {
	e, err := c.Decode()
	if err != nil {
		log.Errorf("Received an invalid change: %s", err)
	} else {
		scope := getScope(e)
		log.Debugf("Received change %d for scope %s", e.CommitSequence, scope)
		err = s.db.PutEntry(scope, e.CommitSequence, e.Index, []byte(c.Data))
		s.tracker.update(e.CommitSequence, scope)
	}
}

func getScope(e *replication.EncodedChange) string {
	if e.New != nil {
		if e.New["_scope"] == nil {
			return ""
		}
		return e.New["_scope"].(string)
	}
	if e.Old != nil {
		if e.Old["_scope"] == nil {
			return ""
		}
		return e.Old["_scope"].(string)
	}
	return ""
}
