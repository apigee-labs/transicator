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
		tag := getTag(e)
		log.Debugf("Received change %d for tag %s", e.CommitSequence, tag)
		err = s.db.PutEntry(tag, e.CommitSequence, e.Index, []byte(c.Data))
	}
}

func getTag(e *replication.EncodedChange) string {
	if e.New != nil {
		if e.New["tag"] == nil {
			return ""
		}
		return e.New["tag"].(string)
	}
	if e.Old != nil {
		if e.Old["tag"] == nil {
			return ""
		}
		return e.Old["tag"].(string)
	}
	return ""
}
