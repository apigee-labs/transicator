package main

import (
	"time"

	log "github.com/Sirupsen/logrus"
)

type cleaner struct {
	s        *server
	maxAge   time.Duration
	stopChan chan bool
}

func (s *server) startCleanup(maxAge time.Duration) {
	c := &cleaner{
		s:        s,
		maxAge:   maxAge,
		stopChan: make(chan bool, 1),
	}
	s.cleaner = c
	go c.run()
}

func (c *cleaner) stop() {
	c.stopChan <- true
}

func (c *cleaner) run() {
	tick := time.NewTicker(cleanupDelay(c.maxAge))
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			c.performCleanup()
		case <-c.stopChan:
			return
		}
	}
}

func (c *cleaner) performCleanup() {
	cleanupAge := time.Now().Add(-c.maxAge).Unix()
	log.Debugf("Cleaning up data records since before %v", cleanupAge)

	cleanupCount, err := c.s.db.PurgeEntries(func(buf []byte) bool {
		ts, err := decodeChangeTimestamp(buf)
		if err == nil && ts > 0 {
			if ts < cleanupAge {
				return true
			}
		}
		return false
	})

	if err != nil {
		log.Errorf("Error after cleaning up %d records: %s", cleanupCount, err)
	} else if cleanupCount > 0 {
		log.Infof("Purged %d old records from the database", cleanupCount)
	}
}

/*
cleanupDelay selects how often to run the cleanup task based
on the duration.
*/
func cleanupDelay(maxAge time.Duration) time.Duration {
	if maxAge <= time.Minute {
		return time.Second
	}
	if maxAge < 5*time.Minute {
		return 5 * time.Second
	}
	if maxAge < time.Hour {
		return 5 * time.Minute
	}
	return time.Hour
}
