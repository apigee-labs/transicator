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
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

const (
	perfTick    = 500 * time.Millisecond
	contentSize = 100000
)

type sender struct {
	ackChan  chan bool
	stopChan chan bool
}

func startSender(selector string, db *sql.DB, windowSize, batchSize int, done *sync.WaitGroup) *sender {
	s := &sender{
		ackChan:  make(chan bool, 1000),
		stopChan: make(chan bool, 1),
	}
	go s.run(selector, db, windowSize, batchSize, done)
	return s
}

func (s *sender) stop() {
	s.stopChan <- true
}

func (s *sender) acknowledge() {
	s.ackChan <- true
}

func (s *sender) run(selector string, db *sql.DB, windowSize, batchSize int, done *sync.WaitGroup) {
	var is, us, ds *sql.Stmt
	var err error

	defer done.Done()

	is, err = db.Prepare(`
  insert into stress_table
  (id, grp, sequence, content, last, _change_selector)
  values ($1, $2, $3, $4, $5, $6)
  `)
	if err == nil {
		ds, err = db.Prepare(`
    delete from stress_table where id = $1
    `)
	}
	if err == nil {
		us, err = db.Prepare(`
    update stress_table set content = $1 where id = $2
    `)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing SQL: %s\n", err)
		return
	}
	defer is.Close()
	defer ds.Close()
	defer us.Close()

	openWindow := 0
	ticks := time.NewTimer(perfTick)

	for {
		if openWindow < windowSize {
			openWindow++
			toSend := rand.Intn(batchSize)
			groupNum := rand.Int31()
			fmt.Printf("Window = %d: sending batch of size %d\n", openWindow, toSend)

			var ids []int64
			idMap := make(map[int64]bool)
			for i := 0; i < toSend; i++ {
				buf := make([]byte, rand.Intn(contentSize))
				rand.Read(buf)
				id := rand.Int63()
				ids = append(ids, id)
				idMap[id] = true
				_, err = is.Exec(rand.Int63(), groupNum, i, buf, false, selector)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Fatal error on SQL insert: %s\n", err)
					return
				}
			}

			for i := 0; i < (toSend / 4); i++ {
				di := ids[rand.Intn(len(ids))]
				if idMap[di] {
					_, err = ds.Exec(di)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Fatal error on SQL delete: %s\n", err)
						return
					}
					delete(idMap, di)
				}
			}

			for i := 0; i < (toSend / 4); i++ {
				ui := ids[rand.Intn(len(ids))]
				if idMap[ui] {
					buf := make([]byte, rand.Intn(contentSize))
					rand.Read(buf)
					_, err = us.Exec(buf, ui)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Fatal error on SQL update: %s\n", err)
						return
					}
				}
			}

			_, err = is.Exec(rand.Int63(), groupNum, toSend, nil, true, selector)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Fatal error on last SQL insert: %s\n", err)
				return
			}
			ticks.Reset(0)
		} else {
			ticks.Reset(perfTick)
		}

		select {
		case <-ticks.C:
			continue
		case <-s.ackChan:
			openWindow--
			fmt.Printf("Ack. Window = %d\n", openWindow)
		case <-s.stopChan:
			ticks.Stop()
			fmt.Printf("Send thread stopping.\n")
			return
		}
	}
}
