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
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apigee-labs/transicator/common"
)

const (
	pollTimeout  = 5
	failureBlock = 2 * time.Second
)

type receiver struct {
	db     *sql.DB
	stopOK int32
}

func startReceiver(selector, dbFile, ssURL, csURL string,
	sender *sender, done *sync.WaitGroup) *receiver {

	s := &receiver{}
	go s.run(selector, dbFile, ssURL, csURL, sender, done)
	return s
}

func (r *receiver) canStop() {
	atomic.StoreInt32(&r.stopOK, 1)
}

func (r *receiver) run(selector, dbFile, ssURL, csURL string,
	sender *sender, done *sync.WaitGroup) {

	db, err := getSnapshot(selector, dbFile, ssURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting snapshot: %s\n", err)
		return
	}
	fmt.Printf("Got myself a snapshot in %s\n", dbFile)
	r.db = db
	defer db.Close()

	snap, err := getSnapshotTx(db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting snapshot transaction: %s\n", err)
		return
	}

	url := fmt.Sprintf("%s/changes?scope=%s&snapshot=%s&block=%d",
		csURL, selector, snap, pollTimeout)
	r.runPoller(url, sender)

	done.Done()
}

func getSnapshot(selector, dbFile, ssURL string) (*sql.DB, error) {
	url := fmt.Sprintf("%s/snapshots?scope=%s", ssURL, selector)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/transicator+sqlite")

	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			err = fmt.Errorf("Invalid HTTP status %d", resp.StatusCode)
		} else {
			var of *os.File
			of, err = os.OpenFile(dbFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
			if err == nil {
				_, err = io.Copy(of, resp.Body)
			}
			of.Close()

			var db *sql.DB
			if err == nil {
				db, err = sql.Open("sqlite3", dbFile)
			}
			if err == nil {
				return db, nil
			}
		}
	}
	return nil, err
}

func (r *receiver) runPoller(baseURL string, sender *sender) {
	lastSequence := ""
	for {
		url := fmt.Sprintf("%s&since=%s", baseURL, lastSequence)
		//fmt.Printf("%s\n", url)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			panic(err.Error())
		}

		req.Header.Set("Accept", "application/transicator+protobuf")

		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			if resp.StatusCode == 200 {
				var body []byte
				body, err = ioutil.ReadAll(resp.Body)
				var cl *common.ChangeList

				if err == nil {
					cl, err = common.UnmarshalChangeListProto(body)
				}
				if err == nil {
					if len(cl.Changes) == 0 {
						if atomic.LoadInt32(&r.stopOK) > 0 {
							fmt.Printf("Receiver thread exiting\n")
							return
						}
					} else {
						lastSequence = r.applyChanges(cl)
					}

					sender.acknowledge()

				} else {
					fmt.Fprintf(os.Stderr, "Invalid response reading body: %s\n", err)
				}

			} else {
				fmt.Fprintf(os.Stderr, "Invalid response getting changes: %d\n",
					resp.StatusCode)
				time.Sleep(failureBlock)
			}
			resp.Body.Close()

		} else {
			fmt.Fprintf(os.Stderr, "Error getting changes: %s\n", err)
			time.Sleep(failureBlock)
		}
	}
}

func (r *receiver) applyChanges(cl *common.ChangeList) (lastSequence string) {
	for _, change := range cl.Changes {
		switch change.Operation {
		case common.Insert:
			stmt, cols := makeInsertSQL(&change)
			args := makeArgs(cols, change.NewRow)
			_, err := r.db.Exec(stmt, args...)
			if err == nil {
				lastSequence = change.Sequence
			} else {
				fmt.Fprintf(os.Stderr, "Error on database insert: %s\n", err)
			}

		case common.Update:
			fmt.Printf("Didn't do update yet\n")
		case common.Delete:
			fmt.Printf("Didn't do delete yet\n")
		}
	}
	return
}

func makeInsertSQL(c *common.Change) (sql string, cols []string) {
	tns := strings.Split(c.Table, ".")
	buf := &bytes.Buffer{}
	buf.WriteString("insert into ")
	buf.WriteString(tns[len(tns)-1])
	buf.WriteString(" (")

	first := true
	for k := range c.NewRow {
		if first {
			first = false
		} else {
			buf.WriteString(", ")
		}
		buf.WriteString(k)
		cols = append(cols, k)
	}
	buf.WriteString(") values (")

	for i := 0; i < len(c.NewRow); i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("?")
	}
	buf.WriteString(")")

	sql = buf.String()
	return
}

func makeArgs(cols []string, r common.Row) (args []interface{}) {
	for _, cn := range cols {
		args = append(args, r[cn].Value)
	}
	return
}

func getSnapshotTx(db *sql.DB) (string, error) {
	row := db.QueryRow(
		"select value from _transicator_metadata where key = 'snapshot'")
	var snap string
	err := row.Scan(&snap)
	return snap, err
}
