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
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	// Empty import to ensure PG driver is loaded
	_ "github.com/apigee-labs/transicator/pgclient"
	// Same for SQLite driver
	_ "github.com/mattn/go-sqlite3"
)

const (
	defaultWindowSize = 100
	defaultBatchSize  = 100
)

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage:\n")
	flag.PrintDefaults()
}

func main() {
	var pgURL, ssURL, csURL, durStr, dataDir string
	var numSenders int

	flag.StringVar(&durStr, "d", "2m", "Duration of test run")
	flag.StringVar(&pgURL, "pg", "", "Postgres URL")
	flag.StringVar(&ssURL, "ss", "", "Snapshot server URL")
	flag.StringVar(&csURL, "cs", "", "Change server URL")
	flag.StringVar(&dataDir, "l", "./data", "Location of test data directory")
	flag.IntVar(&numSenders, "s", 1, "Number of senders and receivers")
	flag.Parse()

	if !flag.Parsed() {
		printUsage()
		return
	}
	if (pgURL == "") || (ssURL == "") || (csURL == "") {
		printUsage()
		return
	}

	testDuration, err := time.ParseDuration(durStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid duration: %s\n", err)
		printUsage()
		return
	}

	// TODO seed RNG

	fmt.Printf("Going to put data in %s\n", dataDir)
	os.RemoveAll(dataDir)
	err = os.MkdirAll(dataDir, 0777)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating data directory: %s\n", err)
		return
	}

	var db *sql.DB

	db, err = sql.Open("transicator", pgURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating PG driver: %s\n", err)
		return
	}
	defer db.Close()

	err = makeTables(db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating PG table: %s\n", err)
		return
	}
	defer func() {
		cleanTables(db)
	}()

	wg := &sync.WaitGroup{}
	wg.Add(numSenders * 2)

	selectors := make([]string, numSenders)
	senders := make([]*sender, numSenders)
	receivers := make([]*receiver, numSenders)

	for i := 0; i < numSenders; i++ {
		selector := strconv.Itoa(rand.Int())
		selectors[i] = selector
		sender := startSender(
			selector, db,
			defaultWindowSize, defaultBatchSize, wg)
		senders[i] = sender

		receiver := startReceiver(
			selector, getDataDir(dataDir, i),
			ssURL, csURL,
			sender, wg)
		receivers[i] = receiver
	}

	time.Sleep(testDuration)

	for i := 0; i < numSenders; i++ {
		senders[i].stop()
		receivers[i].canStop()
	}
	wg.Wait()

	for i := 0; i < numSenders; i++ {
		verifyTables(i, selectors[i], db, dataDir)
	}
}

func makeTables(db *sql.DB) error {
	cleanTables(db)
	_, err := db.Exec(testTable)
	return err
}

func cleanTables(db *sql.DB) error {
	_, err := db.Exec("drop table stress_table")
	return err
}

func verifyTables(i int, selector string, pgDB *sql.DB, dataDir string) {
	liteDB, err := sql.Open("sqlite3", getDataDir(dataDir, i))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can't open SQLite: %s\n", err)
		return
	}
	defer liteDB.Close()

	pgRows, err := pgDB.Query(`
		select content from stress_table where _change_selector = $1 order by id, grp
	`, selector)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can't select from Postgres: %s\n", err)
		return
	}
	defer pgRows.Close()

	liteRows, err := liteDB.Query(`
		select content from stress_table order by id, grp
	`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can't select from SQLite: %s\n", err)
		return
	}
	defer liteRows.Close()

	rc := 0
	for pgRows.Next() {
		if !liteRows.Next() {
			fmt.Fprintf(os.Stderr, "** More Postgres rows than SQLite rows\n")
			break
		}

		var pgBuf, sBuf []byte
		err = pgRows.Scan(&pgBuf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "** Error scanning PG row: %s\n", err)
			continue
		}

		err = liteRows.Scan(&sBuf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "** Error scanning SQLite row: %s\n", err)
			continue
		}

		if !bytes.Equal(pgBuf, sBuf) {
			fmt.Fprintf(os.Stderr, "** Postgres and SQLite content does not match\n")
			continue
		}
		rc++
	}
	if liteRows.Next() {
		fmt.Fprintf(os.Stderr, "** More SQLite rows than Postgres rows\n")
	}
	fmt.Printf("Done verifying sender. Verified %d rows\n", rc)
}

func getDataDir(base string, i int) string {
	return path.Join(base, strconv.Itoa(i))
}

const testTable = `
create table stress_table (
  id integer primary key,
  grp integer not null,
  sequence integer not null,
  content bytea,
  last bool,
  _change_selector varchar not null
);
`
