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

	senders := make([]*sender, numSenders)
	receivers := make([]*receiver, numSenders)

	for i := 0; i < numSenders; i++ {
		selector := strconv.Itoa(rand.Int())
		sender := startSender(
			selector, db,
			defaultWindowSize, defaultBatchSize)
		senders[i] = sender

		dbFileName := path.Join(dataDir, strconv.Itoa(i))

		receiver := startReceiver(
			selector, dbFileName,
			ssURL, csURL,
			sender, wg)
		receivers[i] = receiver
	}

	time.Sleep(testDuration)

	for i := 0; i < numSenders; i++ {
		senders[i].stop(wg)
		receivers[i].canStop()
	}
	wg.Wait()
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
