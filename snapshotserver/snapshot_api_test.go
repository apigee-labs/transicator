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
package snapshotserver

import (
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"

	"time"

	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Snapshot API Tests", func() {
	It("Default snapshot", func() {
		insertApp("jsonSnap", "jsonSnap", "snaptests")

		resp, err := http.Get(fmt.Sprintf("%s/snapshots?scope=snaptests", testBase))
		Expect(err).Should(Succeed())
		defer resp.Body.Close()
		Expect(resp.StatusCode).Should(Equal(200))
		Expect(resp.Header.Get("Content-Type")).Should(Equal("application/json"))
		bod, err := ioutil.ReadAll(resp.Body)
		Expect(err).Should(Succeed())
		ss, err := common.UnmarshalSnapshot(bod)
		Expect(err).Should(Succeed())

		appTable := getTable(ss, "public.app")
		r := getRowByID(appTable, "jsonSnap")
		Expect(r).ShouldNot(BeNil())

		var org string
		err = r.Get("org", &org)
		Expect(err).Should(Succeed())
		Expect(org).Should(Equal("testorg"))

		var appID string
		err = r.Get("id", &appID)
		Expect(err).Should(Succeed())
		Expect(appID).Should(Equal("jsonSnap"))

		var devID string
		err = r.Get("dev_id", &devID)
		Expect(err).Should(Succeed())
		Expect(devID).Should(Equal("jsonSnap"))

		devTable := getTable(ss, "public.developer")
		r = getRowByID(devTable, "jsonSnap")

		err = r.Get("org", &org)
		Expect(err).Should(Succeed())
		Expect(org).Should(Equal("testorg"))

		err = r.Get("id", &devID)
		Expect(err).Should(Succeed())
		Expect(devID).Should(Equal("jsonSnap"))
	})

	It("JSON snapshot two scopes", func() {
		insertApp("jsonSnap2", "jsonSnap2", "snaptests2")
		insertApp("jsonSnap3", "jsonSnap3", "snaptests2")

		resp, err := http.Get(fmt.Sprintf("%s/snapshots?scope=snaptests2&scope=snaptests3", testBase))
		Expect(err).Should(Succeed())
		defer resp.Body.Close()
		Expect(resp.StatusCode).Should(Equal(200))
		Expect(resp.Header.Get("Content-Type")).Should(Equal("application/json"))
		bod, err := ioutil.ReadAll(resp.Body)
		Expect(err).Should(Succeed())
		ss, err := common.UnmarshalSnapshot(bod)
		Expect(err).Should(Succeed())

		appTable := getTable(ss, "public.app")
		r := getRowByID(appTable, "jsonSnap2")
		Expect(r).ShouldNot(BeNil())
		r2 := getRowByID(appTable, "jsonSnap3")
		Expect(r2).ShouldNot(BeNil())

		var org string
		err = r.Get("org", &org)
		Expect(err).Should(Succeed())
		Expect(org).Should(Equal("testorg"))

		var appID string
		err = r.Get("id", &appID)
		Expect(err).Should(Succeed())
		Expect(appID).Should(Equal("jsonSnap2"))

		var devID string
		err = r.Get("dev_id", &devID)
		Expect(err).Should(Succeed())
		Expect(devID).Should(Equal("jsonSnap2"))

		devTable := getTable(ss, "public.developer")
		r = getRowByID(devTable, "jsonSnap2")

		err = r.Get("org", &org)
		Expect(err).Should(Succeed())
		Expect(org).Should(Equal("testorg"))

		err = r.Get("id", &devID)
		Expect(err).Should(Succeed())
		Expect(devID).Should(Equal("jsonSnap2"))
	})

	It("SQLite snapshot", func() {
		dbDir, err := ioutil.TempDir("", "snaptest")
		Expect(err).Should(Succeed())
		defer os.RemoveAll(dbDir)

		insertApp("sqlSnap", "sqlSnap", "snaptests3")

		req, err := http.NewRequest("GET",
			fmt.Sprintf("%s/snapshots?scope=snaptests3", testBase), nil)
		Expect(err).Should(Succeed())
		req.Header = http.Header{}
		req.Header.Set("Accept", "application/transicator+sqlite")

		resp, err := http.DefaultClient.Do(req)
		Expect(err).Should(Succeed())
		defer resp.Body.Close()
		Expect(resp.StatusCode).Should(Equal(200))
		Expect(resp.Header.Get("Content-Type")).Should(Equal("application/transicator+sqlite"))

		dbFileName := path.Join(dbDir, "snapdb")
		outFile, err := os.OpenFile(dbFileName, os.O_RDWR|os.O_CREATE, 0666)
		Expect(err).Should(Succeed())
		_, err = io.Copy(outFile, resp.Body)
		outFile.Close()
		Expect(err).Should(Succeed())

		sdb, err := sql.Open("sqlite3", dbFileName)
		Expect(err).Should(Succeed())
		defer sdb.Close()

		row := sdb.QueryRow("select org, id, dev_id from app where id = 'sqlSnap'")

		var org string
		var id string
		var devID string

		err = row.Scan(&org, &id, &devID)
		Expect(err).Should(Succeed())
		Expect(org).Should(Equal("testorg"))
		Expect(id).Should(Equal("sqlSnap"))
		Expect(devID).Should(Equal("sqlSnap"))
	})

	It("SQLite types", func() {
		is, err := db.Prepare(`
		insert into snapshot_test
		(id, bool, smallint, bigint, float, double, date, time, timestamp, timestampp, blob, _change_selector)
		values
		($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		`)
		Expect(err).Should(Succeed())
		testTime := time.Now()
		testTimestampStr := testTime.UTC().Format(sqliteTimestampFormat)
		testDateStr := testTime.UTC().Format("2006-01-02")
		testTimeStr := testTime.Format("15:04:05-07")
		testBuf := []byte("Hello, World!")

		_, err = is.Exec("types1", true, int16(123), int64(456), float32(1.23), float64(4.56),
			testTime, testTime, testTime, testTime, testBuf, "typetest")
		Expect(err).Should(Succeed())
		_, err = is.Exec("types2", true, int16(-123), int64(-456), float32(-1.23), float64(-4.56),
			testTime, testTime, testTime, testTime, testBuf, "typetest")
		Expect(err).Should(Succeed())
		_, err = is.Exec("types3", false, 0, 0, 0.0, 0.0, nil, nil, nil, nil, nil, "typetest")
		Expect(err).Should(Succeed())

		dbDir, err := ioutil.TempDir("", "snaptest3")
		Expect(err).Should(Succeed())
		defer os.RemoveAll(dbDir)

		sdb := getSqliteSnapshot(dbDir, "typetest")
		defer sdb.Close()

		query := `
		select bool, smallint, bigint, float, double, date, time, timestamp, timestampp, blob
		from snapshot_test
		where _change_selector = 'typetest'
		order by id
		`

		rows, err := sdb.Query(query)
		Expect(err).Should(Succeed())
		defer rows.Close()

		var b bool
		var si int16
		var sl int64
		var fs float32
		var fl float64
		var da string
		var tim string
		var ts string
		var tss string
		var bb []byte

		Expect(rows.Next()).Should(BeTrue())
		err = rows.Scan(&b, &si, &sl, &fs, &fl, &da, &tim, &ts, &tss, &bb)
		Expect(err).Should(Succeed())
		Expect(b).Should(BeTrue())
		Expect(si).Should(Equal(int16(123)))
		Expect(sl).Should(Equal(int64(456)))
		Expect(fs).Should(Equal(float32(1.23)))
		Expect(fl).Should(Equal(float64(4.56)))
		Expect(da).Should(Equal(testDateStr))
		Expect(tim).Should(Equal(testTimeStr))
		Expect(ts).Should(Equal(testTimestampStr))
		Expect(tss).Should(Equal(testTimestampStr))
		Expect(bb).Should(Equal(testBuf))

		Expect(rows.Next()).Should(BeTrue())
		err = rows.Scan(&b, &si, &sl, &fs, &fl, &da, &tim, &ts, &tss, &bb)
		Expect(err).Should(Succeed())
		Expect(b).Should(BeTrue())
		Expect(si).Should(Equal(int16(-123)))
		Expect(sl).Should(Equal(int64(-456)))
		Expect(fs).Should(Equal(float32(-1.23)))
		Expect(fl).Should(Equal(float64(-4.56)))
		Expect(da).Should(Equal(testDateStr))
		Expect(tim).Should(Equal(testTimeStr))
		Expect(ts).Should(Equal(testTimestampStr))
		Expect(tss).Should(Equal(testTimestampStr))
		Expect(bb).Should(Equal(testBuf))

		Expect(rows.Next()).Should(BeTrue())
		err = rows.Scan(&b, &si, &sl, &fs, &fl, &da, &tim, &ts, &tss, &bb)
		Expect(err).Should(Succeed())
		Expect(b).Should(BeFalse())
		Expect(si).Should(BeZero())
		Expect(sl).Should(BeZero())
		Expect(fs).Should(BeZero())
		Expect(fl).Should(BeZero())
		Expect(da).Should(BeEmpty())
		Expect(tim).Should(BeEmpty())
		Expect(ts).Should(BeEmpty())
		Expect(tss).Should(BeEmpty())
		Expect(bb).Should(BeNil())

		Expect(rows.Next()).ShouldNot(BeTrue())
	})
})

func insertApp(devID, appID, selector string) {
	_, err := db.Exec(`
	insert into public.developer (org, id, created_at, created_by, _change_selector)
	values ('testorg', $1, now(), 'testcreator', $2)
	`, devID, selector)
	Expect(err).Should(Succeed())

	_, err = db.Exec(`
	insert into public.app (org, id, dev_id, created_at, created_by, _change_selector)
	values ('testorg', $1, $2, now(), 'testcreator', $3)
	`, appID, devID, selector)
	Expect(err).Should(Succeed())
}

func getTable(ss *common.Snapshot, name string) *common.Table {
	for _, t := range ss.Tables {
		if t.Name == name {
			return &t
		}
	}
	Expect(false).Should(BeTrue())
	return nil
}

func getRowByID(t *common.Table, id string) common.Row {
	for _, r := range t.Rows {
		if r["id"] != nil && r["id"].Value.(string) == id {
			return r
		}
	}
	Expect(false).Should(BeTrue())
	return nil
}

func getSqliteSnapshot(dbDir, scope string) *sql.DB {
	req, err := http.NewRequest("GET",
		fmt.Sprintf("%s/snapshots?scope=%s", testBase, scope), nil)
	Expect(err).Should(Succeed())
	req.Header = http.Header{}
	req.Header.Set("Accept", "application/transicator+sqlite")

	resp, err := http.DefaultClient.Do(req)
	Expect(err).Should(Succeed())
	defer resp.Body.Close()
	Expect(resp.StatusCode).Should(Equal(200))
	Expect(resp.Header.Get("Content-Type")).Should(Equal("application/transicator+sqlite"))

	dbFileName := path.Join(dbDir, "snapdb")
	outFile, err := os.OpenFile(dbFileName, os.O_RDWR|os.O_CREATE, 0666)
	Expect(err).Should(Succeed())
	_, err = io.Copy(outFile, resp.Body)
	outFile.Close()
	Expect(err).Should(Succeed())

	sdb, err := sql.Open("sqlite3", dbFileName)
	Expect(err).Should(Succeed())
	return sdb
}
