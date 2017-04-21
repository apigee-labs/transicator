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
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/apigee-labs/transicator/common"
	"github.com/julienschmidt/httprouter"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

const roundedTimestampFormat = "2006-01-02 15:04:05.999999-07:00"

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

		req, err := http.NewRequest("GET",
			fmt.Sprintf("%s/snapshots?scope=snaptests2&scope=snaptests3", testBase),
			nil)
		Expect(err).Should(Succeed())
		req.Header = http.Header{}
		req.Header.Set("Accept", "application/json")

		resp, err := http.DefaultClient.Do(req)
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

	It("should return error if DB error", func() {

		router := httprouter.New()
		router.GET("/snapshots", func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
			defer GinkgoRecover()
			GenSnapshot(w, r)
		})
		router.GET("/data", func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
			defer GinkgoRecover()
			sql.Register("badDriver", badDriver{})
			db, err := sql.Open("badDriver", "badDriver")
			Expect(err).NotTo(HaveOccurred())
			DownloadSnapshot(w, r, db, p)
		})

		ts := httptest.NewServer(router)

		resp, err := http.Get(fmt.Sprintf("%s/snapshots?scope=snaptests", ts.URL))
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).Should(Equal(500))
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

		row := sdb.QueryRow("select org, id, dev_id from public_app where id = 'sqlSnap'")

		var org string
		var id string
		var devID string

		err = row.Scan(&org, &id, &devID)
		Expect(err).Should(Succeed())
		Expect(org).Should(Equal("testorg"))
		Expect(id).Should(Equal("sqlSnap"))
		Expect(devID).Should(Equal("sqlSnap"))

		rows, err := sdb.Query(`
			select columnName, primaryKey from _transicator_tables
			where tableName = 'public_app'
		`)
		Expect(err).Should(Succeed())
		defer rows.Close()

		r := make(map[string]int)
		for rows.Next() {
			var cn string
			var pk bool
			err = rows.Scan(&cn, &pk)
			Expect(err).Should(Succeed())
			if pk {
				r[cn] = 2
			} else {
				r[cn] = 1
			}
		}

		Expect(r["org"]).Should(Equal(1))
		Expect(r["dev_id"]).Should(Equal(1))
		Expect(r["display_name"]).Should(Equal(1))
		Expect(r["name"]).Should(Equal(1))
		Expect(r["created_at"]).Should(Equal(1))
		Expect(r["created_by"]).Should(Equal(1))
		Expect(r["id"]).Should(Equal(2))
		Expect(r["_change_selector"]).Should(Equal(2))

		row = sdb.QueryRow(
			"select value from _transicator_metadata where key = 'snapshot'")
		var snap string
		err = row.Scan(&snap)

		Expect(err).Should(Succeed())
		Expect(resp.Header.Get("Transicator-Snapshot-TXID")).To(Equal(snap))

	})

	It("SQLite snapshot empty scope", func() {
		dbDir, err := ioutil.TempDir("", "snaptest")
		Expect(err).Should(Succeed())
		defer os.RemoveAll(dbDir)

		req, err := http.NewRequest("GET",
			fmt.Sprintf("%s/snapshots?scope=wrong_scope_you_dope", testBase), nil)
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

		row := sdb.QueryRow("select count(*) from public_app")
		var count int
		err = row.Scan(&count)
		Expect(err).Should(Succeed())
		Expect(count).Should(BeZero())
	})

	It("SQLite snapshot missing scope", func() {
		req, err := http.NewRequest("GET",
			fmt.Sprintf("%s/snapshots", testBase), nil)
		Expect(err).Should(Succeed())
		req.Header = http.Header{}
		req.Header.Set("Accept", "application/json")

		resp, err := http.DefaultClient.Do(req)
		Expect(err).Should(Succeed())
		defer resp.Body.Close()
		Expect(resp.StatusCode).Should(Equal(400))
		Expect(resp.Header.Get("Content-Type")).Should(Equal("application/json"))

		bod, err := ioutil.ReadAll(resp.Body)
		Expect(err).Should(Succeed())

		var errMsg common.APIError
		err = json.Unmarshal(bod, &errMsg)
		Expect(err).Should(Succeed())
		Expect(errMsg.Code).Should(Equal("MISSING_SCOPE"))
	})

	It("SQLite snapshot bad accept header", func() {
		req, err := http.NewRequest("GET",
			fmt.Sprintf("%s/snapshots?scope=nope_not_the_scope", testBase), nil)
		Expect(err).Should(Succeed())
		req.Header = http.Header{}
		req.Header.Set("Accept", "text/plain")

		resp, err := http.DefaultClient.Do(req)
		Expect(err).Should(Succeed())
		defer resp.Body.Close()
		Expect(resp.StatusCode).Should(Equal(415))
		Expect(resp.Header.Get("Content-Type")).Should(Equal("application/json"))

		bod, err := ioutil.ReadAll(resp.Body)
		Expect(err).Should(Succeed())

		var errMsg common.APIError
		err = json.Unmarshal(bod, &errMsg)
		Expect(err).Should(Succeed())
		Expect(errMsg.Code).Should(Equal("UNSUPPORTED_MEDIA_TYPE"))
	})

	It("SQLite types", func() {
		is, err := db.Prepare(`
		insert into public.snapshot_test
		(id, bool, smallint, bigint, float, double, date, time, timestamp, timestampp, blob, _change_selector)
		values
		($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		`)
		Expect(err).Should(Succeed())
		testTime := time.Now()
		testTimestampStr := testTime.UTC().Format(roundedTimestampFormat)
		testDateStr := testTime.Format("2006-01-02")
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

		sdb, _ := getSqliteSnapshot(dbDir, "typetest")
		defer sdb.Close()

		query := `
		select bool, smallint, bigint, float, double, date, time, timestamp, timestampp, blob
		from public_snapshot_test
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

	It("should detect invalid chars in scope query param", func() {
		req, err := createStandardRequest("GET",
			fmt.Sprintf("%s/snapshots?scope=%s", testBase, url.QueryEscape("snaptests;select now()")), nil)
		resp, err := http.DefaultClient.Do(req)
		Expect(err).Should(Succeed())
		defer resp.Body.Close()
		checkApiErrorCode(resp, http.StatusBadRequest, "INVALID_REQUEST_PARAM")
	})

	It("should detect invalid chars in multiple scope query params", func() {
		req, err := createStandardRequest("GET",
			fmt.Sprintf("%s/snapshots?scope=abc123&scope=%s", testBase, url.QueryEscape("snapstests;select now()")), nil)
		resp, err := http.DefaultClient.Do(req)
		Expect(err).Should(Succeed())
		defer resp.Body.Close()
		checkApiErrorCode(resp, http.StatusBadRequest, "INVALID_REQUEST_PARAM")
	})

})

func createStandardRequest(method string, urlStr string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, urlStr, body)
	Expect(err).Should(Succeed())
	req.Header = http.Header{}
	req.Header.Set("Accept", "application/json")
	dump, err := httputil.DumpRequestOut(req, true)
	fmt.Fprintf(GinkgoWriter, "\ndump client req: %q\nerr: %+v\n", dump, err)
	return req, err
}

func checkApiErrorCode(resp *http.Response, sc int, ec string) {
	dump, err := httputil.DumpResponse(resp, true)
	fmt.Fprintf(GinkgoWriter, "\nAPIError response: %q\nerr: %+v\n", dump, err)

	Expect(resp.StatusCode).Should(Equal(sc))
	Expect(resp.Header.Get("Content-Type")).Should(Equal("application/json"))

	bod, err := ioutil.ReadAll(resp.Body)
	Expect(err).Should(Succeed())

	var errMsg common.APIError
	err = json.Unmarshal(bod, &errMsg)
	Expect(err).Should(Succeed())
	Expect(errMsg.Code).Should(Equal(ec))
}

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

func getSqliteSnapshot(dbDir, scope string) (*sql.DB, string) {
	req, err := http.NewRequest("GET",
		fmt.Sprintf("%s/snapshots?scope=%s", testBase, scope), nil)
	Expect(err).Should(Succeed())
	req.Header = http.Header{}
	req.Header.Set("Accept", "application/transicator+sqlite")

	resp, err := http.DefaultClient.Do(req)
	Expect(err).Should(Succeed())
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		var buf []byte
		buf, err = ioutil.ReadAll(resp.Body)
		Expect(err).Should(Succeed())
		fmt.Fprintf(GinkgoWriter, "Error on response: %s\n", string(buf))
	}
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

	row := sdb.QueryRow(
		"select value from _transicator_metadata where key = 'snapshot'")
	var snap string
	err = row.Scan(&snap)
	Expect(err).Should(Succeed())

	return sdb, snap
}

type badDriver struct{}

func (d badDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("intentional failure for testing")
}
