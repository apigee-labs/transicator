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
package pgclient

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/Sirupsen/logrus"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
)

const (
	debugTests = false
)

var dbURL string

func TestPGClient(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter("../test-reports/pgclient.xml")
	RunSpecsWithDefaultAndCustomReporters(t, "Pgclient suite", []Reporter{junitReporter})
}

var _ = BeforeSuite(func() {
	if debugTests {
		logrus.SetLevel(logrus.DebugLevel)
	}

	dbURL = os.Getenv("TEST_PG_URL")
	if dbURL == "" {
		fmt.Println("Skipping many tests because TEST_PG_URL not set")
		fmt.Println("Format:")
		fmt.Println("  postgres://user:password@host:port/database")
	} else {
		db, err := sql.Open("transicator", dbURL)
		Expect(err).Should(Succeed())
		defer db.Close()

		_, err = db.Exec(`
			create table client_test (
				id integer primary key,
				string varchar,
				int bigint,
				sint smallint,
				iint int,
				double float8,
				timestamp timestamp with time zone,
				ts timestamp,
				yesno bool,
				blob bytea
			) with oids`)
		Expect(err).Should(Succeed())
	}
})

var _ = AfterSuite(func() {
	if dbURL != "" {
		db, err := sql.Open("transicator", dbURL)
		Expect(err).Should(Succeed())
		defer db.Close()

		db.Exec("drop table client_test")
	}
})
