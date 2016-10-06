package test

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"testing"

	"github.com/30x/transicator/pgclient"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var portSpec = regexp.MustCompile("^[\\.0-9]+:([0-9]+)->.*$")

func TestCombined(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Combined suite")
}

var changeBase string
var snapshotBase string
var dbURL string
var db *sql.DB

var _ = BeforeSuite(func() {
	changeHost := os.Getenv("CHANGE_HOST")
	Expect(changeHost).ShouldNot(BeEmpty())
	snapshotHost := os.Getenv("SNAPSHOT_HOST")
	Expect(snapshotHost).ShouldNot(BeEmpty())
	changePortSpec := os.Getenv("CHANGE_PORT")
	Expect(changePortSpec).ShouldNot(BeEmpty())
	snapPortSpec := os.Getenv("SNAPSHOT_PORT")
	Expect(snapPortSpec).ShouldNot(BeEmpty())
	dbURL = os.Getenv("TEST_PG_URL")
	Expect(dbURL).ShouldNot(BeEmpty())

	changeBase = fmt.Sprintf("http://%s:%s", changeHost, changePortSpec)
	fmt.Printf("Change server at %s\n", changeBase)
	snapshotBase = fmt.Sprintf("http://%s:%s", snapshotHost, snapPortSpec)
	fmt.Printf("Snapshot server at %s\n", snapshotBase)

	var err error
	pgclient.RegisterDriver()
	db, err = sql.Open("transicator", dbURL)
	Expect(err).Should(Succeed())

	_, err = db.Exec(tableSQL)
	Expect(err).Should(Succeed())
})

var _ = AfterSuite(func() {
	if db != nil {
		db.Exec("drop table combined_test")
		db.Close()
	}
})

var tableSQL = `
  create table combined_test(
    id integer primary key,
    value varchar,
		_apid_scope varchar);
  alter table combined_test replica identity full;
`
