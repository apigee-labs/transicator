package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/30x/transicator/common"
	"github.com/30x/transicator/pgclient"
	"github.com/30x/transicator/replication"
	"github.com/Sirupsen/logrus"

	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	debugTests = true
)

var (
	dbURL string

	repl    *replication.Replicator
	dbConn0 *pgclient.PgConnection
	dbConn1 *pgclient.PgConnection
	dbConn2 *pgclient.PgConnection
	dbConn3 *pgclient.PgConnection
)

func TestSnapshot(t *testing.T) {
	dbURL = os.Getenv("TEST_PG_URL")
	if dbURL == "" {
		fmt.Println("Skipping replication tests because TEST_PG_URL not set")
		fmt.Println("  Example: postgres://user:password@host:port/database")
	} else {
		RegisterFailHandler(Fail)
		RunSpecs(t, "replication suite")
	}
}

var _ = BeforeSuite(func() {
	if debugTests {
		logrus.SetLevel(logrus.DebugLevel)
	}

	var err error
	dbConn0, err = pgclient.Connect(dbURL)
	Expect(err).Should(Succeed())
	dbConn1, err = pgclient.Connect(dbURL)
	Expect(err).Should(Succeed())
	dbConn2, err = pgclient.Connect(dbURL)
	Expect(err).Should(Succeed())
	dbConn3, err = pgclient.Connect(dbURL)
	Expect(err).Should(Succeed())

	if tableExists("snapshot_test") {
		err = truncateTable("snapshot_test")
		Expect(err).Should(Succeed())
	} else {
		_, _, err = dbConn1.SimpleQuery(testTableSQL)
		Expect(err).Should(Succeed())
	}

	repl, err = replication.Start(dbURL, "snapshot_test_slot")
	Expect(err).Should(Succeed())
	// There may be duplicates, so always drain first
	drainReplication(repl)
})

var _ = AfterSuite(func() {
	fmt.Println("AfterSuite: stop replication")
	if repl != nil {
		repl.Stop()
	}

	fmt.Println("AfterSuite: drop replication slot")
	dbConn1.SimpleQuery("select * from pg_drop_replication_slot('snapshot_test_slot')")

	fmt.Println("AfterSuite: close conns")
	if dbConn0 != nil {
		dbConn0.SimpleQuery("close")
		dbConn0.Close()
	}
	if dbConn2 != nil {
		dbConn2.Close()
	}
	if dbConn3 != nil {
		dbConn3.Close()
	}

	if dbConn1 != nil {
		tables := []string{"snapshot_test", "APP", "DEVELOPER"}
		for _, table := range tables {
			dropTable(table)
		}
		dbConn1.Close()
	}
})

func tableExists(name string) bool {
	_, _, err := dbConn1.SimpleQuery(fmt.Sprintf("select * from %s limit 0", name))
	return err == nil
}

func truncateTable(name string) error {
	// Deleting all the rows of the table does not lock as much and does not block tests
	_, _, err := dbConn1.SimpleQuery(fmt.Sprintf("delete from %s", name))
	return err
}

func dropTable(name string) error {
	_, _, err := dbConn1.SimpleQuery(fmt.Sprintf("drop table %s", name))
	return err
}

type SnapInfo struct {
	xmin     int32
	xmax     int32
	xip_list []int32
}

func getCurrentSnapshotInfo() *SnapInfo {
	_, s, err := dbConn0.SimpleQuery("begin isolation level repeatable read; select txid_current_snapshot()")
	Expect(err).Should(Succeed())
	_, _, err = dbConn0.SimpleQuery("commit")
	Expect(err).Should(Succeed())
	a := strings.Split(s[0][0], ":")
	si := new(SnapInfo)
	xmin, _ := strconv.Atoi(a[0])
	si.xmin = int32(xmin)
	xmax, _ := strconv.Atoi(a[1])
	si.xmax = int32(xmax)
	xips := strings.Split(a[2], ",")
	for _, xip := range xips {
		if xip != "" {
			v, _ := strconv.Atoi(xip)
			si.xip_list = append(si.xip_list, int32(v))
		}
	}
	return si
}

func doExecute1(query string) {
	_, _, err := dbConn1.SimpleQuery(query)
	Expect(err).Should(Succeed())
}
func doExecute2(query string) {
	_, _, err := dbConn2.SimpleQuery(query)
	Expect(err).Should(Succeed())
}
func doExecute3(query string) {
	_, _, err := dbConn3.SimpleQuery(query)
	Expect(err).Should(Succeed())
}

const testTableSQL = `
  create table snapshot_test (
    id varchar(32) primary key,
    bool boolean,
    chars char(64),
    varchars varchar(64),
    int integer,
    smallint smallint,
    bigint bigint,
    float float4,
    double float8,
    date date,
    time time with time zone,
    _apid_scope varchar(255),
    timestamp timestamp with time zone,
    timestampp timestamp
  );
  CREATE TABLE DEVELOPER (
	org varchar(255),
	id varchar(255),
	username varchar(255),
	firstname varchar(255),
	created_at integer,
	created_by varchar(255),
 	_apid_scope varchar(255),
	PRIMARY KEY (id, _apid_scope)
);
alter table developer replica identity full;
  CREATE TABLE APP (
	org varchar(255),
	id varchar(255),
	dev_id varchar(255) null,
	display_name varchar(255),
	name varchar(255),
	created_at integer,
	created_by varchar(255),
 	_apid_scope varchar(255),
	PRIMARY KEY (id, _apid_scope)
);
alter table developer replica identity full;
`

func getCurrentTxid1() int32 {
	return getCurrentTxid(dbConn1)
}

func getCurrentTxid2() int32 {
	return getCurrentTxid(dbConn2)
}

func getCurrentTxid(conn *pgclient.PgConnection) int32 {
	_, s, err := conn.SimpleQuery("select txid_current()")
	Expect(err).Should(Succeed())
	txid, _ := strconv.Atoi(s[0][0])
	return int32(txid)
}

func drainReplication(repl *replication.Replicator) {
	// Just pull stuff until we get a bit of a delay
	var maxLSN int64
	timedOut := false
	for !timedOut {
		timeout := time.After(1 * time.Second)
		select {
		case <-timeout:
			timedOut = true
		case change := <-repl.Changes():
			Expect(change.Error).Should(Succeed())
			fmt.Fprintf(GinkgoWriter, "LSN %d\n", change.CommitSequence)
			if change.CommitSequence > maxLSN {
				maxLSN = change.CommitSequence
			}
		}
	}

	fmt.Fprintf(GinkgoWriter, "Acknowledging %d\n", maxLSN)
	repl.Acknowledge(maxLSN)
}

func getReplChange(r *replication.Replicator) *common.Change {

	var c *common.Change
	Eventually(r.Changes()).Should(Receive(&c))
	Consistently(r.Changes()).ShouldNot(Receive())
	return c
}

/*
    ip_list   _actual latest commit txid of snapshot
          v  /
x x x x - - x - - -
        ^     ^
        xmin  xmax

xmin - Earliest transaction ID (txid) that is still active. All earlier transactions will either be committed and visible, or rolled back and dead.

xmax - First as-yet-unassigned txid. All txids greater than or equal to this are not yet started as of the time of the snapshot, and thus invisible.

xip_list - Active txids at the time of the snapshot. The list includes only those active txids between xmin and xmax; there might be active txids higher than xmax. A txid that is xmin <= txid < xmax and not in this list was already completed at the time of the snapshot, and thus either visible or dead according to its commit status. The list does not include txids of subtransactions.
*/

var _ = Describe("Taking a snapshot", func() {

	tables := []string{"snapshot_test", "developer", "app"}

	BeforeEach(func() {
		// There may be records in the replication queue from deletes done in AfterEach
		drainReplication(repl)
	})

	AfterEach(func() {
		//fmt.Println("AfterEach: rollback conns")
		//dbConn1.SimpleQuery("rollback")
		//dbConn2.SimpleQuery("rollback")
		//dbConn3.SimpleQuery("rollback")

		fmt.Println("AfterEach: truncate tables")
		for _, table := range tables {
			err := truncateTable(table)
			Expect(err).Should(Succeed())
		}
	})
	/*
		1. No pending tx:
		C0
			<-- xmin=1 xmax=1 xip=nil LSN=0
		C1
			<-- xmin=2 xmax=2 xip=nil LSN=1
		...
	*/
	Context("when no pending transactions exist", func() {

		It("should set xmin and xmax to commit txid + 1", func() {

			doExecute1("insert into snapshot_test (id) values ('no-pending-tx 0')")
			s0 := getCurrentSnapshotInfo()
			enc := getReplChange(repl)
			fmt.Printf("\nSnapshot info %+v Commit LSN = %d\n", s0, enc.CommitSequence)
			Expect(enc.NewRow["id"].Value).Should(Equal("no-pending-tx 0"))
			Expect(s0.xmin).To(BeEquivalentTo(enc.TransactionID + 1))
			Expect(s0.xmax).To(BeEquivalentTo(s0.xmin))
			Expect(s0.xip_list).To(BeNil())

			doExecute1("insert into snapshot_test (id) values ('no-pending-tx 1')")
			s1 := getCurrentSnapshotInfo()
			enc = getReplChange(repl)
			fmt.Printf("\nSnapshot info %+v Commit LSN = %d\n", s1, enc.CommitSequence)
			Expect(enc.NewRow["id"].Value).Should(Equal("no-pending-tx 1"))
			Expect(s1.xmin).To(BeEquivalentTo(enc.TransactionID + 1))
			Expect(s1.xmax).To(BeEquivalentTo(s1.xmin))
			Expect(s1.xip_list).To(BeNil())
		})

	})

	/*
		2. Single pending tx
		B1
			<-- xmin=1 xmax=1 xip=nil LSN(0)
		B2
			<-- xmin=1 xmax=1 xip=nil LSN(0)
		C1
			<-- xmin=2 xmax=2 xip=nil LSN(1)
		C2
			<-- xmin=3 xmax=3 xip=nil LSN(2)
	*/
	Context("when single pending transaction exists", func() {

		It("should ignore begins and set xmin and xmax to commit txid + 1", func() {

			doExecute1("begin")
			doExecute1("insert into snapshot_test (id) values ('single-pending-tx 1')")
			s0 := getCurrentSnapshotInfo()
			fmt.Printf("\nSnapshot info %+v\n", s0)
			Expect(s0.xmax).To(BeEquivalentTo(s0.xmin))
			Expect(s0.xip_list).To(BeNil())

			doExecute2("begin")
			doExecute2("insert into snapshot_test (id) values ('single-pending-tx 2')")
			s1 := getCurrentSnapshotInfo()
			fmt.Printf("\nSnapshot info %+v\n", s1)
			Expect(s1.xmin).To(BeEquivalentTo(s0.xmin))
			Expect(s1.xmax).To(BeEquivalentTo(s1.xmin))
			Expect(s1.xip_list).To(BeNil())

			doExecute1("commit")
			s2 := getCurrentSnapshotInfo()
			enc := getReplChange(repl)
			fmt.Printf("\nSnapshot info %+v Commit LSN = %d\n", s2, enc.CommitSequence)
			Expect(enc.NewRow["id"].Value).Should(Equal("single-pending-tx 1"))
			Expect(s2.xmin).To(BeEquivalentTo(enc.TransactionID + 1))
			Expect(s2.xmax).To(BeEquivalentTo(s2.xmin))
			Expect(s2.xip_list).To(BeNil())

			doExecute2("commit")
			s3 := getCurrentSnapshotInfo()
			enc = getReplChange(repl)
			fmt.Printf("\nSnapshot info %+v Commit LSN = %d\n", s3, enc.CommitSequence)
			Expect(enc.NewRow["id"].Value).Should(Equal("single-pending-tx 2"))
			Expect(s3.xmin).To(BeEquivalentTo(enc.TransactionID + 1))
			Expect(s3.xmax).To(BeEquivalentTo(s3.xmin))
			Expect(s3.xip_list).To(BeNil())
		})
	})

	/*
		3. Two pending tx in order
			B1
			B2
			B3
				<-- xmin=1 xmax=1 xip=nil LSN(0)
			C2
				<-- xmin=1 xmax=3 xip=1 LSN(2)
			C3
				<-- xmin=1 xmax=4 xip=1 LSN(3)
			C1
				<-- xmin=4 xmax=4 xip=nil LSN(1)
	*/
	Context("when two pending transactions committed in order", func() {

		It("should set xmax only to commit txid + 1", func() {

			doExecute1("begin")
			doExecute1("insert into snapshot_test (id) values ('2-pending-tx 1')")
			doExecute2("begin")
			doExecute2("insert into snapshot_test (id) values ('2-pending-tx 2')")
			doExecute3("begin")
			doExecute3("insert into snapshot_test (id) values ('2-pending-tx 3')")
			s0 := getCurrentSnapshotInfo()
			fmt.Printf("\nSnapshot info %+v\n", s0)
			Expect(s0.xmax).To(BeEquivalentTo(s0.xmin))
			Expect(s0.xip_list).To(BeNil())

			doExecute2("commit")
			s1 := getCurrentSnapshotInfo()
			enc := getReplChange(repl)
			fmt.Printf("\nSnapshot info %+v Commit LSN = %d\n", s1, enc.CommitSequence)
			Expect(enc.NewRow["id"].Value).Should(Equal("2-pending-tx 2"))
			Expect(s1.xmin).To(BeEquivalentTo(s0.xmin))
			Expect(s1.xmax).To(BeEquivalentTo(enc.TransactionID + 1))
			Expect(s1.xip_list).To(Equal([]int32{s1.xmin}))

			doExecute3("commit")
			s2 := getCurrentSnapshotInfo()
			enc = getReplChange(repl)
			fmt.Printf("\nSnapshot info %+v Commit LSN = %d\n", s2, enc.CommitSequence)
			Expect(enc.NewRow["id"].Value).Should(Equal("2-pending-tx 3"))
			Expect(s2.xmin).To(BeEquivalentTo(s0.xmin))
			Expect(s2.xmax).To(BeEquivalentTo(enc.TransactionID + 1))
			Expect(s2.xip_list).To(Equal([]int32{s1.xmin}))

			doExecute1("commit")
			s3 := getCurrentSnapshotInfo()
			enc = getReplChange(repl)
			fmt.Printf("\nSnapshot info %+v Commit LSN = %d\n", s3, enc.CommitSequence)
			Expect(enc.NewRow["id"].Value).Should(Equal("2-pending-tx 1"))
			Expect(s3.xmin).To(BeEquivalentTo(s2.xmax))
			Expect(s3.xmax).To(BeEquivalentTo(s3.xmin))
			Expect(s3.xip_list).To(BeNil())
		})
	})

	/*
		4. Two pending tx in reverse order
			B1
			B2
			B3
				<-- xmin=1 xmax=1 xip=nil LSN(0)
			C3
				<-- xmin=1 xmax=4 xip=1,2 LSN(3)
			C2
				<-- xmin=1 xmax=4 xip=1 LSN(2)
			C1
				<-- xmin=4 xmax=4 xip=nil LSN(1)
	*/
	Context("when two pending transactions committed in reverse order", func() {

		It("should set xmax to commit txid + 1, xip ", func() {

			doExecute1("begin")
			txid1 := getCurrentTxid1()
			doExecute1("insert into snapshot_test (id) values ('2-pending-tx-reverse 1')")
			doExecute2("begin")
			txid2 := getCurrentTxid2()
			doExecute2("insert into snapshot_test (id) values ('2-pending-tx-reverse 2')")
			doExecute3("begin")
			doExecute3("insert into snapshot_test (id) values ('2-pending-tx-reverse 3')")
			s0 := getCurrentSnapshotInfo()
			fmt.Printf("\nSnapshot info %+v\n", s0)
			Expect(s0.xmax).To(Equal(s0.xmin))
			Expect(s0.xip_list).To(BeNil())

			doExecute3("commit")
			s1 := getCurrentSnapshotInfo()
			enc := getReplChange(repl)
			fmt.Printf("\nSnapshot info %+v Commit LSN = %d\n", s1, enc.CommitSequence)
			Expect(enc.NewRow["id"].Value).Should(Equal("2-pending-tx-reverse 3"))
			Expect(s1.xmin).To(BeEquivalentTo(s0.xmin))
			Expect(s1.xmax).To(BeEquivalentTo(enc.TransactionID + 1))
			Expect(s1.xip_list).To(Equal([]int32{txid1, txid2}))

			doExecute2("commit")
			s2 := getCurrentSnapshotInfo()
			enc = getReplChange(repl)
			fmt.Printf("\nSnapshot info %+v Commit LSN = %d\n", s2, enc.CommitSequence)
			Expect(enc.NewRow["id"].Value).Should(Equal("2-pending-tx-reverse 2"))
			Expect(s2.xmin).To(BeEquivalentTo(s0.xmin))
			Expect(s2.xmax).To(BeEquivalentTo(s1.xmax))
			Expect(s2.xip_list).To(Equal([]int32{txid1}))

			doExecute1("commit")
			s3 := getCurrentSnapshotInfo()
			enc = getReplChange(repl)
			fmt.Printf("\nSnapshot info %+v Commit LSN = %d\n", s3, enc.CommitSequence)
			Expect(enc.NewRow["id"].Value).Should(Equal("2-pending-tx-reverse 1"))
			Expect(s3.xmin).To(BeEquivalentTo(s1.xmax))
			Expect(s3.xmax).To(BeEquivalentTo(s3.xmin))
			Expect(s3.xip_list).To(BeNil())
		})
	})

	/*
		Snapshot test cases

		5. Rollback no pending tx
			B1
				<-- xmin=1 xmax=1 xip=nil LSN(0)
			R1
				<-- xmin=2 xmax=2 xip=nil LSN(0)
	*/

	Context("Rollback when No pending transactions present", func() {

		It("should xmin == xmax, with xip == nil, xmin ==  txid + 1", func() {

			doExecute1("begin")
			txid1 := getCurrentTxid1()
			doExecute1("insert into snapshot_test (id) values ('0-pending-tx-1')")
			s0 := getCurrentSnapshotInfo()
			fmt.Printf("\nSnapshot info %+v\n", s0)
			Expect(s0.xmax).To(BeEquivalentTo(s0.xmin))
			Expect(s0.xip_list).To(BeNil())

			doExecute1("rollback")
			s1 := getCurrentSnapshotInfo()
			fmt.Printf("\nSnapshot info %+v\n", s1)
			Expect(s1.xmin).To(BeEquivalentTo(txid1 + 1))
			Expect(s1.xmax).To(BeEquivalentTo(s1.xmin))
			Expect(s1.xip_list).To(BeNil())

		})
	})

	/*
		6. Rollback Single pending tx
			B1
				<-- xmin=1 xmax=1 xip=nil LSN(0)
			B2
				<-- xmin=1 xmax=1 xip=nil LSN(0)
			R2
				<-- xmin=1 xmax=3 xip=1 LSN(0)
			C1
				<-- xmin=3 xmax=3 xip=nil LSN(1)
	*/

	tables = []string{"snapshot_test", "developer", "app"}

	BeforeEach(func() {
		// There may be records in the replication queue from deletes done in AfterEach

	})

	AfterEach(func() {
		//fmt.Println("AfterEach: rollback conns")
		//dbConn1.SimpleQuery("rollback")
		//dbConn2.SimpleQuery("rollback")
		//dbConn3.SimpleQuery("rollback")

		fmt.Println("AfterEach: truncate tables")
		for _, table := range tables {
			err := truncateTable(table)
			Expect(err).Should(Succeed())
		}
	})

	Context("Insert Tables (App & Developer), Get JSON data for ONE scope", func() {
		It("Insert with single scope, retrieve single scope data", func() {
			tables := []string{"app", "developer", "snapshot_test"}
			keys := []string{"org", "id", "username", "created_at", "_apid_scope", "dev_id", "display_name", "created_by", "created_at", "firstname", "name"}
			doExecute1("begin")
			doExecute1("insert into APP (org, id, dev_id, display_name, created_at, _apid_scope) values ('Pepsi', 'aaa-bbb-ccc', 'xxx-yyy-zzz', 'Oracle', 9859348, 'pepsi__dev')")

			doExecute1("commit")

			scope := []string{"pepsi__dev"}
			conn, err := pgclient.Connect(dbURL)
			Expect(err).Should(Succeed())
			defer conn.Close()
			b, err := GetTenantSnapshotData(scope, conn)
			Expect(err).Should(Succeed())
			Expect(conn).ShouldNot(BeNil())
			s, err := common.UnmarshalSnapshot(b)
			Expect(err).Should(Succeed())

			for _, table := range s.Tables {
				Expect(tables).To(ContainElement(table.Name))
				for _, row := range table.Rows {
					for l, col := range row {
						Expect(keys).To(ContainElement(l))
						if l == "id" {
							Expect(col.Value).To(Equal("aaa-bbb-ccc"))
						}
					}
				}
			}
			scope = []string{"pepsi_bad"}
			b, err = GetTenantSnapshotData(scope, conn)
			Expect(err).Should(Succeed())

			s, err = common.UnmarshalSnapshot(b)
			Expect(err).Should(Succeed())

			for _, table := range s.Tables {
				Expect(tables).To(ContainElement(table.Name))
				Expect(len(table.Rows)).To(Equal(0))
			}
		})

	})

	Context("Insert Tables (App & Developer), Get JSON data for Multi scopes", func() {
		It("Insert with multiple scopes, retrieve multiple scopes data", func() {
			tables := []string{"app", "developer", "snapshot_test"}
			keys := []string{"org", "id", "username", "created_at", "_apid_scope", "dev_id", "display_name", "created_by", "created_at", "firstname", "name"}
			idvals := []string{"xxx-yyy-zzz", "xxy-yyy-zzz", "xxz-yyy-zzz", "aaa-bbb-ccc"}
			doExecute1("begin")
			doExecute1("insert into DEVELOPER (org, id, username, created_at, _apid_scope) values ('Pepsi', 'xxx-yyy-zzz', 'sundar', 3231231, 'pepsi__dev')")
			doExecute1("insert into DEVELOPER (org, id, username, created_at, _apid_scope) values ('Pepsi', 'xxy-yyy-zzz', 'sundar', 3221231, 'pepsi__dev')")
			doExecute1("insert into DEVELOPER (org, id, username, created_at, _apid_scope) values ('Pepsi', 'xxz-yyy-zzz', 'sundar', 3231231, 'pepsi__test')")
			doExecute1("insert into APP (org, id, dev_id, display_name, created_at, _apid_scope) values ('Pepsi', 'aaa-bbb-ccc', 'xxx-yyy-zzz', 'Oracle', 9859348, 'pepsi__dev')")

			doExecute1("commit")

			scope := []string{"pepsi__dev", "pepsi__test"}
			conn, err := pgclient.Connect(dbURL)
			Expect(err).Should(Succeed())
			defer conn.Close()
			b, err := GetTenantSnapshotData(scope, conn)
			Expect(err).Should(Succeed())

			s, err := common.UnmarshalSnapshot(b)
			Expect(err).Should(Succeed())

			for _, table := range s.Tables {
				fmt.Fprintf(GinkgoWriter, "Got data for %s\n", table.Name)
				Expect(tables).To(ContainElement(table.Name))
				for _, row := range table.Rows {
					for l, col := range row {
						Expect(keys).To(ContainElement(l))
						if l == "id" {
							Expect(idvals).To(ContainElement(col.Value))
						}
					}
				}
			}
			scope = []string{"pepsi_bad"}
			b, err = GetTenantSnapshotData(scope, conn)
			Expect(err).Should(Succeed())

			s, err = common.UnmarshalSnapshot(b)
			Expect(err).Should(Succeed())

			for _, table := range s.Tables {
				Expect(tables).To(ContainElement(table.Name))
				Expect(len(table.Rows)).To(Equal(0))
			}
		})

	})

})
