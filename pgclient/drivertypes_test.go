package pgclient

import (
	"bytes"
	"database/sql"
	"fmt"
	"testing/quick"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var driverTypeDB *sql.DB

var _ = Describe("Data type tests", func() {
	BeforeEach(func() {
		if dbURL != "" {
			var err error
			driverTypeDB, err = sql.Open("transicator", dbURL)
			Expect(err).Should(Succeed())
		}
	})

	AfterEach(func() {
		if driverTypeDB != nil {
			driverTypeDB.Exec("truncate table client_test")
			driverTypeDB.Close()
		}
	})

	It("String type", func() {
		ix := 0
		err := quick.Check(func(val string) bool {
			ix++
			return testStringType(ix, val)
		}, nil)
		Expect(err).Should(Succeed())
	})

	It("Integer type", func() {
		ix := 0
		err := quick.Check(func(val int) bool {
			ix++
			return testIntType(ix, val)
		}, nil)
		Expect(err).Should(Succeed())
	})

	It("Float type", func() {
		ix := 0
		err := quick.Check(func(val float32) bool {
			ix++
			return testFloatType(ix, val)
		}, nil)
		Expect(err).Should(Succeed())
	})

	It("Bool type", func() {
		ix := 0
		err := quick.Check(func(val bool) bool {
			ix++
			return testBoolType(ix, val)
		}, nil)
		Expect(err).Should(Succeed())
	})

	PIt("Bytes type", func() {
		ix := 0
		err := quick.Check(func(val []byte) bool {
			ix++
			return testBytesType(ix, val)
		}, nil)
		Expect(err).Should(Succeed())
	})

	PIt("Time type", func() {
		ix := 0
		err := quick.Check(func(val *time.Time) bool {
			ix++
			return testTimeType(ix, val)
		}, nil)
		Expect(err).Should(Succeed())
	})
})

func testStringType(ix int, val string) bool {
	_, err := driverTypeDB.Exec("insert into client_test (id, string) values ($1, $2)",
		ix, val)
	Expect(err).Should(Succeed())

	row := driverTypeDB.QueryRow("select string from client_test where id = $1",
		ix)
	var ret string
	err = row.Scan(&ret)
	Expect(err).Should(Succeed())
	Expect(ret).Should(Equal(val))
	return true
}

func testIntType(ix int, val int) bool {
	_, err := driverTypeDB.Exec("insert into client_test (id, int) values ($1, $2)",
		ix, val)
	Expect(err).Should(Succeed())

	row := driverTypeDB.QueryRow("select int from client_test where id = $1",
		ix)
	var ret int
	err = row.Scan(&ret)
	Expect(err).Should(Succeed())
	Expect(ret).Should(Equal(val))
	return true
}

func testFloatType(ix int, val float32) bool {
	_, err := driverTypeDB.Exec("insert into client_test (id, double) values ($1, $2)",
		ix, val)
	Expect(err).Should(Succeed())

	row := driverTypeDB.QueryRow("select double from client_test where id = $1",
		ix)
	var ret float32
	err = row.Scan(&ret)
	Expect(err).Should(Succeed())
	Expect(ret).Should(Equal(val))
	return true
}

func testBoolType(ix int, val bool) bool {
	_, err := driverTypeDB.Exec("insert into client_test (id, yesno) values ($1, $2)",
		ix, val)
	Expect(err).Should(Succeed())

	row := driverTypeDB.QueryRow("select yesno from client_test where id = $1",
		ix)
	var ret bool
	err = row.Scan(&ret)
	Expect(err).Should(Succeed())
	Expect(ret).Should(Equal(val))
	return true
}

func testBytesType(ix int, val []byte) bool {
	fmt.Fprintf(GinkgoWriter, "Testing bytes of length %d\n", len(val))
	_, err := driverTypeDB.Exec("insert into client_test (id, blob) values ($1, $2)",
		ix, val)
	Expect(err).Should(Succeed())

	row := driverTypeDB.QueryRow("select blob from client_test where id = $1",
		ix)
	var ret []byte
	err = row.Scan(&ret)
	Expect(err).Should(Succeed())
	Expect(bytes.Equal(ret, val)).Should(BeTrue())
	return true
}

func testTimeType(ix int, val *time.Time) bool {
	_, err := driverTypeDB.Exec("insert into client_test (id, timestamp) values ($1, $2)",
		ix, val)
	Expect(err).Should(Succeed())

	row := driverTypeDB.QueryRow("select timestamp from client_test where id = $1",
		ix)
	var ret time.Time
	err = row.Scan(&ret)
	Expect(err).Should(Succeed())
	fmt.Fprintf(GinkgoWriter, "Time %s == %s\n", ret, val)
	Expect(&ret).Should(Equal(val))
	return true
}