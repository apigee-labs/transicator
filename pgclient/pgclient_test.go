package pgclient

import (
	"fmt"
	"os"
	"testing"

	"github.com/Sirupsen/logrus"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var dbURL string

func TestPGClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pgclient suite")
}

var _ = BeforeSuite(func() {
	logrus.SetLevel(logrus.DebugLevel)

	dbURL = os.Getenv("TEST_PG_URL")
	if dbURL == "" {
		fmt.Println("Skipping many tests because TEST_PG_URL not set")
		fmt.Println("Format:")
		fmt.Println("  postgres://user:password@host:port/database")
	}
})
