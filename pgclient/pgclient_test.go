package pgclient

import (
	"fmt"
	"os"
	"testing"

	"github.com/Sirupsen/logrus"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var dbHost string

func TestPGClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pgclient suite")
}

var _ = BeforeSuite(func() {
	logrus.SetLevel(logrus.DebugLevel)

	dbHost = os.Getenv("TEST_PG_HOST")
	if dbHost == "" {
		fmt.Printf("Skipping many tests because TEST_PG_HOST not set\n")
	}
})
