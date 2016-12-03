package storage

import (
	"flag"
	"fmt"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
)

func TestStorage(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter("../test-reports/storage.xml")
	RunSpecsWithDefaultAndCustomReporters(t, "Storage suite", []Reporter{junitReporter})
}

func TestMain(m *testing.M) {
	flag.Parse()
	ret := m.Run()
	if largeDB != nil {
		fmt.Printf("Deleting %s\n", largeDBDir)
		largeDB.Close()
		largeDB.Delete()
	}
	os.Exit(ret)
}
