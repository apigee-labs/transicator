/*
Copyright 2016 Google Inc.

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
package storage

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/Sirupsen/logrus"
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
	logrus.SetLevel(logrus.ErrorLevel)
	flag.Parse()
	ret := m.Run()
	if largeDB != nil {
		fmt.Printf("Deleting %s\n", largeDBDir)
		largeDB.Close()
		largeDB.Delete()
	}
	if cleanDB != nil {
		fmt.Printf("Deleting %s\n", cleanDBDir)
		cleanDB.Close()
		cleanDB.Delete()
	}
	os.Exit(ret)
}
