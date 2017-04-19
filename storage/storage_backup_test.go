// +build !rocksdb

/*
Copyright 2017 The Transicator Authors

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
	"bytes"
	"fmt"
	"os"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	backupFile = "./testDBBackup"
)

var _ = Describe("Backup tests", func() {
	BeforeEach(func() {
		var err error
		testDB, err = Open(testDBDir)
		Expect(err).Should(Succeed())
	})

	AfterEach(func() {
		testDB.Close()
		err := testDB.Delete()
		Expect(err).Should(Succeed())
		os.RemoveAll(backupFile)
	})

	It("Backup empty", func() {
		bc := testDB.Backup(backupFile)

		for {
			br := <-bc
			Expect(br.Error).Should(Succeed())
			if br.Done {
				return
			}
		}
	})

	It("Backup destination not found", func() {
		bc := testDB.Backup("/not/found/at/all")

		br := <-bc
		Expect(br.Error).ShouldNot(Succeed())
		Expect(br.Done).Should(BeTrue())
	})

	It("Backup small", func() {
		err := testDB.Put("foo", 1, 1, []byte("Hello!"))
		Expect(err).Should(Succeed())
		ret, err := testDB.Get("foo", 1, 1)
		Expect(err).Should(Succeed())
		Expect(ret).Should(Equal([]byte("Hello!")))

		bc := testDB.Backup(backupFile)

		for {
			br := <-bc
			Expect(br.Error).Should(Succeed())
			fmt.Fprintf(GinkgoWriter, "Backup %d remaining\n", br.PagesRemaining)
			if br.Done {
				break
			}
		}

		bdb, err := Open(backupFile)
		Expect(err).Should(Succeed())

		ret, err = bdb.Get("foo", 1, 1)
		Expect(err).Should(Succeed())
		Expect(ret).Should(Equal([]byte("Hello!")))
		bdb.Close()
	})

	It("Backup sequence", func() {
		testBlobs := generateRandomRecords()

		bc := testDB.Backup(backupFile)

		for {
			br := <-bc
			Expect(br.Error).Should(Succeed())
			fmt.Fprintf(GinkgoWriter, "Backup %d remaining\n", br.PagesRemaining)
			if br.Done {
				break
			}
		}

		bdb, err := Open(backupFile)
		Expect(err).Should(Succeed())
		defer bdb.Close()

		fmt.Fprintf(GinkgoWriter, "Reading %d test blobs from backup\n", len(testBlobs))
		testBlobs, _, _, err = bdb.Scan([]string{"seq"}, 0, 0, len(testBlobs), nil)
		Expect(err).Should(Succeed())

		for i, b := range testBlobs {
			Expect(bytes.Equal(b, testBlobs[i])).Should(BeTrue())
		}
	})

	It("Backup to an existed dir should fail", func() {
		os.Mkdir(backupFile, 0775)
		bc := testDB.Backup(backupFile)

		// expect error
		br := <-bc
		Expect(br.Error).ShouldNot(Succeed())

	})

	It("Backup to an existed db should fail", func() {
		generateRandomRecords()
		bc := testDB.Backup(backupFile)

		for {
			br := <-bc
			Expect(br.Error).Should(Succeed())
			fmt.Fprintf(GinkgoWriter, "Backup %d remaining\n", br.PagesRemaining)
			if br.Done {
				break
			}
		}

		// backup again to the same dest
		bc = testDB.Backup(backupFile)

		// expect error
		br := <-bc
		Expect(br.Error).ShouldNot(Succeed())

	})

	It("Test GetBackup", func() {
		testBlobs := generateRandomRecords()

		// use GetBackup to write backup to a file
		err := os.Mkdir(backupFile, 0775)
		fileWriter, err := os.OpenFile(backupFile+"/transicator", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0775)
		Expect(err).Should(Succeed())
		err = testDB.GetBackup(fileWriter)
		Expect(err).Should(Succeed())
		fileWriter.Close()

		// compare backup
		bdb, err := Open(backupFile)
		Expect(err).Should(Succeed())
		defer bdb.Close()

		fmt.Fprintf(GinkgoWriter, "Reading %d test blobs from backup\n", len(testBlobs))
		testBlobs, _, _, err = bdb.Scan([]string{"seq"}, 0, 0, len(testBlobs), nil)
		Expect(err).Should(Succeed())

		for i, b := range testBlobs {
			Expect(bytes.Equal(b, testBlobs[i])).Should(BeTrue())
		}
	})
})

// Generate some random byte records for testing.
func generateRandomRecords() (testBlobs [][]byte) {
	err := quick.Check(func(b []byte) bool {
		testBlobs = append(testBlobs, b)
		return true
	}, &quick.Config{
		// This will generate 100 records to test.
		MaxCountScale: 1.0,
	})
	Expect(err).Should(Succeed())
	fmt.Fprintf(GinkgoWriter, "Generated %d test blobs\n", len(testBlobs))

	for i, b := range testBlobs {
		err = testDB.Put("seq", uint64(i), 0, b)
		Expect(err).Should(Succeed())
	}
	fmt.Fprintf(GinkgoWriter, "Inserted %d test blobs\n", len(testBlobs))

	// Read them all back and compare them
	checkBlobs, _, _, err := testDB.Scan([]string{"seq"}, 0, 0, len(testBlobs), nil)
	Expect(err).Should(Succeed())
	for i, b := range checkBlobs {
		Expect(bytes.Equal(b, testBlobs[i])).Should(BeTrue())
	}
	return
}
