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
package bootstrap_test

import (
	"github.com/apigee-labs/transicator/bootstrap"
	"github.com/apigee-labs/transicator/storage"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"os"
	"time"
)

const (
	secret = "testing"
)

var _ = Describe("Bootstrap tests", func() {

	var (
		testDB  storage.DB
		testDir string
	)

	BeforeEach(func() {
		tmpDir, err := ioutil.TempDir("", "bootstrap_test")
		Expect(err).NotTo(HaveOccurred())

		testDB, err = storage.Open(tmpDir)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		testDB.Close()
		err := testDB.Delete()
		Expect(err).Should(Succeed())
		os.RemoveAll(testDir)
	})

	It("can backup and restore a database", func() {
		id := "test1"

		err := testDB.Put("foo", 1, 1, []byte("Hello!"))
		Expect(err).NotTo(HaveOccurred())

		err, uploaded := bootstrap.Backup(testDB, backupURI, id, secret)
		Expect(err).NotTo(HaveOccurred())
		Expect(uploaded).To(BeTrue())

		restoreDir, err := ioutil.TempDir(testDir, "bootstrap_test")
		Expect(err).NotTo(HaveOccurred())

		// this is to help (although it can't guarantee) that GCS synchronizes between backup calls
		time.Sleep(5 * time.Second)

		err = bootstrap.Restore(restoreDir, restoreURI, id, secret)
		Expect(err).NotTo(HaveOccurred())

		db, err := storage.Open(restoreDir)
		Expect(err).Should(Succeed())

		ret, err := db.Get("foo", 1, 1)
		Expect(err).Should(Succeed())
		Expect(ret).Should(Equal([]byte("Hello!")))
		db.Close()
	}, 6)

	Context("backup", func() {

		It("should not replace a recent backup", func() {
			id := "test2"

			err, uploaded := bootstrap.Backup(testDB, backupURI, id, secret)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploaded).To(BeTrue())

			// this is to help (although it can't guarantee) that GCS synchronizes between backup calls
			time.Sleep(5 * time.Second)

			err, uploaded = bootstrap.Backup(testDB, backupURI, id, secret)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploaded).To(BeFalse())
		}, 6)

		It("should fail without id", func() {
			err, uploaded := bootstrap.Backup(testDB, backupURI, "", secret)
			Expect(err).To(HaveOccurred())
			Expect(uploaded).To(BeFalse())
		})

		It("should fail without secret", func() {
			err, uploaded := bootstrap.Backup(testDB, backupURI, "xxx", "")
			Expect(err).To(HaveOccurred())
			Expect(uploaded).To(BeFalse())
		})
	})

	Context("restore", func() {

		It("should fail without id", func() {
			err := bootstrap.Restore(testDir, backupURI, "", secret)
			Expect(err).To(HaveOccurred())
		})

		It("should fail without secret", func() {
			err := bootstrap.Restore(testDir, backupURI, "xxx", "")
			Expect(err).To(HaveOccurred())
		})

		It("should fail with 404 if backup is not present", func() {
			err := bootstrap.Restore(testDir, restoreURI, "not a real backup", secret)
			Expect(err).To(HaveOccurred())
			e, ok := err.(bootstrap.Error)
			Expect(ok).To(BeTrue())
			Expect(e.Status).To(Equal(404))
		})

		It("should fail with 403 if wrong secret", func() {
			id := "test3"

			err, uploaded := bootstrap.Backup(testDB, backupURI, id, secret)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploaded).To(BeTrue())

			// this is to help (although it can't guarantee) that GCS synchronizes between backup calls
			time.Sleep(5 * time.Second)

			err = bootstrap.Restore(testDir, restoreURI, id, "not the real secret")
			Expect(err).To(HaveOccurred())
			e, ok := err.(bootstrap.Error)
			Expect(ok).To(BeTrue())
			Expect(e.Status).To(Equal(403))
		}, 6)

	})
})
