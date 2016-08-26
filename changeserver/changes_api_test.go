package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/30x/transicator/replication"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Changes API Tests", func() {
	var lastTestSequence = time.Now().Unix()
	var lastChangeSequence int64

	It("First Change", func() {
		lastTestSequence++
		fmt.Fprintf(GinkgoWriter, "Inserting sequence %d\n", lastTestSequence)
		executeSQL(fmt.Sprintf(
			"insert into changeserver_test (sequence, tag) values(%d, 'foo')",
			lastTestSequence))

		Eventually(func() bool {
			bod := executeGet(fmt.Sprintf(
				"%s/changes?since=%d&tag=foo", baseURL, lastChangeSequence))
			var changes []replication.EncodedChange
			err := json.Unmarshal(bod, &changes)
			Expect(err).Should(Succeed())
			for _, c := range changes {
				if c.New["sequence"] == float64(lastTestSequence) {
					lastChangeSequence = c.CommitSequence
					return true
				}
			}
			fmt.Fprintf(GinkgoWriter, "Received %d changes last sequence = %v\n",
				len(changes), changes[len(changes)-1].New["sequence"])
			return false
		}).Should(BeTrue())
	})

	It("Next change", func() {
		lastTestSequence++
		fmt.Fprintf(GinkgoWriter, "Inserting sequence %d\n", lastTestSequence)
		executeSQL(fmt.Sprintf(
			"insert into changeserver_test (sequence, tag) values(%d, 'foo')",
			lastTestSequence))

		Eventually(func() bool {
			changes := getChanges(fmt.Sprintf(
				"%s/changes?since=%d&tag=foo", baseURL, lastChangeSequence))
			if len(changes) < 1 {
				return false
			}
			if changes[0].New["sequence"] == float64(lastTestSequence) {
				lastChangeSequence = changes[0].CommitSequence
				return true
			}
			return false
		}).Should(BeTrue())
	})

	It("Group of changes", func() {
		for i := 0; i <= 3; i++ {
			lastTestSequence++
			executeSQL(fmt.Sprintf(
				"insert into changeserver_test (sequence, tag) values(%d, 'foo')",
				lastTestSequence))
		}
		fmt.Fprintf(GinkgoWriter, "Last inserted sequence %d\n", lastTestSequence)

		origLastSequence := lastChangeSequence

		Eventually(func() bool {
			changes := getChanges(fmt.Sprintf(
				"%s/changes?since=%d&tag=foo", baseURL, origLastSequence))
			if len(changes) != 4 {
				return false
			}
			if changes[3].New["sequence"] == float64(lastTestSequence) {
				lastChangeSequence = changes[3].CommitSequence
				return true
			}
			return false
		}).Should(BeTrue())

		// Un-matched tag
		changes := getChanges(fmt.Sprintf(
			"%s/changes?since=%d&tag=bar", baseURL, origLastSequence))
		Expect(changes).Should(BeEmpty())

		// Out of range
		changes = getChanges(fmt.Sprintf(
			"%s/changes?since=%d&tag=foo", baseURL, lastChangeSequence+10))
		Expect(changes).Should(BeEmpty())

		// Short limit
		changes = getChanges(fmt.Sprintf(
			"%s/changes?since=%d&tag=foo&limit=0", baseURL, origLastSequence))
		Expect(changes).Should(BeEmpty())

		// Less short limit
		changes = getChanges(fmt.Sprintf(
			"%s/changes?since=%d&tag=foo&limit=1", baseURL, origLastSequence))
		Expect(len(changes)).Should(Equal(1))
		Expect(changes[0].New["sequence"]).Should(BeEquivalentTo(lastTestSequence - 3))
	})
})

func getChanges(url string) []replication.EncodedChange {
	fmt.Fprintf(GinkgoWriter, "URL: %s\n", url)
	bod := executeGet(url)
	var changes []replication.EncodedChange
	err := json.Unmarshal(bod, &changes)
	Expect(err).Should(Succeed())
	fmt.Fprintf(GinkgoWriter, "Num changes: %d\n", len(changes))
	return changes
}
