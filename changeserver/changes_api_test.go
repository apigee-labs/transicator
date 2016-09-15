package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/30x/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	testTimeout  = 5 * time.Second
	testInterval = 250 * time.Millisecond
)

var _ = Describe("Changes API Tests", func() {
	var lastTestSequence = time.Now().Unix()
	lastChangeSequence := common.Sequence{}

	It("First Change", func() {
		lastTestSequence++
		fmt.Fprintf(GinkgoWriter, "Inserting sequence %d\n", lastTestSequence)
		executeSQL(fmt.Sprintf(
			"insert into changeserver_test (sequence, _apid_scope) values(%d, 'foo')",
			lastTestSequence))

		Eventually(func() bool {
			bod := executeGet(fmt.Sprintf(
				"%s/changes?since=%s&scope=foo", baseURL, lastChangeSequence))
			cl, err := common.UnmarshalChangeList(bod)
			Expect(err).Should(Succeed())
			if len(cl.Changes) == 0 {
				return false
			}
			for _, c := range cl.Changes {
				ltss := strconv.FormatInt(lastTestSequence, 10)
				if c.NewRow["sequence"].Value == ltss {
					Expect(c.Sequence).Should(Equal(c.GetSequence().String()))
					lastChangeSequence = c.GetSequence()
					return true
				}
			}
			fmt.Fprintf(GinkgoWriter, "Received %d changes last sequence = %s\n",
				len(cl.Changes), cl.Changes[len(cl.Changes)-1].NewRow["sequence"].Value)
			return false
		}, testTimeout, testInterval).Should(BeTrue())
	})

	It("Next change", func() {
		lastTestSequence++
		fmt.Fprintf(GinkgoWriter, "Inserting sequence %d\n", lastTestSequence)
		executeSQL(fmt.Sprintf(
			"insert into changeserver_test (sequence, _apid_scope) values(%d, 'foo')",
			lastTestSequence))

		Eventually(func() bool {
			cl := getChanges(fmt.Sprintf(
				"%s/changes?since=%s&scope=foo", baseURL, lastChangeSequence))
			if len(cl.Changes) < 1 {
				return false
			}
			if compareSequence(cl, 0, lastTestSequence) {
				lastChangeSequence = cl.Changes[0].GetSequence()
				return true
			}
			return false
		}).Should(BeTrue())
	})

	It("Group of changes", func() {
		for i := 0; i <= 3; i++ {
			lastTestSequence++
			executeSQL(fmt.Sprintf(
				"insert into changeserver_test (sequence, _apid_scope) values(%d, 'foo')",
				lastTestSequence))
		}
		fmt.Fprintf(GinkgoWriter, "Last inserted sequence %d\n", lastTestSequence)

		origLastSequence := lastChangeSequence

		Eventually(func() bool {
			cl := getChanges(fmt.Sprintf(
				"%s/changes?since=%s&scope=foo", baseURL, origLastSequence))
			if len(cl.Changes) != 4 {
				return false
			}
			if compareSequence(cl, 3, lastTestSequence) {
				lastChangeSequence = cl.Changes[3].GetSequence()
				return true
			}
			return false
		}).Should(BeTrue())

		// Un-matched scope
		cl := getChanges(fmt.Sprintf(
			"%s/changes?since=%s&scope=bar", baseURL, origLastSequence))
		Expect(cl.Changes).Should(BeEmpty())

		// Out of range
		oorSeq := common.MakeSequence(lastChangeSequence.LSN+10, lastChangeSequence.Index)
		cl = getChanges(fmt.Sprintf(
			"%s/changes?since=%s&scope=foo", baseURL, oorSeq))
		Expect(cl.Changes).Should(BeEmpty())

		// Short limit
		cl = getChanges(fmt.Sprintf(
			"%s/changes?since=%s&scope=foo&limit=0", baseURL, origLastSequence))
		Expect(cl.Changes).Should(BeEmpty())

		// Less short limit
		cl = getChanges(fmt.Sprintf(
			"%s/changes?since=%s&scope=foo&limit=1", baseURL, origLastSequence))
		Expect(len(cl.Changes)).Should(Equal(1))
		Expect(compareSequence(cl, 0, lastTestSequence-3)).Should(BeTrue())
	})

	It("Long polling empty", func() {
		lastCommit := lastChangeSequence
		cl := getChanges(fmt.Sprintf(
			"%s/changes?since=%s&scope=foo", baseURL, lastChangeSequence))
		if len(cl.Changes) > 0 {
			lastCommit = cl.Changes[len(cl.Changes)-1].GetSequence()
		}

		oorSeq := common.MakeSequence(lastCommit.LSN+10, lastCommit.Index)
		cl = getChanges(fmt.Sprintf(
			"%s/changes?since=%s&scope=foo&block=1", baseURL, oorSeq))
		Expect(cl.Changes).Should(BeEmpty())
	})

	It("Long polling", func() {
		resultChan := make(chan *common.ChangeList, 1)

		go func() {
			lc := getChanges(fmt.Sprintf(
				"%s/changes?since=%s&scope=foo", baseURL, lastChangeSequence))
			resultChan <- lc
			lc = getChanges(fmt.Sprintf(
				"%s/changes?since=%s&scope=foo&block=30", baseURL, lastChangeSequence))
			resultChan <- lc
		}()

		cl := <-resultChan
		Expect(cl.Changes).Should(BeEmpty())

		lastTestSequence++
		executeSQL(fmt.Sprintf(
			"insert into changeserver_test (sequence, _apid_scope) values(%d, 'foo')",
			lastTestSequence))

		cl = <-resultChan
		Expect(len(cl.Changes)).Should(Equal(1))
		Expect(compareSequence(cl, 0, lastTestSequence)).Should(BeTrue())
		lastChangeSequence = cl.Changes[0].GetSequence()
	})

	It("Long polling no scope", func() {
		resultChan := make(chan *common.ChangeList, 1)

		go func() {
			lc := getChanges(fmt.Sprintf(
				"%s/changes?since=%s", baseURL, lastChangeSequence))
			resultChan <- lc
			lc = getChanges(fmt.Sprintf(
				"%s/changes?since=%s&block=30", baseURL, lastChangeSequence))
			resultChan <- lc
		}()

		cl := <-resultChan
		Expect(cl.Changes).Should(BeEmpty())

		lastTestSequence++
		executeSQL(fmt.Sprintf(
			"insert into changeserver_test (sequence, _apid_scope) values(%d, '')",
			lastTestSequence))

		cl = <-resultChan
		Expect(len(cl.Changes)).Should(Equal(1))
		Expect(compareSequence(cl, 0, lastTestSequence)).Should(BeTrue())
		lastChangeSequence = cl.Changes[0].GetSequence()
	})

	It("Long polling two scopes", func() {
		resultChan := make(chan *common.ChangeList, 1)

		go func() {
			lc := getChanges(fmt.Sprintf(
				"%s/changes?since=%s&scope=foo&scope=bar", baseURL, lastChangeSequence))
			resultChan <- lc
			lc = getChanges(fmt.Sprintf(
				"%s/changes?since=%s&scope=foo&scope=bar&block=30", baseURL, lastChangeSequence))
			resultChan <- lc
		}()

		cl := <-resultChan
		Expect(cl.Changes).Should(BeEmpty())

		lastTestSequence++
		executeSQL(fmt.Sprintf(
			"insert into changeserver_test (sequence, _apid_scope) values(%d, 'bar')",
			lastTestSequence))

		cl = <-resultChan
		Expect(len(cl.Changes)).Should(Equal(1))
		Expect(compareSequence(cl, 0, lastTestSequence)).Should(BeTrue())
		lastChangeSequence = cl.Changes[0].GetSequence()
	})
})

func getChanges(url string) *common.ChangeList {
	fmt.Fprintf(GinkgoWriter, "URL: %s\n", url)
	bod := executeGet(url)
	cl, err := common.UnmarshalChangeList(bod)
	Expect(err).Should(Succeed())
	fmt.Fprintf(GinkgoWriter, "Num changes: %d\n", len(cl.Changes))
	return cl
}

func compareSequence(cl *common.ChangeList, index int, lts int64) bool {
	ltss := strconv.FormatInt(lts, 10)
	return cl.Changes[index].NewRow["sequence"].Value == ltss
}
