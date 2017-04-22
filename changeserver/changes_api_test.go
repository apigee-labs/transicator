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
package main

import (
	"fmt"
	"time"

	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	testTimeout  = 5 * time.Second
	testInterval = 250 * time.Millisecond
)

var emptySequenceStr = common.Sequence{}.String()

var _ = Describe("Changes API Tests", func() {
	var lastTestSequence = time.Now().Unix()
	lastChangeSequence := common.Sequence{}
	veryFirstSequence := common.Sequence{}

	It("Empty database", func() {
		for _, p := range []string{"scope", "selector"} {
			bod := executeGet(fmt.Sprintf(
				"%s/changes?%s=foo", baseURL, p))
			cl, err := common.UnmarshalChangeList(bod)
			Expect(err).Should(Succeed())
			Expect(cl.Changes).Should(BeEmpty())
			Expect(cl.FirstSequence).Should(Equal(emptySequenceStr))
			Expect(cl.LastSequence).Should(Equal(emptySequenceStr))
		}
	})

	It("First Change", func() {
		lastTestSequence++
		fmt.Fprintf(GinkgoWriter, "Inserting sequence %d\n", lastTestSequence)
		_, err := insertStmt.Exec(lastTestSequence, "foo")
		Expect(err).Should(Succeed())

		lastChangeSequenceCopy := lastChangeSequence
		for _, p := range []string{"scope", "selector"} {
			Eventually(func() bool {
				bod := executeGet(fmt.Sprintf(
					"%s/changes?since=%s&%s=foo", baseURL, lastChangeSequenceCopy, p))
				cl, err := common.UnmarshalChangeList(bod)
				Expect(err).Should(Succeed())
				if len(cl.Changes) == 0 {
					return false
				}
				for _, c := range cl.Changes {
					var newSeq int64
					err = c.NewRow.Get("sequence", &newSeq)
					Expect(err).Should(Succeed())
					if newSeq == lastTestSequence {
						Expect(c.Sequence).Should(Equal(c.GetSequence().String()))
						lastChangeSequence = c.GetSequence()
						veryFirstSequence = lastChangeSequence
						Expect(cl.FirstSequence).Should(Equal(lastChangeSequence.String()))
						Expect(cl.LastSequence).Should(Equal(lastChangeSequence.String()))
						return true
					}
				}
				fmt.Fprintf(GinkgoWriter, "Received %d changes last sequence = %s\n",
					len(cl.Changes), cl.Changes[len(cl.Changes)-1].NewRow["sequence"].Value)
				return false
			}, testTimeout, testInterval).Should(BeTrue())
		}
	})

	It("Different scope", func() {
		selectorColumn = "_different_selector"
		defer func() { selectorColumn = defaultSelectorColumn }()
		lastTestSequence++
		fmt.Fprintf(GinkgoWriter, "Insert alternative scope sequence %d\n", lastTestSequence)
		_, err := insertAlterativeSelectorStmt.Exec(lastTestSequence, "boop")
		Expect(err).Should(Succeed())

		lastChangeSequenceCopy := lastChangeSequence
		for _, p := range []string{"scope", "selector"} {
			Eventually(func() bool {
				bod := executeGet(fmt.Sprintf(
					"%s/changes?since=%s&%s=boop", baseURL, lastChangeSequenceCopy, p))
				cl, err := common.UnmarshalChangeList(bod)
				Expect(err).Should(Succeed())
				if len(cl.Changes) == 0 {
					return false
				}
				for _, c := range cl.Changes {
					var tableName string
					err = c.NewRow.Get("table", &tableName)
					Expect(err).Should(Succeed())
					if tableName == "public.changeserver_scope_test" {
						return true
					}
				}
				fmt.Fprintf(GinkgoWriter, "Received %d changes last sequence = %s\n",
					len(cl.Changes), cl.Changes[len(cl.Changes)-1].NewRow["sequence"].Value)
				return false
			})
		}
	})

	It("Next change", func() {
		lastTestSequence++
		fmt.Fprintf(GinkgoWriter, "Inserting sequence %d\n", lastTestSequence)
		_, err := insertStmt.Exec(lastTestSequence, "foo")
		Expect(err).Should(Succeed())

		lastChangeSequenceCopy := lastChangeSequence
		for _, p := range []string{"scope", "selector"} {
			Eventually(func() bool {
				cl := getChanges(fmt.Sprintf(
					"%s/changes?since=%s&%s=foo", baseURL, lastChangeSequenceCopy, p))
				if len(cl.Changes) < 1 {
					return false
				}
				if compareSequence(cl, 0, lastTestSequence) {
					lastChangeSequence = cl.Changes[0].GetSequence()
					Expect(cl.LastSequence).Should(Equal(lastChangeSequence.String()))
					Expect(cl.FirstSequence).Should(Equal(veryFirstSequence.String()))
					return true
				}
				return false
			}).Should(BeTrue())
		}
	})

	It("Snapshot filter", func() {
		lastTestSequence++
		fmt.Fprintf(GinkgoWriter, "Inserting sequence %d\n", lastTestSequence)
		_, err := insertStmt.Exec(lastTestSequence, "foo")
		Expect(err).Should(Succeed())

		tx, err := db.Begin()
		Expect(err).Should(Succeed())
		tst := tx.Stmt(insertStmt)
		defer tst.Close()
		lastTestSequence++
		fmt.Fprintf(GinkgoWriter, "Transactionally inserting sequence %d\n", lastTestSequence)
		_, err = tst.Exec(lastTestSequence, "foo")
		Expect(err).Should(Succeed())
		row := tx.QueryRow("select * from txid_current_snapshot()")
		var snap string
		err = row.Scan(&snap)
		Expect(err).Should(Succeed())
		err = tx.Commit()
		Expect(err).Should(Succeed())

		finalChangeSequence := lastChangeSequence

		lastChangeSequenceCopy := lastChangeSequence
		for _, p := range []string{"scope", "selector"} {
			// Without snapshot ID, should get two changes
			Eventually(func() bool {
				cl := getChanges(fmt.Sprintf(
					"%s/changes?since=%s&%s=foo", baseURL, lastChangeSequenceCopy, p))
				if len(cl.Changes) < 2 {
					return false
				}
				fmt.Fprintf(GinkgoWriter, "Seq 0: %s %d\n",
					cl.Changes[0].NewRow["sequence"].Value, cl.Changes[0].TransactionID)
				fmt.Fprintf(GinkgoWriter, "Seq 1: %s %d\n",
					cl.Changes[1].NewRow["sequence"].Value, cl.Changes[1].TransactionID)
				if compareSequence(cl, 0, lastTestSequence-1) &&
					compareSequence(cl, 1, lastTestSequence) {
					finalChangeSequence = cl.Changes[1].GetSequence()
					return true
				}
				return false
			}).Should(BeTrue())

			// With snapshot ID, should get only the second change
			Eventually(func() bool {
				fmt.Fprintf(GinkgoWriter, "Reading with snapshot %s\n", snap)
				cl := getChanges(fmt.Sprintf(
					"%s/changes?since=%s&%s=foo&snapshot=%s", baseURL, lastChangeSequenceCopy, p, snap))
				if len(cl.Changes) < 1 {
					return false
				}
				fmt.Fprintf(GinkgoWriter, "Seq 0: %s %d\n",
					cl.Changes[0].NewRow["sequence"].Value, cl.Changes[0].TransactionID)
				if compareSequence(cl, 0, lastTestSequence) {
					return true
				}
				return false
			}).Should(BeTrue())
		}
		lastChangeSequence = finalChangeSequence
	})

	It("Group of changes", func() {
		for i := 0; i <= 3; i++ {
			lastTestSequence++
			_, err := insertStmt.Exec(lastTestSequence, "foo")
			Expect(err).Should(Succeed())
		}
		fmt.Fprintf(GinkgoWriter, "Last inserted sequence %d\n", lastTestSequence)

		origLastSequence := lastChangeSequence

		for _, p := range []string{"scope", "selector"} {
			Eventually(func() bool {
				cl := getChanges(fmt.Sprintf(
					"%s/changes?since=%s&%s=foo", baseURL, origLastSequence, p))
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
				"%s/changes?since=%s&%s=bar", baseURL, origLastSequence, p))
			Expect(cl.Changes).Should(BeEmpty())

			// Out of range
			oorSeq := common.MakeSequence(lastChangeSequence.LSN+10, lastChangeSequence.Index)
			cl = getChanges(fmt.Sprintf(
				"%s/changes?since=%s&%s=foo", baseURL, oorSeq, p))
			Expect(cl.Changes).Should(BeEmpty())

			// Short limit
			cl = getChanges(fmt.Sprintf(
				"%s/changes?since=%s&%s=foo&limit=0", baseURL, origLastSequence, p))
			Expect(cl.Changes).Should(BeEmpty())

			// Less short limit
			cl = getChanges(fmt.Sprintf(
				"%s/changes?since=%s&%s=foo&limit=1", baseURL, origLastSequence, p))
			Expect(len(cl.Changes)).Should(Equal(1))
			Expect(compareSequence(cl, 0, lastTestSequence-3)).Should(BeTrue())

			// Over limit
			func() {
				limit := maxLimitChanges + 1
				u := fmt.Sprintf("%s/changes?since=%s&%s=foo&limit=%d", baseURL, origLastSequence, p, limit)
				req := createStandardRequest("GET", u, "application/json", nil)
				resp, err := http.DefaultClient.Do(req)
				Expect(err).Should(Succeed())
				defer resp.Body.Close()
				checkAPIErrorCode(resp, http.StatusBadRequest, "PARAMETER_INVALID")
			}()
		}

	})

	It("Last sequence inserted", func() {
		for i := 0; i < 3; i++ {
			lastTestSequence++
			_, err := insertStmt.Exec(lastTestSequence, "foo")
			Expect(err).Should(Succeed())
			lastTestSequence++
			_, err = insertStmt.Exec(lastTestSequence, "bar")
			Expect(err).Should(Succeed())
		}

		origLastSequence := lastChangeSequence
		var highestSequence string

		for _, p := range []string{"scope", "selector"} {
			Eventually(func() int {
				cl := getChanges(fmt.Sprintf(
					"%s/changes?since=%s&%s=bar", baseURL, origLastSequence, p))
				if len(cl.Changes) == 3 {
					lastChangeSequence = cl.Changes[2].GetSequence()
					highestSequence = cl.LastSequence
				}
				return len(cl.Changes)
			}).Should(Equal(3))

			// Now we have six new records, alternating in scope
			// Get them all and we should always get the same lastSequence
			cl := getChanges(fmt.Sprintf(
				"%s/changes?since=%s&%s=foo&%s=bar&limit=10", baseURL, origLastSequence, p, p))
			Expect(len(cl.Changes)).Should(Equal(6))
			Expect(cl.LastSequence).Should(Equal(highestSequence))

			cl = getChanges(fmt.Sprintf(
				"%s/changes?since=%s&%s=foo&limit=10", baseURL, origLastSequence, p))
			Expect(len(cl.Changes)).Should(Equal(3))
			Expect(cl.LastSequence).Should(Equal(highestSequence))

			cl = getChanges(fmt.Sprintf(
				"%s/changes?since=%s&%s=bar&limit=10", baseURL, origLastSequence, p))
			Expect(len(cl.Changes)).Should(Equal(3))
			Expect(cl.LastSequence).Should(Equal(highestSequence))

			// But if we run into the limit, then it's important that we return
			// only as far as we got with the query -- otherwise we'll drop stuff
			cl = getChanges(fmt.Sprintf(
				"%s/changes?since=%s&%s=foo&limit=2", baseURL, origLastSequence, p))
			Expect(len(cl.Changes)).Should(Equal(2))
			Expect(cl.LastSequence).Should(Equal(cl.Changes[1].Sequence))

			cl = getChanges(fmt.Sprintf(
				"%s/changes?since=%s&%s=bar&limit=2", baseURL, origLastSequence, p))
			Expect(len(cl.Changes)).Should(Equal(2))
			Expect(cl.LastSequence).Should(Equal(cl.Changes[1].Sequence))

			cl = getChanges(fmt.Sprintf(
				"%s/changes?since=%s&%s=foo&%s=bar&limit=5", baseURL, origLastSequence, p, p))
			Expect(len(cl.Changes)).Should(Equal(5))
			Expect(cl.LastSequence).Should(Equal(cl.Changes[4].Sequence))

			// Requests that return no data should return the highest change
			// index in the system to indicate that there is nothing to look for.
			cl = getChanges(fmt.Sprintf(
				"%s/changes?since=%s&%s=notfound", baseURL, origLastSequence, p))
			Expect(cl.Changes).Should(BeEmpty())
			Expect(cl.LastSequence).Should(Equal(highestSequence))

			cl = getChanges(fmt.Sprintf(
				"%s/changes?since=%s&%s=notfound&block1=1", baseURL, origLastSequence, p))
			Expect(cl.Changes).Should(BeEmpty())
			Expect(cl.LastSequence).Should(Equal(highestSequence))

			cl = getChanges(fmt.Sprintf(
				"%s/changes?since=%s&%s=foo&%s=bar", baseURL, highestSequence, p, p))
			Expect(cl.Changes).Should(BeEmpty())
			Expect(cl.LastSequence).Should(Equal(highestSequence))

			cl = getChanges(fmt.Sprintf(
				"%s/changes?since=%s&%s=foo&%s=bar&block=1", baseURL, highestSequence, p, p))
			Expect(cl.Changes).Should(BeEmpty())
			Expect(cl.LastSequence).Should(Equal(highestSequence))
		}
	})

	It("Long polling empty", func() {
		for _, p := range []string{"scope", "selector"} {
			lastCommit := lastChangeSequence
			cl := getChanges(fmt.Sprintf(
				"%s/changes?since=%s&%s=foo", baseURL, lastChangeSequence, p))
			if len(cl.Changes) > 0 {
				lastCommit = cl.Changes[len(cl.Changes)-1].GetSequence()
			}

			oorSeq := common.MakeSequence(lastCommit.LSN+10, lastCommit.Index)
			cl = getChanges(fmt.Sprintf(
				"%s/changes?since=%s&%s=foo&block=1", baseURL, oorSeq, p))
			Expect(cl.Changes).Should(BeEmpty())
		}
	})

	It("Long polling", func() {
		for _, p := range []string{"scope", "selector"} {
			resultChan := make(chan *common.ChangeList, 1)

			go func() {
				lc := getChanges(fmt.Sprintf(
					"%s/changes?since=%s&%s=foo", baseURL, lastChangeSequence, p))
				resultChan <- lc
				lc = getChanges(fmt.Sprintf(
					"%s/changes?since=%s&%s=foo&block=30", baseURL, lastChangeSequence, p))
				resultChan <- lc
			}()

			cl := <-resultChan
			Expect(cl.Changes).Should(BeEmpty())

			// Make sure that we don't get anything for a short time before we insert
			Consistently(resultChan, time.Second).ShouldNot(Receive())

			lastTestSequence++
			_, err := insertStmt.Exec(lastTestSequence, "foo")
			Expect(err).Should(Succeed())

			cl = <-resultChan
			Expect(len(cl.Changes)).Should(Equal(1))
			Expect(compareSequence(cl, 0, lastTestSequence)).Should(BeTrue())
			lastChangeSequence = cl.Changes[0].GetSequence()
		}
	})

	It("Long polling with snapshot", func() {
		for _, p := range []string{"scope", "selector"} {
			// Get current snapshot so we can find visibility
			row := db.QueryRow("select * from txid_current_snapshot()")
			var snapshot string
			err := row.Scan(&snapshot)
			Expect(err).Should(Succeed())

			resultChan := make(chan *common.ChangeList, 1)

			go func() {
				lc := getChanges(fmt.Sprintf(
					"%s/changes?snapshot=%s&%s=foo", baseURL, snapshot, p))
				resultChan <- lc
				lc = getChanges(fmt.Sprintf(
					"%s/changes?snapshot=%s&%s=foo&block=30", baseURL, snapshot, p))
				resultChan <- lc
			}()

			cl := <-resultChan
			Expect(cl.Changes).Should(BeEmpty())

			// Make sure that we don't get anything for a short time before we insert
			Consistently(resultChan, time.Second).ShouldNot(Receive())

			lastTestSequence++
			_, err = insertStmt.Exec(lastTestSequence, "foo")
			Expect(err).Should(Succeed())

			cl = <-resultChan
			Expect(len(cl.Changes)).Should(Equal(1))
			Expect(compareSequence(cl, 0, lastTestSequence)).Should(BeTrue())
			lastChangeSequence = cl.Changes[0].GetSequence()
		}
	})

	It("Long polling no selector", func() {
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
		_, err := insertStmt.Exec(lastTestSequence, "")
		Expect(err).Should(Succeed())

		cl = <-resultChan
		Expect(len(cl.Changes)).Should(Equal(1))
		Expect(compareSequence(cl, 0, lastTestSequence)).Should(BeTrue())
		lastChangeSequence = cl.Changes[0].GetSequence()
	})

	It("Long polling two selectors", func() {
		for _, p := range []string{"scope", "selector"} {
			resultChan := make(chan *common.ChangeList, 1)

			go func() {
				lc := getChanges(fmt.Sprintf(
					"%s/changes?since=%s&%s=foo&%s=bar", baseURL, lastChangeSequence, p, p))
				resultChan <- lc
				lc = getChanges(fmt.Sprintf(
					"%s/changes?since=%s&%s=foo&%s=bar&block=30", baseURL, lastChangeSequence, p, p))
				resultChan <- lc
			}()

			cl := <-resultChan
			Expect(cl.Changes).Should(BeEmpty())

			// Make sure that we don't get anything for a short time before we insert
			Consistently(resultChan, time.Second).ShouldNot(Receive())

			lastTestSequence++
			_, err := insertStmt.Exec(lastTestSequence, "bar")
			Expect(err).Should(Succeed())

			cl = <-resultChan
			Expect(len(cl.Changes)).Should(Equal(1))
			Expect(compareSequence(cl, 0, lastTestSequence)).Should(BeTrue())
			lastChangeSequence = cl.Changes[0].GetSequence()
		}
	})

	It("Cleanup", func() {
		for _, p := range []string{"scope", "selector"} {
			func() {
				testServer.startCleanup(5 * time.Second)
				defer func() {
					testServer.cleaner.stop()
					testServer.cleaner = nil
				}()

				// Insert new record here to ensure oldest sequence
				// returned by /changes API is NOT 0.0.0, which is
				// ignored for SNAPSHOT_TOO_OLD detection
				lastTestSequence++
				_, err := insertStmt.Exec(lastTestSequence, "clean")
				Expect(err).Should(Succeed())

				Eventually(func() bool {
					isDetected := false
					lc := getChanges(fmt.Sprintf(
						"%s/changes?%s=clean", baseURL, p))
					if len(lc.Changes) > 0 {
						// Check that initial change is newest result
						isDetected = compareSequence(lc, len(lc.Changes)-1, lastTestSequence)
						if isDetected {
							fmt.Fprintf(GinkgoWriter, "detected initial change lc: %+v\n", lc)
						}
					}
					return isDetected
				}, 2*time.Second, 200*time.Millisecond).Should(BeTrue())

				// Get oldest sequence and ensure we can still use it in API calls
				lc := getChanges(fmt.Sprintf(
					"%s/changes?%s=clean", baseURL, p))
				origFirstSequence := lc.FirstSequence
				fmt.Fprintf(GinkgoWriter, "lc: %+v\n", lc)
				fmt.Fprintf(GinkgoWriter, "origFirstSequence: %s\n", origFirstSequence)
				resp, err := http.Get(fmt.Sprintf(
					"%s/changes?%s=clean&since=%s", baseURL, p, origFirstSequence))
				Expect(err).Should(Succeed())
				resp.Body.Close()
				Expect(resp.StatusCode).Should(Equal(200))

				// Insert a new record
				lastTestSequence++
				_, err = insertStmt.Exec(lastTestSequence, "clean")
				Expect(err).Should(Succeed())
				fmt.Fprintf(GinkgoWriter, "added change lastTestSequnce: %+v\n", lastTestSequence)
				var newLast common.Sequence

				// Should be visible well before five seconds is up
				Eventually(func() bool {
					isDetected := false
					// query for new changes since oldest sequence
					lc := getChanges(fmt.Sprintf(
						"%s/changes?%s=clean&since=%s", baseURL, p, origFirstSequence))
					if len(lc.Changes) > 0 {
						// Check that added change is newest result
						isDetected = compareSequence(lc, len(lc.Changes)-1, lastTestSequence)
						if isDetected {
							fmt.Fprintf(GinkgoWriter, "detected added change lc: %+v\n", lc)
							newLast = lc.Changes[len(lc.Changes)-1].GetSequence()
						}
					}
					return isDetected
				}, 2*time.Second, 200*time.Millisecond).Should(BeTrue())

				defer func() {
					lastChangeSequence = newLast
				}()

				// Should start to get "old snapshot" error once
				// cleanup is triggered
				Eventually(func() bool {
					u := fmt.Sprintf("%s/changes?%s=clean&since=%s", baseURL, p, origFirstSequence)
					req := createStandardRequest("GET", u, "application/json", nil)
					resp, err := http.DefaultClient.Do(req)
					Expect(err).Should(Succeed())
					defer resp.Body.Close()
					dump, err := httputil.DumpResponse(resp, true)
					fmt.Fprintf(GinkgoWriter, "dump client response: %s\nerr: %+v\n", dump, err)
					if resp.StatusCode == 400 {
						fmt.Fprintln(GinkgoWriter, "detected SNAPSHOT_TOO_OLD")
						verifyErrorCode(resp.Body, "SNAPSHOT_TOO_OLD")
						return true
					}
					return false
				}, 7*time.Second, 500*time.Millisecond).Should(BeTrue())

				// All records should be deleted before 10 seconds is up
				Eventually(func() int {
					lcc := getChanges(fmt.Sprintf(
						"%s/changes?%s=clean", baseURL, p))
					return len(lcc.Changes)
				}, 10*time.Second, time.Second).Should(BeZero())

				// Even after a full cleanup to an empty database, we should have
				// remembered the last sequence that we got so that clients know
				// whether they are up to date.
				lastCl := getChanges(fmt.Sprintf(
					"%s/changes?%s=clean", baseURL, p))
				Expect(lastCl.FirstSequence).Should(Equal(newLast.String()))
				Expect(lastCl.LastSequence).Should(Equal(newLast.String()))
			}()
		}
	})

	It("should detect invalid chars in scope query param", func() {
		for _, p := range []string{"scope", "selector"} {
			func() {
				invalidScope := url.QueryEscape("snaptests;select now()")
				u := fmt.Sprintf("%s/changes?%s=%s", baseURL, p, invalidScope)
				req := createStandardRequest("GET", u, "application/json", nil)
				resp, err := http.DefaultClient.Do(req)
				Expect(err).Should(Succeed())
				defer resp.Body.Close()
				checkAPIErrorCode(resp, http.StatusBadRequest, "PARAMETER_INVALID")
			}()
		}
	})

	It("should detect invalid chars in multiple scope query params", func() {
		for _, p := range []string{"scope", "selector"} {
			func() {
				invalidScope := url.QueryEscape("snaptests;select now()")
				u := fmt.Sprintf("%s/changes?%s=abc123&%s=%s", baseURL, p, p, invalidScope)
				req := createStandardRequest("GET", u, "application/json", nil)
				resp, err := http.DefaultClient.Do(req)
				Expect(err).Should(Succeed())
				defer resp.Body.Close()
				checkAPIErrorCode(resp, http.StatusBadRequest, "PARAMETER_INVALID")
			}()
		}
	})
})

func createStandardRequest(method string, urlStr string, acceptHdr string, body io.Reader) *http.Request {
	req, err := http.NewRequest(method, urlStr, body)
	Expect(err).Should(Succeed())
	req.Header = http.Header{}
	req.Header.Set("Accept", acceptHdr)
	dump, err := httputil.DumpRequestOut(req, true)
	fmt.Fprintf(GinkgoWriter, "\ndump client req: %q\nerr: %+v\n", dump, err)
	return req
}

func getChanges(url string) *common.ChangeList {
	fmt.Fprintf(GinkgoWriter, "URL: %s\n", url)
	bod := executeGet(url)
	fmt.Fprintf(GinkgoWriter, "%s\n", string(bod))
	cl, err := common.UnmarshalChangeList(bod)
	Expect(err).Should(Succeed())
	fmt.Fprintf(GinkgoWriter, "Num changes: %d\n", len(cl.Changes))
	return cl
}

func checkAPIErrorCode(resp *http.Response, sc int, ec string) {
	dump, err := httputil.DumpResponse(resp, true)
	fmt.Fprintf(GinkgoWriter, "\nAPIError response: %s\nerr: %+v\n", dump, err)

	Expect(resp.StatusCode).Should(Equal(sc))
	Expect(resp.Header.Get("Content-Type")).Should(Equal("application/json"))

	bod, err := ioutil.ReadAll(resp.Body)
	Expect(err).Should(Succeed())

	var errMsg common.APIError
	err = json.Unmarshal(bod, &errMsg)
	Expect(err).Should(Succeed())
	Expect(errMsg.Code).Should(Equal(ec))
}

func compareSequence(cl *common.ChangeList, index int, lts int64) bool {
	var newSeq int64
	err := cl.Changes[index].NewRow.Get("sequence", &newSeq)
	Expect(err).Should(Succeed())
	fmt.Fprintf(GinkgoWriter, "New seq = %d last = %d orig = %v\n", newSeq, lts, cl.Changes[index].NewRow["sequence"])
	return newSeq == lts
}

func verifyErrorCode(in io.Reader, code string) {
	bod, err := ioutil.ReadAll(in)
	Expect(err).Should(Succeed())

	msg := make(map[string]string)
	err = json.Unmarshal(bod, &msg)
	Expect(err).Should(Succeed())
	Expect(msg["code"]).Should(Equal(code))
}
