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
package test

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var insecureClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	},
}

var _ = Describe("Combined tests", func() {
	It("Check parameters", func() {
		resp, err := insecureClient.Get(changeBase)
		Expect(err).Should(Succeed())
		resp.Body.Close()
		Expect(resp.StatusCode).Should(Equal(404))

		resp, err = insecureClient.Get(snapshotBase)
		Expect(err).Should(Succeed())
		resp.Body.Close()
		Expect(resp.StatusCode).Should(Equal(404))
	})

	It("Combined test", func() {
		// Insert some data to PG
		insert, err := db.Prepare("insert into combined_test (id, value, _change_selector) values ($1, $2, $3)")
		Expect(err).Should(Succeed())
		defer insert.Close()

		_, err = insert.Exec(1, "one", "scope1")
		Expect(err).Should(Succeed())
		_, err = insert.Exec(2, "two", "scope2")
		Expect(err).Should(Succeed())
		_, err = insert.Exec(3, "three", "scope1")
		Expect(err).Should(Succeed())

		// Take a snapshot. Specify the streaming protobuf format.
		// We will get a 303 and automatically follow the redirect
		url := fmt.Sprintf("%s/snapshots?scope=scope1", snapshotBase)
		fmt.Fprintf(GinkgoWriter, "GET %s\n", url)
		req, err := http.NewRequest("GET", url, nil)
		Expect(err).Should(Succeed())
		req.Header.Add("Accept", "application/transicator+protobuf")
		resp, err := insecureClient.Do(req)
		Expect(err).Should(Succeed())
		Expect(resp.StatusCode).Should(Equal(200))
		fmt.Fprintf(GinkgoWriter, "Response: %s\n", resp.Header.Get("Content-Type"))
		fmt.Fprintf(GinkgoWriter, "Response: %s\n", resp.Header.Get("Content-Length"))

		sr, err := common.CreateSnapshotReader(resp.Body)
		Expect(err).Should(Succeed())
		defer resp.Body.Close()

		// Verify the snapshot. Don't sweat about other tables to make tests easier.
		foundTable := false
		rowCount := 0
		tableName := ""

		for sr.Next() {
			n := sr.Entry()
			switch n.(type) {
			case common.TableInfo:
				table := n.(common.TableInfo)
				fmt.Fprintf(GinkgoWriter, "Table %s\n", table.Name)
				tableName = table.Name
			case common.Row:
				if tableName == "public.combined_test" {
					row := n.(common.Row)
					foundTable = true
					var id int
					var value string
					err = row.Get("id", &id)
					Expect(err).Should(Succeed())
					err = row.Get("value", &value)
					Expect(err).Should(Succeed())

					switch rowCount {
					case 0:
						Expect(id).Should(Equal(1))
						Expect(value).Should(Equal("one"))
					case 1:
						Expect(id).Should(Equal(3))
						Expect(value).Should(Equal("three"))
					default:
						Expect(rowCount).Should(BeNumerically("<", 2))
					}
					rowCount++
				}
			case error:
				Expect(n.(error)).Should(Succeed())
			default:
				Expect("Unexpected default type").Should(BeEmpty())
			}
		}
		Expect(foundTable).Should(BeTrue())

		// Check for changes. There should be none.
		changes := getChanges(fmt.Sprintf("snapshot=%s&scope=scope1",
			sr.SnapshotInfo()), 0)
		Expect(changes.Changes).Should(BeEmpty())

		// Insert some more data
		_, err = insert.Exec(4, "four", "scope1")
		Expect(err).Should(Succeed())

		// Verify the changes
		changes = getChanges(fmt.Sprintf("snapshot=%s&scope=scope1&since=%s&block=5",
			sr.SnapshotInfo(), changes.LastSequence), 1)
		Expect(changes.Changes[0].NewRow["id"]).ShouldNot(BeNil())
		Expect(changes.Changes[0].NewRow["id"].Value).Should(Equal("4"))

		// Do a delete just for kicks
		result, err := db.Exec("delete from combined_test where id = 1")
		Expect(err).Should(Succeed())
		Expect(result.RowsAffected()).Should(BeEquivalentTo(1))

		changes = getChanges(fmt.Sprintf("snapshot=%s&scope=scope1&since=%s&block=5",
			sr.SnapshotInfo(), changes.LastSequence), 1)
		Expect(changes.Changes[0].OldRow["id"]).ShouldNot(BeNil())
		Expect(changes.Changes[0].OldRow["id"].Value).Should(Equal("1"))
		Expect(changes.Changes[0].OldRow["value"].Value).Should(Equal("one"))
	})
})

func getChanges(qs string, numExpected int) *common.ChangeList {
	url := fmt.Sprintf("%s/changes?%s", changeBase, qs)
	fmt.Fprintf(GinkgoWriter, "GET %s\n", url)
	var ret *common.ChangeList

	resp, err := insecureClient.Get(url)
	Expect(err).Should(Succeed())
	Expect(resp.StatusCode).Should(Equal(200))

	changesBuf, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	Expect(err).Should(Succeed())
	ret, err = common.UnmarshalChangeList(changesBuf)
	Expect(err).Should(Succeed())
	Expect(len(ret.Changes)).Should(Equal(numExpected))
	return ret
}
