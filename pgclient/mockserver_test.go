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

package pgclient

import (
	"database/sql"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Mock server tests", func() {
	var server *MockServer

	BeforeEach(func() {
		var err error

		server, err = NewMockServer(0)
		Expect(err).Should(Succeed())
	})

	AfterEach(func() {
		server.Stop()
	})

	It("Connect", func() {
		url := fmt.Sprintf("postgres://mock@%s/turtle", server.Address())
		db, err := sql.Open("transicator", url)
		Expect(err).Should(Succeed())
		err = db.Ping()
		Expect(err).Should(Succeed())
		err = db.Close()
		Expect(err).Should(Succeed())
	})

	It("Bad Exec", func() {
		url := fmt.Sprintf("postgres://mock@%s/turtle", server.Address())
		db, err := sql.Open("transicator", url)
		Expect(err).Should(Succeed())
		_, err = db.Exec("This is not sql")
		Expect(err).ShouldNot(Succeed())
		err = db.Close()
		Expect(err).Should(Succeed())
	})

	It("Insert", func() {
		url := fmt.Sprintf("postgres://mock@%s/turtle", server.Address())
		db, err := sql.Open("transicator", url)
		Expect(err).Should(Succeed())
		_, err = db.Exec("insert into mock values('foo', 'bar')")
		Expect(err).Should(Succeed())
		err = db.Close()
		Expect(err).Should(Succeed())
	})
})
