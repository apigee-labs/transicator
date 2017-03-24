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
package pgclient

import (
	"database/sql"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Connection Tests", func() {
	var mock *MockServer

	BeforeEach(func() {
		mock = NewMockServer()
	})

	AfterEach(func() {
		mock.Stop()
	})

	It("Trusted Connect", func() {
		mock.Start(0)
		tryConnect(fmt.Sprintf("postgres://mock@%s/turtle", mock.Address()))
		// Connect with TLS required should fail
		failToConnect(fmt.Sprintf("postgres://mock@%s/turtle?ssl=true", mock.Address()))
	})

	It("Connect to bad host", func() {
		failToConnect("postgres://badhost:9999/postgres")
	})

	It("Connect to wrong database", func() {
		mock.Start(0)
		failToConnect(fmt.Sprintf("postgres://mock@%s/wrongdatabase", mock.Address()))
	})

	It("Cleartext Connect", func() {
		mock.SetAuthType(MockClear)
		mock.Start(0)
		tryConnect(fmt.Sprintf("postgres://mock:mocketty@%s/turtle", mock.Address()))
		failToConnect(fmt.Sprintf("postgres://mock@%s/turtle", mock.Address()))
		failToConnect(fmt.Sprintf("postgres://mock:notthepassword@%s/turtle", mock.Address()))
	})

	It("MD5 Connect", func() {
		mock.SetAuthType(MockMD5)
		mock.Start(0)
		tryConnect(fmt.Sprintf("postgres://mock:mocketty@%s/turtle", mock.Address()))
		failToConnect(fmt.Sprintf("postgres://mock@%s/turtle", mock.Address()))
		failToConnect(fmt.Sprintf("postgres://mock:notthepassword@%s/turtle", mock.Address()))
	})

	It("Connect to non-TLS server", func() {
		mock.SetAuthType(MockMD5)
		err := mock.Start(0)
		Expect(err).Should(Succeed())

		// disable -- should work
		tryConnect(fmt.Sprintf("postgres://mock:mocketty@%s/turtle?sslmode=disable", mock.Address()))
		// allow -- should work
		tryConnect(fmt.Sprintf("postgres://mock:mocketty@%s/turtle?sslmode=allow", mock.Address()))
		// prefer -- should still work
		tryConnect(fmt.Sprintf("postgres://mock:mocketty@%s/turtle?sslmode=prefer", mock.Address()))
		// require -- should fail
		failToConnect(fmt.Sprintf("postgres://mock:mocketty@%s/turtle?sslmode=require", mock.Address()))
	})

	It("Connect to TLS server", func() {
		mock.SetAuthType(MockMD5)
		mock.SetTLSInfo("../test/keys/clearcert.pem", "../test/keys/clearkey.pem")
		err := mock.Start(0)
		Expect(err).Should(Succeed())

		// disable -- should work because server doesn't care
		tryConnect(fmt.Sprintf("postgres://mock:mocketty@%s/turtle?sslmode=disable", mock.Address()))
		// allow -- should work
		tryConnect(fmt.Sprintf("postgres://mock:mocketty@%s/turtle?sslmode=allow", mock.Address()))
		// prefer -- should still work
		tryConnect(fmt.Sprintf("postgres://mock:mocketty@%s/turtle?sslmode=prefer", mock.Address()))
		// require -- should work
		tryConnect(fmt.Sprintf("postgres://mock:mocketty@%s/turtle?sslmode=require", mock.Address()))
	})

	It("Connect to force TLS server", func() {
		mock.SetAuthType(MockMD5)
		mock.SetTLSInfo("../test/keys/clearcert.pem", "../test/keys/clearkey.pem")
		mock.SetForceTLS()
		err := mock.Start(0)
		Expect(err).Should(Succeed())

		// disable -- should fail
		failToConnect(fmt.Sprintf("postgres://mock:mocketty@%s/turtle?sslmode=disable", mock.Address()))
		// allow -- should work
		tryConnect(fmt.Sprintf("postgres://mock:mocketty@%s/turtle?sslmode=allow", mock.Address()))
		// prefer -- should still work
		tryConnect(fmt.Sprintf("postgres://mock:mocketty@%s/turtle?sslmode=prefer", mock.Address()))
		// require -- should work
		tryConnect(fmt.Sprintf("postgres://mock:mocketty@%s/turtle?sslmode=require", mock.Address()))
	})
})

func tryConnect(url string) {
	db, err := sql.Open("transicator", url)
	Expect(err).Should(Succeed())
	err = db.Ping()
	Expect(err).Should(Succeed())
	err = db.Close()
	Expect(err).Should(Succeed())
}

func failToConnect(url string) {
	db, err := sql.Open("transicator", url)
	Expect(err).Should(Succeed())
	err = db.Ping()
	Expect(err).ShouldNot(Succeed())
	err = db.Close()
	Expect(err).Should(Succeed())
}
