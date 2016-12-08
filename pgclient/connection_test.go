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
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Connection Tests", func() {
	It("Basic Connect", func() {
		if dbURL == "" {
			return
		}
		conn, err := Connect(dbURL)
		Expect(err).Should(Succeed())
		Expect(conn).ShouldNot(BeNil())
		conn.Close()
	})

	It("Connect to bad host", func() {
		_, err := Connect("postgres://badhost:9999/postgres")
		Expect(err).ShouldNot(Succeed())
	})

	It("Connect to bad database", func() {
		if dbURL == "" {
			return
		}
		_, err := Connect("postgres://postgres@localhost/baddatabase")
		Expect(err).ShouldNot(Succeed())
		fmt.Fprintf(GinkgoWriter, "Error from database: %s\n", err)
	})

	PIt("Basic Connect with SSL", func() {
		if dbURL == "" {
			return
		}
		conn, err := Connect(dbURL + "?ssl=true")
		Expect(err).Should(Succeed())
		Expect(conn).ShouldNot(BeNil())
		conn.Close()
	})
})
