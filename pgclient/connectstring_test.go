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
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Connect string", func() {
	It("Empty URI", func() {
		i, err := parseConnectString("postgresql://")
		Expect(err).Should(Succeed())
		Expect(i.host).Should(Equal("localhost"))
		Expect(i.port).Should(Equal(5432))
		Expect(i.database).Should(Equal("postgres"))
		Expect(i.user).Should(BeEmpty())
		Expect(i.creds).Should(BeEmpty())
		Expect(i.options).Should(BeEmpty())
	})

	It("Empty URI Postgres", func() {
		i, err := parseConnectString("postgres://")
		Expect(err).Should(Succeed())
		Expect(i.host).Should(Equal("localhost"))
		Expect(i.port).Should(Equal(5432))
		Expect(i.database).Should(Equal("postgres"))
		Expect(i.user).Should(BeEmpty())
		Expect(i.creds).Should(BeEmpty())
		Expect(i.options).Should(BeEmpty())
	})

	It("Host only", func() {
		i, err := parseConnectString("postgresql://localhost")
		Expect(err).Should(Succeed())
		Expect(i.host).Should(Equal("localhost"))
		Expect(i.port).Should(Equal(5432))
		Expect(i.database).Should(Equal("postgres"))
		Expect(i.user).Should(BeEmpty())
		Expect(i.creds).Should(BeEmpty())
		Expect(i.options).Should(BeEmpty())
	})

	It("Host Port", func() {
		i, err := parseConnectString("postgresql://myhost:5433")
		Expect(err).Should(Succeed())
		Expect(i.host).Should(Equal("myhost"))
		Expect(i.port).Should(Equal(5433))
		Expect(i.database).Should(Equal("postgres"))
		Expect(i.user).Should(BeEmpty())
		Expect(i.creds).Should(BeEmpty())
		Expect(i.options).Should(BeEmpty())
		Expect(i.ssl).ShouldNot(BeTrue())
	})

	It("Database", func() {
		i, err := parseConnectString("postgresql://localhost/mydb")
		Expect(err).Should(Succeed())
		Expect(i.host).Should(Equal("localhost"))
		Expect(i.port).Should(Equal(5432))
		Expect(i.database).Should(Equal("mydb"))
		Expect(i.user).Should(BeEmpty())
		Expect(i.creds).Should(BeEmpty())
		Expect(i.options).Should(BeEmpty())
	})

	It("User", func() {
		i, err := parseConnectString("postgresql://user@localhost")
		Expect(err).Should(Succeed())
		Expect(i.host).Should(Equal("localhost"))
		Expect(i.port).Should(Equal(5432))
		Expect(i.database).Should(Equal("postgres"))
		Expect(i.user).Should(Equal("user"))
		Expect(i.creds).Should(BeEmpty())
		Expect(i.options).Should(BeEmpty())
	})

	It("User and PW params", func() {
		i, err := parseConnectString("postgresql://localhost?user=foo&password=bar")
		Expect(err).Should(Succeed())
		Expect(i.host).Should(Equal("localhost"))
		Expect(i.port).Should(Equal(5432))
		Expect(i.database).Should(Equal("postgres"))
		Expect(i.user).Should(Equal("foo"))
		Expect(i.creds).Should(Equal("bar"))
		Expect(i.options).Should(BeEmpty())
	})

	It("User Host", func() {
		i, err := parseConnectString("postgresql://user:secret@myhost")
		Expect(err).Should(Succeed())
		Expect(i.host).Should(Equal("myhost"))
		Expect(i.port).Should(Equal(5432))
		Expect(i.database).Should(Equal("postgres"))
		Expect(i.user).Should(Equal("user"))
		Expect(i.creds).Should(Equal("secret"))
		Expect(i.options).Should(BeEmpty())
	})

	It("User Host Port", func() {
		i, err := parseConnectString("postgresql://user:secret@myhost:9999")
		Expect(err).Should(Succeed())
		Expect(i.host).Should(Equal("myhost"))
		Expect(i.port).Should(Equal(9999))
		Expect(i.database).Should(Equal("postgres"))
		Expect(i.user).Should(Equal("user"))
		Expect(i.creds).Should(Equal("secret"))
		Expect(i.options).Should(BeEmpty())
	})

	It("User Host Port SSL", func() {
		i, err := parseConnectString("postgresql://user:secret@myhost:9999?ssl=true")
		Expect(err).Should(Succeed())
		Expect(i.host).Should(Equal("myhost"))
		Expect(i.port).Should(Equal(9999))
		Expect(i.database).Should(Equal("postgres"))
		Expect(i.user).Should(Equal("user"))
		Expect(i.creds).Should(Equal("secret"))
		Expect(i.ssl).Should(BeTrue())
		Expect(i.options).Should(BeEmpty())
	})

	It("User Host Port Override", func() {
		i, err := parseConnectString("postgresql://user:secret@myhost:9999?user=foo&password=bar")
		Expect(err).Should(Succeed())
		Expect(i.host).Should(Equal("myhost"))
		Expect(i.port).Should(Equal(9999))
		Expect(i.database).Should(Equal("postgres"))
		Expect(i.user).Should(Equal("foo"))
		Expect(i.creds).Should(Equal("bar"))
		Expect(i.options).Should(BeEmpty())
	})

	It("Params", func() {
		i, err := parseConnectString("postgres://other@otherhost/otherdb?connect_timeout=10&application_name=myapp&ssl=true")
		Expect(err).Should(Succeed())
		Expect(i.host).Should(Equal("otherhost"))
		Expect(i.port).Should(Equal(5432))
		Expect(i.database).Should(Equal("otherdb"))
		Expect(i.user).Should(Equal("other"))
		Expect(i.creds).Should(BeEmpty())
		Expect(i.options["connect_timeout"]).Should(Equal("10"))
		Expect(i.options["application_name"]).Should(Equal("myapp"))
		Expect(i.ssl).Should(BeTrue())
	})

	It("Invalid scheme", func() {
		_, err := parseConnectString("http://www.apigee.com")
		Expect(err).ShouldNot(Succeed())
	})

	It("Invalid URI", func() {
		_, err := parseConnectString("Hello, World!")
		Expect(err).ShouldNot(Succeed())
	})
})
