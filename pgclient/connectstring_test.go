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
	"time"

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
		Expect(i.ssl).Should(Equal(sslPrefer))
		Expect(i.keepAlive).Should(BeTrue())
		Expect(i.keepAliveIdle).Should(BeNil())
		Expect(i.connectTimeout).Should(BeNil())
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
		Expect(i.ssl).Should(Equal(sslPrefer))
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
		Expect(i.ssl).Should(Equal(sslRequire))
		Expect(i.options).Should(BeEmpty())
	})

	It("User Host Port DB Override", func() {
		i, err := parseConnectString("postgresql://user:secret@myhost:9999/wrongdb?user=foo&password=bar&host=yourhost&port=9998&dbname=rightdb")
		Expect(err).Should(Succeed())
		Expect(i.host).Should(Equal("yourhost"))
		Expect(i.port).Should(Equal(9998))
		Expect(i.database).Should(Equal("rightdb"))
		Expect(i.user).Should(Equal("foo"))
		Expect(i.creds).Should(Equal("bar"))
		Expect(i.options).Should(BeEmpty())
	})

	It("SSL Disable", func() {
		i, err := parseConnectString("postgresql://localhost?sslmode=disable")
		Expect(err).Should(Succeed())
		Expect(i.ssl).Should(Equal(sslDisable))
	})

	It("SSL Allow", func() {
		i, err := parseConnectString("postgresql://localhost?sslmode=allow")
		Expect(err).Should(Succeed())
		Expect(i.ssl).Should(Equal(sslAllow))
	})

	It("SSL Prefer", func() {
		i, err := parseConnectString("postgresql://localhost?sslmode=prefer")
		Expect(err).Should(Succeed())
		Expect(i.ssl).Should(Equal(sslPrefer))
	})

	It("SSL Require", func() {
		i, err := parseConnectString("postgresql://localhost?sslmode=require")
		Expect(err).Should(Succeed())
		Expect(i.ssl).Should(Equal(sslRequire))
	})

	It("SSL Verify CA", func() {
		i, err := parseConnectString("postgresql://localhost?sslmode=verify-ca")
		Expect(err).Should(Succeed())
		Expect(i.ssl).Should(Equal(sslVerifyCA))
	})

	It("SSL Verify Full", func() {
		i, err := parseConnectString("postgresql://localhost?sslmode=verify-full")
		Expect(err).Should(Succeed())
		Expect(i.ssl).Should(Equal(sslVerifyFull))
	})

	It("SSL Unknown", func() {
		_, err := parseConnectString("postgresql://localhost?sslmode=notsupported")
		Expect(err).ShouldNot(Succeed())
	})

	It("SSL true", func() {
		i, err := parseConnectString("postgresql://localhost?ssl=true")
		Expect(err).Should(Succeed())
		Expect(i.ssl).Should(Equal(sslRequire))
	})

	It("SSL false", func() {
		i, err := parseConnectString("postgresql://localhost?ssl=false")
		Expect(err).Should(Succeed())
		Expect(i.ssl).Should(Equal(sslPrefer))
	})

	It("Require SSL 1", func() {
		i, err := parseConnectString("postgresql://localhost?requiressl=1")
		Expect(err).Should(Succeed())
		Expect(i.ssl).Should(Equal(sslRequire))
	})

	It("Require SSL 0", func() {
		i, err := parseConnectString("postgresql://localhost?requiressl=0")
		Expect(err).Should(Succeed())
		Expect(i.ssl).Should(Equal(sslPrefer))
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
		Expect(i.ssl).Should(Equal(sslRequire))
	})

	It("Keepalive", func() {
		i, err := parseConnectString("postgresql://localhost?keepalives=1")
		Expect(err).Should(Succeed())
		Expect(i.keepAlive).Should(BeTrue())
		Expect(i.keepAliveIdle).Should(BeNil())
	})

	It("Keepalive off", func() {
		i, err := parseConnectString("postgresql://localhost?keepalives=0")
		Expect(err).Should(Succeed())
		Expect(i.keepAlive).Should(BeFalse())
		Expect(i.keepAliveIdle).Should(BeNil())
	})

	It("Keepalive interval", func() {
		i, err := parseConnectString("postgresql://localhost?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=2")
		Expect(err).Should(Succeed())
		Expect(i.keepAlive).Should(BeTrue())
		Expect(i.keepAliveIdle).ShouldNot(BeNil())
		Expect(*(i.keepAliveIdle)).Should(Equal(10 * time.Second))
	})

	It("Connect timeout", func() {
		i, err := parseConnectString("postgresql://localhost?connect_timeout=100")
		Expect(err).Should(Succeed())
		Expect(i.connectTimeout).ShouldNot(BeNil())
		Expect(*(i.connectTimeout)).Should(Equal(100 * time.Second))
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
