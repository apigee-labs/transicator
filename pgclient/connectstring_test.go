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

	It("Params", func() {
		i, err := parseConnectString("postgres://other@otherhost/otherdb?connect_timeout=10&application_name=myapp")
		Expect(err).Should(Succeed())
		Expect(i.host).Should(Equal("otherhost"))
		Expect(i.port).Should(Equal(5432))
		Expect(i.database).Should(Equal("otherdb"))
		Expect(i.user).Should(Equal("other"))
		Expect(i.creds).Should(BeEmpty())
		Expect(i.options["connect_timeout"]).Should(Equal("10"))
		Expect(i.options["application_name"]).Should(Equal("myapp"))
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
