/*
Copyright 2016 Google Inc.

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
	"io/ioutil"
	"net/http"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Diag API tests", func() {
	It("Stack test", func() {
		url := fmt.Sprintf("%s/diagnostics/stack", baseURL)
		resp, err := http.Get(url)
		Expect(err).Should(Succeed())
		defer resp.Body.Close()

		bod, err := ioutil.ReadAll(resp.Body)
		Expect(err).Should(Succeed())
		Expect(len(bod)).ShouldNot(BeZero())
	})
})
