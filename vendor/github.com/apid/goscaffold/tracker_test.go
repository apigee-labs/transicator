// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package goscaffold

import (
	"errors"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tracker tests", func() {
	It("Basic tracker", func() {
		t := startRequestTracker(10 * time.Second)
		t.start()
		Consistently(t.C, 250*time.Millisecond).ShouldNot(Receive())
		t.shutdown(errors.New("Basic"))
		Consistently(t.C, 250*time.Millisecond).ShouldNot(Receive())
		t.end()
		Eventually(t.C).Should(Receive(MatchError("Basic")))
	})

	It("Tracker stop idle", func() {
		t := startRequestTracker(10 * time.Second)
		t.shutdown(errors.New("Stop"))
		Eventually(t.C).Should(Receive(MatchError("Stop")))
	})

	It("Tracker grace timeout", func() {
		t := startRequestTracker(time.Second)
		t.start()
		t.shutdown(errors.New("Stop"))
		Eventually(t.C, 2*time.Second).Should(Receive(MatchError("Stop")))
	})
})
