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
	"fmt"
	"net/http"
)

const (
	port = 8080
)

func Example() {
	// Create a new scaffold that will listen for HTTP on port 8080
	scaf := CreateHTTPScaffold()
	scaf.SetInsecurePort(port)

	// Direct the scaffold to catch common signals and trigger a
	// graceful shutdown.
	scaf.CatchSignals()

	// Set up a URL that may be used by a load balancer to test
	// if the server is ready to handle requests
	scaf.SetReadyPath("/ready")

	// Set up a URL that may be used by infrastructure to test
	// if the server is working or if it needs to be restarted or replaced
	scaf.SetHealthPath("/healthy")

	listener := &TestListener{}
	fmt.Printf("Listening on %d\n", port)

	// Listen now. The listener will return when the server is actually
	// shut down.
	err := scaf.Listen(listener)

	// If we get here, and if we care to know, then the error will tell
	// us why we were shut down.
	fmt.Printf("HTTP server shut down: %s\n", err.Error())
}

/*
TestListener is an HTTP listener used for the example code. It just returns
200 and "Hello, World!"
*/
type TestListener struct {
}

func (l *TestListener) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	resp.Header().Set("Content-Type", "text/plain")
	resp.WriteHeader(http.StatusOK)
	resp.Write([]byte("Hello, World!"))
}
