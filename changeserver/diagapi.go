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
package main

import (
	"net/http"
	"runtime"

	"github.com/julienschmidt/httprouter"
)

func (s *server) initDiagAPI(prefix string, router *httprouter.Router) {
	router.HandlerFunc("GET", prefix+"/diagnostics/stack", s.handleGetStack)
}

func (s *server) handleGetStack(
	resp http.ResponseWriter, req *http.Request) {
	stackBufLen := 64
	for {
		stackBuf := make([]byte, stackBufLen)
		stackLen := runtime.Stack(stackBuf, true)
		if stackLen == len(stackBuf) {
			// Must be truncated
			stackBufLen *= 2
		} else {
			resp.Header().Set("Content-Type", textContent)
			resp.Write(stackBuf[:stackLen])
			return
		}
	}
}
