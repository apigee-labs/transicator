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
	"github.com/apigee-labs/transicator/common"
	"net/http"
)

type errorCode int

const (
	unsupportedFormat errorCode = iota
	invalidParameter  errorCode = iota
	missingParameter  errorCode = iota
	snapshotOld       errorCode = iota
	serverError       errorCode = iota
)

func sendAPIError(code errorCode, description string,
	resp http.ResponseWriter, req *http.Request) {

	ec, em, sc := code.errInfo()
	common.SendAPIError(ec, em, description, sc, resp, req)
}

func (e errorCode) errInfo() (string, string, int) {
	switch e {
	case unsupportedFormat:
		return "UNSUPPORTED_FORMAT", "The specified media type is not supported", http.StatusUnsupportedMediaType
	case invalidParameter:
		return "PARAMETER_INVALID", "A parameter has an invalid value", http.StatusBadRequest
	case missingParameter:
		return "MISSING_PARAMETER", "A required parameter is missing", http.StatusBadRequest
	case snapshotOld:
		return "SNAPSHOT_TOO_OLD", "The client is operating on an old snapshot", http.StatusBadRequest
	case serverError:
		return "INTERNAL_SERVER_ERROR", "An error occurred in the server", http.StatusInternalServerError
	default:
		return "UNKNOWN", "An unknown error occurred", http.StatusInternalServerError
	}
}
