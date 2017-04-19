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

package snapshotserver

import (
	"net/http"

	"github.com/apigee-labs/transicator/common"
)

type errorCode int

const (
	missingScope         errorCode = iota
	unsupportedMediaType errorCode = iota
	serverError          errorCode = iota
	invalidRequestParam  errorCode = iota
)

func sendAPIError(code errorCode, description string,
	resp http.ResponseWriter, req *http.Request) {

	ec, em, sc := code.errInfo()
	common.SendAPIError(ec, em, description, sc, resp, req)
}

func (e errorCode) errInfo() (string, string, int) {
	switch e {
	case missingScope:
		return "MISSING_SCOPE", "The \"scope\" parameter must be included", http.StatusBadRequest
	case unsupportedMediaType:
		return "UNSUPPORTED_MEDIA_TYPE", "The media type is not supported", http.StatusUnsupportedMediaType
	case serverError:
		return "INTERNAL_SERVER_ERROR", "An error occurred in the server", http.StatusInternalServerError
	case invalidRequestParam:
		return "INVALID_REQUEST_PARAM", "An invalid param was in the request", http.StatusBadRequest
	default:
		return "UNKNOWN", "An unknown error occurred", http.StatusInternalServerError
	}
}
