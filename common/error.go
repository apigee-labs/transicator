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

package common

import (
	"encoding/json"
	"net/http"
)

/*
An APIError is an error returned, usually in JSON, from an API.
*/
type APIError struct {
	// Code is a short description of the error in symbolic form
	Code string `json:"code"`
	// Error is one sentence describing the error
	Error string `json:"error"`
	// Description is longer, if you prefer that
	Description string `json:"description,omitempty"`
}

/*
SendAPIError sends a standard error response in JSON format.
*/
func SendAPIError(code, error, description string,
	statusCode int,
	resp http.ResponseWriter, req *http.Request) {

	em := &APIError{
		Code:        code,
		Error:       error,
		Description: description,
	}

	resp.Header().Set("Content-Type", "application/json")
	resp.WriteHeader(statusCode)
	buf, _ := json.MarshalIndent(em, indentPrefix, indent)
	resp.Write(buf)
}
