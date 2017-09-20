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
	"context"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/SermoDigital/jose/crypto"
	"github.com/SermoDigital/jose/jws"
	"github.com/julienschmidt/httprouter"
	"github.com/justinas/alice"
)

const params = "params"

// Errors to return
type Errors []string

/*
The SSO key parameters
*/
type ssoKey struct {
	Alg   string `json:"alg"`
	Value string `json:"value"`
	Kty   string `json:"kty"`
	Use   string `json:"use"`
	N     string `json:"n"`
	E     string `json:"e"`
}

/*
oauth provides http an connection to the URL that has the public
key for verifying the JWT token
*/
type oauth struct {
	gPkey   *rsa.PublicKey
	rwMutex *sync.RWMutex
}

/*
ErrorResponse delivers the errors back to the caller, once validation
has failed
*/
type ErrorResponse struct {
	Status  string   `json:"status"`
	Message string   `json:"message"`
	Errors  []string `json:"errors"`
}

/*
OAuthService offers interface functions that act on OAuth param,
used to verify JWT tokens for the Http handler functions client
wishes to validate against (via SSOHandler).
*/
type OAuthService interface {
	SSOHandler(p string, h func(http.ResponseWriter, *http.Request)) (string, httprouter.Handle)
}

/*
CreateOAuth is a constructor that creates OAuth for OAuthService
interface. OAuthService interface offers method:-
(1) SSOHandler(): Offers the user to attach http handler for JWT
verification.
*/
func (s *HTTPScaffold) CreateOAuth(keyURL string) OAuthService {
	pk, _ := getPublicKey(keyURL)
	oa := &oauth{
		rwMutex: &sync.RWMutex{},
	}
	oa.setPkSafe(pk)
	oa.updatePublicKeysPeriodic(keyURL)
	return oa
}

/*
SetParamsInRequest Sets the params and its values in the request
*/
func SetParamsInRequest(r *http.Request, ps httprouter.Params) *http.Request {
	newContext := context.WithValue(r.Context(), params, ps)
	return r.WithContext(newContext)
}

/*
FetchParams fetches the param values, given the params in the request
*/
func FetchParams(r *http.Request) httprouter.Params {
	ctx := r.Context()
	return ctx.Value(params).(httprouter.Params)
}

/*
SSOHandler offers the users the flexibility of choosing which http handlers
need JWT validation.
*/
func (a *oauth) SSOHandler(p string, h func(http.ResponseWriter, *http.Request)) (string, httprouter.Handle) {
	return p, a.VerifyOAuth(alice.New().ThenFunc(h))
}

/*
VerifyOAuth verifies the JWT token in the request using the public key configured
via CreateOAuth constructor.
*/
func (a *oauth) VerifyOAuth(next http.Handler) httprouter.Handle {

	return func(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {

		/* Parse the JWT from the input request */
		jwt, err := jws.ParseJWTFromRequest(r)
		if err != nil {
			WriteErrorResponse(http.StatusBadRequest, err.Error(), rw)
			return
		}

		/* Get the pulic key from cache */
		pk := a.getPkSafe()
		if pk == nil {
			WriteErrorResponse(http.StatusBadRequest, "Public key not configured. Validation failed.", rw)
			return
		}

		/* Validate the token */
		err = jwt.Validate(pk, crypto.SigningMethodRS256)
		if err != nil {
			WriteErrorResponse(http.StatusBadRequest, err.Error(), rw)
			return
		}

		/* Set the input params in the request */
		r = SetParamsInRequest(r, ps)
		next.ServeHTTP(rw, r)
	}

}

/*
WriteErrorResponse write a non 200 error response
*/
func WriteErrorResponse(statusCode int, message string, w http.ResponseWriter) {
	var errstr []string

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	errstr = append(errstr, http.StatusText(statusCode))
	resp := ErrorResponse{
		Status:  http.StatusText(statusCode),
		Message: message,
		Errors:  errstr,
	}
	json.NewEncoder(w).Encode(resp)
}

/*
updatePulicKeysPeriodic updates the cache periodically (every hour)
*/
func (a *oauth) updatePublicKeysPeriodic(keyURL string) {

	ticker := time.NewTicker(time.Hour)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				pk, err := getPublicKey(keyURL)
				if err == nil {
					a.setPkSafe(pk)
				}
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

/*
getPubicKey: Loads the Public key in to memory and returns it.
*/
func getPublicKey(keyURL string) (*rsa.PublicKey, error) {

	/* Connect to the server to fetch Key details */
	r, err := http.Get(keyURL)
	if err != nil {
		return nil, err
	}

	defer r.Body.Close()

	/* Decode the SSO Key */
	ssoKey := &ssoKey{}
	err = json.NewDecoder(r.Body).Decode(ssoKey)
	if err != nil {
		return nil, err
	}

	/* Retrieve the Public Key from SSO Key */
	publicKey, err := crypto.ParseRSAPublicKeyFromPEM([]byte(ssoKey.Value))
	if err != nil {
		return nil, err
	}
	return publicKey, nil

}

/*
setPkSafe Safely stores the Public Key (via a Write Lock)
*/
func (a *oauth) setPkSafe(pk *rsa.PublicKey) {
	a.rwMutex.Lock()
	a.gPkey = pk
	a.rwMutex.Unlock()
}

/*
getPkSafe returns the stored key (via a read lock)
*/
func (a *oauth) getPkSafe() *rsa.PublicKey {
	a.rwMutex.RLock()
	pk := a.gPkey
	a.rwMutex.RUnlock()
	return pk
}
