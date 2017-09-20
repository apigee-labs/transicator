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
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/SermoDigital/jose/crypto"
	"github.com/SermoDigital/jose/jws"
	"github.com/julienschmidt/httprouter"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	validJWTSigner   = "https://raw.githubusercontent.com/apid/goscaffold/master/testkeys/jwtcert.json"
	invalidJWTSigner = "https://raw.githubusercontent.com/apid/goscaffold/master/testkeys/notfound.json"
)

var (
	dbURL string
)

var insecureClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	},
}

var _ = Describe("Scaffold Tests", func() {
	It("Validate framework", func() {
		s := CreateHTTPScaffold()
		s.SetlocalBindIPAddressV4(GetLocalIP())
		stopChan := make(chan error)
		err := s.Open()
		Expect(err).Should(Succeed())

		go func() {
			fmt.Fprintf(GinkgoWriter, "Gonna listen on %s\n", s.InsecureAddress())
			stopErr := s.Listen(&testHandler{})
			fmt.Fprintf(GinkgoWriter, "Done listening\n")
			stopChan <- stopErr
		}()

		Eventually(func() bool {
			return testGet(s, "")
		}, 5*time.Second).Should(BeTrue())
		resp, err := http.Get(fmt.Sprintf("http://%s", s.InsecureAddress()))
		Expect(err).Should(Succeed())
		resp.Body.Close()
		Expect(resp.StatusCode).Should(Equal(200))
		validatePprof(s.InsecureAddress())
		shutdownErr := errors.New("Validate")
		s.Shutdown(shutdownErr)
		Eventually(stopChan).Should(Receive(Equal(shutdownErr)))
	})

	It("Separate management port", func() {
		s := CreateHTTPScaffold()
		s.SetlocalBindIPAddressV4(GetLocalIP())
		s.SetManagementPort(0)
		stopChan := make(chan error)
		err := s.Open()
		Expect(err).Should(Succeed())

		go func() {
			fmt.Fprintf(GinkgoWriter, "Gonna listen on %s and %s\n",
				s.InsecureAddress(), s.ManagementAddress())
			stopErr := s.Listen(&testHandler{})
			fmt.Fprintf(GinkgoWriter, "Done listening\n")
			stopChan <- stopErr
		}()

		// Just make sure that it's up
		Eventually(func() bool {
			return testGet(s, "")
		}, 5*time.Second).Should(BeTrue())
		resp, err := http.Get(fmt.Sprintf("http://%s", s.InsecureAddress()))
		Expect(err).Should(Succeed())
		resp.Body.Close()
		Expect(resp.StatusCode).Should(Equal(200))
		resp, err = http.Get(fmt.Sprintf("http://%s", s.ManagementAddress()))
		Expect(err).Should(Succeed())
		resp.Body.Close()
		Expect(resp.StatusCode).Should(Equal(404))
		validatePprof(s.ManagementAddress())
		shutdownErr := errors.New("Validate")
		s.Shutdown(shutdownErr)
		Eventually(stopChan).Should(Receive(Equal(shutdownErr)))
	})

	It("Shutdown", func() {
		s := CreateHTTPScaffold()
		s.SetHealthPath("/health")
		s.SetReadyPath("/ready")
		stopChan := make(chan error)
		err := s.Open()
		Expect(err).Should(Succeed())

		go func() {
			stopErr := s.Listen(&testHandler{})
			stopChan <- stopErr
		}()

		go func() {
			code, _ := getText(fmt.Sprintf("http://%s?delay=1s", s.InsecureAddress()))
			Expect(code).Should(Equal(200))
		}()

		// Just make sure server is listening
		Eventually(func() bool {
			return testGet(s, "")
		}, 5*time.Second).Should(BeTrue())

		// Ensure that we are healthy and ready
		code, _ := getText(fmt.Sprintf("http://%s/health", s.InsecureAddress()))
		Expect(code).Should(Equal(200))
		code, _ = getText(fmt.Sprintf("http://%s/ready", s.InsecureAddress()))
		Expect(code).Should(Equal(200))

		// Previous call prevents server from exiting
		Consistently(stopChan, 250*time.Millisecond).ShouldNot(Receive())

		// Tell the server to try and exit
		stopErr := errors.New("Stop")
		s.Shutdown(stopErr)

		// Should take one second -- in the meantime, calls should fail with 503,
		// health should be good, but ready should be bad
		code, _ = getText(fmt.Sprintf("http://%s", s.InsecureAddress()))
		Expect(code).Should(Equal(503))
		code, _ = getText(fmt.Sprintf("http://%s/ready", s.InsecureAddress()))
		Expect(code).Should(Equal(503))
		code, _ = getText(fmt.Sprintf("http://%s/health", s.InsecureAddress()))
		Expect(code).Should(Equal(200))

		// Do a bunch more stops because we are funny that way.
		// We just want to make sure that we don't hang if we stop a FEW times.
		for i := 0; i < 25; i++ {
			s.Shutdown(stopErr)
		}

		// But in less than two seconds, server should be down
		Eventually(stopChan, 2*time.Second).Should(Receive(Equal(stopErr)))
		// Calls should now fail
		Eventually(func() bool {
			return testGet(s, "")
		}, time.Second).Should(BeFalse())
	})

	It("Markdown", func() {
		var markedDown int32

		s := CreateHTTPScaffold()
		s.SetHealthPath("/health")
		s.SetReadyPath("/ready")
		s.SetMarkdown("POST", "/markdown", func() {
			atomic.StoreInt32(&markedDown, 1)
		})

		stopChan := make(chan error)
		err := s.Open()
		Expect(err).Should(Succeed())

		go func() {
			listenErr := s.Listen(&testHandler{})
			stopChan <- listenErr
		}()

		// Just make sure server is listening
		Eventually(func() bool {
			return testGet(s, "")
		}, 5*time.Second).Should(BeTrue())

		// Ensure that we are healthy and ready
		code, _ := getText(fmt.Sprintf("http://%s/health", s.InsecureAddress()))
		Expect(code).Should(Equal(200))
		code, _ = getText(fmt.Sprintf("http://%s/ready", s.InsecureAddress()))
		Expect(code).Should(Equal(200))

		// Mark the server down, but don't stop it
		resp, err := http.Post(fmt.Sprintf("http://%s/markdown", s.InsecureAddress()),
			"text/plain", strings.NewReader("Goodbye!"))
		Expect(err).Should(Succeed())
		resp.Body.Close()
		Expect(resp.StatusCode).Should(Equal(200))

		// Server should immediately be marked down, not ready, but healthy
		Expect(atomic.LoadInt32(&markedDown)).Should(BeEquivalentTo(1))
		code, _ = getText(fmt.Sprintf("http://%s", s.InsecureAddress()))
		Expect(code).Should(Equal(503))
		code, _ = getText(fmt.Sprintf("http://%s/ready", s.InsecureAddress()))
		Expect(code).Should(Equal(503))
		code, _ = getText(fmt.Sprintf("http://%s/health", s.InsecureAddress()))
		Expect(code).Should(Equal(200))

		// Server should not have stopped yet
		Consistently(stopChan).ShouldNot(Receive())

		stopErr := errors.New("Test stop")
		s.Shutdown(stopErr)
		Eventually(stopChan).Should(Receive(Equal(stopErr)))
	})

	It("Health Check Functions", func() {
		status := int32(OK)
		var healthErr = &atomic.Value{}

		statusFunc := func() (HealthStatus, error) {
			stat := HealthStatus(atomic.LoadInt32(&status))
			av := healthErr.Load()
			if av != nil {
				errPtr := av.(*error)
				return stat, *errPtr
			}
			return stat, nil
		}

		s := CreateHTTPScaffold()
		s.SetlocalBindIPAddressV4(GetLocalIP())
		s.SetManagementPort(0)
		s.SetHealthPath("/health")
		s.SetReadyPath("/ready")
		s.SetHealthChecker(statusFunc)
		stopChan := make(chan error)
		err := s.Open()
		Expect(err).Should(Succeed())

		go func() {
			fmt.Fprintf(GinkgoWriter, "Gonna listen on %s and %s\n",
				s.InsecureAddress(), s.ManagementAddress())
			stopErr := s.Listen(&testHandler{})
			fmt.Fprintf(GinkgoWriter, "Done listening\n")
			stopChan <- stopErr
		}()

		// Just make sure that it's up
		Eventually(func() bool {
			return testGet(s, "")
		}, 5*time.Second).Should(BeTrue())

		// Health should be good
		code, _ := getText(fmt.Sprintf("http://%s/health", s.ManagementAddress()))
		Expect(code).Should(Equal(200))
		code, _ = getText(fmt.Sprintf("http://%s/ready", s.ManagementAddress()))
		Expect(code).Should(Equal(200))

		// Mark down to "unhealthy" state. Should be bad.
		atomic.StoreInt32(&status, int32(Failed))
		code, bod := getText(fmt.Sprintf("http://%s/health", s.ManagementAddress()))
		Expect(code).Should(Equal(503))
		Expect(bod).Should(Equal("Failed"))
		code, _ = getText(fmt.Sprintf("http://%s/ready", s.ManagementAddress()))
		Expect(code).Should(Equal(503))

		// Change to merely "not ready" state. Should be healthy but not ready.
		atomic.StoreInt32(&status, int32(NotReady))
		code, _ = getText(fmt.Sprintf("http://%s/health", s.ManagementAddress()))
		Expect(code).Should(Equal(200))
		code, bod = getText(fmt.Sprintf("http://%s/ready", s.ManagementAddress()))
		Expect(code).Should(Equal(503))
		Expect(bod).Should(Equal("NotReady"))

		// Customize the error message.
		customErr := errors.New("Custom")
		healthErr.Store(&customErr)
		code, bod = getText(fmt.Sprintf("http://%s/ready", s.ManagementAddress()))
		Expect(code).Should(Equal(503))
		Expect(bod).Should(Equal("Custom"))

		// Check it in JSON
		code, js := getJSON(fmt.Sprintf("http://%s/ready", s.ManagementAddress()))
		Expect(code).Should(Equal(503))
		Expect(js["status"]).Should(Equal("NotReady"))
		Expect(js["reason"]).Should(Equal("Custom"))

		// Mark back up. Should be all good
		atomic.StoreInt32(&status, int32(OK))
		code, _ = getText(fmt.Sprintf("http://%s/health", s.ManagementAddress()))
		Expect(code).Should(Equal(200))
		code, _ = getText(fmt.Sprintf("http://%s/ready", s.ManagementAddress()))
		Expect(code).Should(Equal(200))

		s.Shutdown(nil)
		Eventually(stopChan).Should(Receive(Equal(ErrManualStop)))
	})

	It("Secure And Insecure Ports", func() {
		s := CreateHTTPScaffold()
		s.SetSecurePort(0)
		s.SetKeyFile("./testkeys/clearkey.pem")
		s.SetCertFile("./testkeys/clearcert.pem")
		stopChan := make(chan error)
		err := s.Open()
		Expect(err).Should(Succeed())

		go func() {
			fmt.Fprintf(GinkgoWriter, "Gonna listen on %s and %s\n",
				s.InsecureAddress(), s.SecureAddress())
			stopErr := s.Listen(&testHandler{})
			fmt.Fprintf(GinkgoWriter, "Done listening\n")
			stopChan <- stopErr
		}()

		Eventually(func() bool {
			return testGet(s, "")
		}, 5*time.Second).Should(BeTrue())
		Eventually(func() bool {
			return testGetSecure(s, "")
		}, time.Second).Should(BeTrue())

		shutdownErr := errors.New("Validate")
		s.Shutdown(shutdownErr)
		Eventually(stopChan).Should(Receive(Equal(shutdownErr)))
	})

	It("Secure Port Only", func() {
		s := CreateHTTPScaffold()
		s.SetSecurePort(0)
		s.SetInsecurePort(-1)
		s.SetKeyFile("./testkeys/clearkey.pem")
		s.SetCertFile("./testkeys/clearcert.pem")
		stopChan := make(chan error)
		err := s.Open()
		Expect(err).Should(Succeed())
		Expect(s.InsecureAddress()).Should(BeEmpty())

		go func() {
			fmt.Fprintf(GinkgoWriter, "Gonna listen on %s\n",
				s.SecureAddress())
			stopErr := s.Listen(&testHandler{})
			fmt.Fprintf(GinkgoWriter, "Done listening\n")
			stopChan <- stopErr
		}()

		Eventually(func() bool {
			return testGetSecure(s, "")
		}, 5*time.Second).Should(BeTrue())

		shutdownErr := errors.New("Validate")
		s.Shutdown(shutdownErr)
		Eventually(stopChan).Should(Receive(Equal(shutdownErr)))
	})

	It("DisAllow non-localhost", func() {
		s := CreateHTTPScaffold()
		s.SetInsecurePort(8181)
		s.SetlocalBindIPAddressV4([]byte{127, 0, 0, 1})
		stopChan := make(chan error)
		err := s.Open()
		Expect(err).Should(Succeed())

		go func() {
			fmt.Fprintf(GinkgoWriter, "Gonna listen on %s\n", s.InsecureAddress())
			stopErr := s.Listen(&testHandler{})
			fmt.Fprintf(GinkgoWriter, "Done listening\n")
			stopChan <- stopErr
		}()

		Eventually(func() bool {
			return testGet(s, "")
		}, 5*time.Second).Should(BeTrue())
		_, err = http.Get(fmt.Sprintf("http://%s:%s", GetLocalIPStr(), "8181"))
		Expect(err).ShouldNot(Succeed())
		shutdownErr := errors.New("Validate")
		s.Shutdown(shutdownErr)
		Eventually(stopChan).Should(Receive(Equal(shutdownErr)))

	})

	It("SSO handler validation", func() {

		var vals ErrorResponse

		router := httprouter.New()
		Expect(router).ShouldNot(BeNil())
		scaf := CreateHTTPScaffold()
		Expect(scaf).ShouldNot(BeNil())
		err := scaf.Open()
		Expect(err).Should(Succeed())
		oauth := scaf.CreateOAuth(validJWTSigner)
		Expect(oauth).ShouldNot(BeNil())
		go func() {
			fmt.Fprintf(GinkgoWriter, "Gonna listen on %s\n", scaf.InsecureAddress())
			router.GET(oauth.SSOHandler("/foobar/:param1/:param2", buslogicHandler))
			scaf.Listen(router)
		}()

		Eventually(func() int {
			req, reqerr := http.NewRequest("GET",
				"http://"+scaf.InsecureAddress()+"/foobar/xyz/123", nil)
			Expect(reqerr).Should(Succeed())
			req.Header.Set("Authorization", "Bearer "+string(createJWT()))
			client := &http.Client{}
			resp, reqerr := client.Do(req)
			Expect(reqerr).Should(Succeed())
			defer resp.Body.Close()
			return resp.StatusCode
		}, 2*time.Second).Should(Equal(200))

		req, err := http.NewRequest("GET",
			"http://"+scaf.InsecureAddress()+"/foobar/xyz/123", nil)
		Expect(err).Should(Succeed())
		req.Header.Set("Authorization", "Bearer DEADBEEF")
		client := &http.Client{}
		resp, err := client.Do(req)
		Expect(err).Should(Succeed())
		defer resp.Body.Close()
		Expect(resp.StatusCode).Should(Equal(400))
		body, err := ioutil.ReadAll(resp.Body)
		Expect(err).Should(Succeed())
		err = json.Unmarshal(body, &vals)
		Expect(err).Should(Succeed())
		Expect(vals.Status).Should(Equal("Bad Request"))
		Expect(vals.Message).Should(Equal("not a compact JWS"))
	})

	It("SSO handler validation bad public key", func() {

		var vals ErrorResponse

		router := httprouter.New()
		Expect(router).ShouldNot(BeNil())
		scaf := CreateHTTPScaffold()
		Expect(scaf).ShouldNot(BeNil())
		err := scaf.Open()
		Expect(err).Should(Succeed())
		oauth := scaf.CreateOAuth(invalidJWTSigner)
		Expect(oauth).ShouldNot(BeNil())
		go func() {
			fmt.Fprintf(GinkgoWriter, "Gonna listen on %s\n", scaf.InsecureAddress())
			router.GET(oauth.SSOHandler("/foobar/:param1/:param2", buslogicHandler))
			scaf.Listen(router)
		}()

		req, err := http.NewRequest("GET",
			"http://"+scaf.InsecureAddress()+"/foobar/xyz/123", nil)
		Expect(err).Should(Succeed())
		req.Header.Set("Authorization", "Bearer "+string(createJWT()))
		client := &http.Client{}
		resp, err := client.Do(req)
		Expect(err).Should(Succeed())
		defer resp.Body.Close()
		Expect(resp.StatusCode).Should(Equal(400))
		body, err := ioutil.ReadAll(resp.Body)
		Expect(err).Should(Succeed())
		err = json.Unmarshal(body, &vals)
		Expect(err).Should(Succeed())
		Expect(vals.Status).Should(Equal("Bad Request"))
		Expect(vals.Message).Should(Equal("Public key not configured. Validation failed."))
	})

	It("Get stack trace", func() {
		b := &bytes.Buffer{}
		dumpStack(b)
		Expect(b.Len()).ShouldNot(BeZero())
	})

	It("Verify JWT creation", func() {
		// Ensure that our logic in this test for creating a JWT really works
		jwt := createJWT()
		fmt.Fprintf(GinkgoWriter, "JWT: %s\n", string(jwt))

		certBytes, err := ioutil.ReadFile("./testkeys/jwtcert.pem")
		Expect(err).Should(Succeed())
		cert, err := crypto.ParseRSAPublicKeyFromPEM(certBytes)
		Expect(err).Should(Succeed())

		parsedJwt, err := jws.ParseJWT(jwt)
		Expect(err).Should(Succeed())

		err = parsedJwt.Validate(cert, crypto.SigningMethodRS256)
		Expect(err).Should(Succeed())
	})
})

func buslogicHandler(w http.ResponseWriter, r *http.Request) {
	p := FetchParams(r)
	cid := p.ByName("param1")
	Expect(cid).To(Equal("xyz"))
	cid = p.ByName("param2")
	Expect(cid).To(Equal("123"))
}

func getText(url string) (int, string) {
	req, err := http.NewRequest("GET", url, nil)
	Expect(err).Should(Succeed())
	req.Header.Set("Accept", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	Expect(err).Should(Succeed())
	defer resp.Body.Close()
	bod, err := ioutil.ReadAll(resp.Body)
	Expect(err).Should(Succeed())
	return resp.StatusCode, string(bod)
}

func getJSON(url string) (int, map[string]string) {
	req, err := http.NewRequest("GET", url, nil)
	Expect(err).Should(Succeed())
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	Expect(err).Should(Succeed())
	defer resp.Body.Close()
	bod, err := ioutil.ReadAll(resp.Body)
	Expect(err).Should(Succeed())
	var vals map[string]string
	err = json.Unmarshal(bod, &vals)
	Expect(err).Should(Succeed())
	return resp.StatusCode, vals
}

func validatePprof(addr string) {
	code, _ := getText(fmt.Sprintf("http://%s/debug/pprof/", addr))
	Expect(code).Should(Equal(200))
	code, cmdline := getText(fmt.Sprintf("http://%s/debug/pprof/cmdline", addr))
	Expect(code).Should(Equal(200))
	Expect(cmdline).ShouldNot(BeEmpty())
}

func testGet(s *HTTPScaffold, path string) bool {
	resp, err := http.Get(fmt.Sprintf("http://%s", s.InsecureAddress()))
	if err != nil {
		fmt.Fprintf(GinkgoWriter, "Get %s = %s\n", path, err)
		return false
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		fmt.Fprintf(GinkgoWriter, "Get %s = %d\n", path, resp.StatusCode)
		return false
	}
	return true
}

func testGetSecure(s *HTTPScaffold, path string) bool {
	resp, err := insecureClient.Get(fmt.Sprintf("https://%s", s.SecureAddress()))
	if err != nil {
		fmt.Fprintf(GinkgoWriter, "Get %s = %s\n", path, err)
		return false
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		fmt.Fprintf(GinkgoWriter, "Get %s = %d\n", path, resp.StatusCode)
		return false
	}
	return true
}

type testHandler struct {
}

func (h *testHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	var err error
	var delayTime time.Duration

	delayStr := req.URL.Query().Get("delay")
	if delayStr != "" {
		delayTime, err = time.ParseDuration(delayStr)
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	if delayTime > 0 {
		time.Sleep(delayTime)
	}
}

func GetLocalIP() []byte {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP
			}
		}
	}
	return nil
}

func GetLocalIPStr() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func createJWT() []byte {
	keyBytes, err := ioutil.ReadFile("./testkeys/jwtkey.pem")
	Expect(err).Should(Succeed())
	pk, err := crypto.ParseRSAPrivateKeyFromPEM(keyBytes)
	Expect(err).Should(Succeed())

	claims := jws.Claims{}
	now := time.Now()
	claims.SetAudience("http://github.com/apid/goscaffold")
	claims.SetIssuer("http://github.com/apid/goscaffold")
	claims.SetSubject("http://github.com/apid/goscaffold")
	claims.SetIssuedAt(now)
	claims.SetNotBefore(now)
	claims.SetExpiration(now.Add(time.Hour))
	jwt := jws.NewJWT(claims, crypto.SigningMethodRS256)

	rawJwt, err := jwt.Serialize(pk)
	Expect(err).Should(Succeed())
	return rawJwt
}
