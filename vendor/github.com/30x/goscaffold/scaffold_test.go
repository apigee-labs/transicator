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

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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

	It("Get stack trace", func() {
		b := &bytes.Buffer{}
		dumpStack(b)
		Expect(b.Len()).ShouldNot(BeZero())
	})
})

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
