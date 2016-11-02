package goscaffold

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

const (
	// DefaultGraceTimeout is the default amount of time to wait for a request
	// to complete. Default is 30 seconds, which is also the default grace period
	// in Kubernetes.
	DefaultGraceTimeout = 30 * time.Second
)

/*
ErrSignalCaught is used in the "Shutdown" mechanism when the shutdown was
caused by a SIGINT or SIGTERM.
*/
var ErrSignalCaught = errors.New("Caught shutdown signal")

/*
ErrManualStop is used when the user doesn't have a reason.
*/
var ErrManualStop = errors.New("Shutdown called")

/*
ErrMarkedDown is used after being marked down but before being shut down.
*/
var ErrMarkedDown = errors.New("Marked down")

/*
HealthStatus is a type of response from a health check.
*/
type HealthStatus int

//go:generate stringer -type HealthStatus .

const (
	// OK denotes that everything is good
	OK HealthStatus = iota
	// NotReady denotes that the server is OK, but cannot process requests now
	NotReady HealthStatus = iota
	// Failed denotes that the server is bad
	Failed HealthStatus = iota
)

/*
HealthChecker is a type of function that an implementer may
implement in order to customize what we return from the "health"
and "ready" URLs. It must return either "OK", which means that everything
is fine, "not ready," which means that the "ready" check will fail but
the health check is OK, and "failed," which means that both are bad.
The function may return an optional error, which will be returned as
a reason for the status and will be placed in responses.
*/
type HealthChecker func() (HealthStatus, error)

/*
MarkdownHandler is a type of function that an user may implement in order to
be notified when the server is marked down. The function may do anything
it needs to do in response to a markdown request. However, markdown will
proceed even if the function fails. In case the function takes a long time,
the scaffold will always invoke it inside a new goroutine.
*/
type MarkdownHandler func()

/*
An HTTPScaffold provides a set of features on top of a standard HTTP
listener. It includes an HTTP handler that may be plugged in to any
standard Go HTTP server. It is intended to be placed before any other
handlers.
*/
type HTTPScaffold struct {
	insecurePort       int
	managementPort     int
	open               bool
	tracker            *requestTracker
	insecureListener   net.Listener
	managementListener net.Listener
	healthCheck        HealthChecker
	healthPath         string
	readyPath          string
	markdownPath       string
	markdownMethod     string
	markdownHandler    MarkdownHandler
}

/*
CreateHTTPScaffold makes a new scaffold. The default scaffold will
do nothing.
*/
func CreateHTTPScaffold() *HTTPScaffold {
	return &HTTPScaffold{
		insecurePort:   0,
		managementPort: -1,
		open:           false,
	}
}

/*
SetInsecurePort sets the port number to listen on in regular "HTTP" mode.
It may be set to zero, which indicates to listen on an ephemeral port.
It must be called before "listen".
*/
func (s *HTTPScaffold) SetInsecurePort(ip int) {
	s.insecurePort = ip
}

/*
InsecureAddress returns the actual address (including the port if an
ephemeral port was used) where we are listening. It must only be
called after "Listen."
*/
func (s *HTTPScaffold) InsecureAddress() string {
	if s.insecureListener == nil {
		return ""
	}
	return s.insecureListener.Addr().String()
}

/*
SetManagementPort sets the port number for management operations, including
health checks and diagnostic operations. If not set, then these operations
happen on the other ports. If set, then they only happen on this port.
*/
func (s *HTTPScaffold) SetManagementPort(p int) {
	s.managementPort = p
}

/*
ManagementAddress returns the actual address (including the port if an
ephemeral port was used) where we are listening for management
operations. If "SetManagementPort" was not set, then it returns null.
*/
func (s *HTTPScaffold) ManagementAddress() string {
	if s.managementListener == nil {
		return ""
	}
	return s.managementListener.Addr().String()
}

/*
SetHealthPath sets up a health check on the management port (if set) or
otherwise the main port. If a health check function has been supplied,
it will return 503 if the function returns "Failed" and 200 otherwise.
This path is intended to be used by systems like Kubernetes as the
"health check." These systems will shut down the server if we return
a non-200 URL.
*/
func (s *HTTPScaffold) SetHealthPath(p string) {
	s.healthPath = p
}

/*
SetReadyPath sets up a readines check on the management port (if set) or
otherwise the main port. If a health check function has been supplied,
it will return 503 if the function returns "Failed" or "Not Ready".
It will also return 503 if the "Shutdown" function was called
(or caught by signal handler). This path is intended to be used by
load balancers that will decide whether to route calls, but not by
systems like Kubernetes that will decide to shut down this server.
*/
func (s *HTTPScaffold) SetReadyPath(p string) {
	s.readyPath = p
}

/*
SetMarkdown sets up a URI that will cause the server to mark it
self down. However, this URI will not cause the server to actually shut
down. Once any HTTP request is received on this path with a matching
method, the server will be marked down. (the "readyPath" will respond
with 503, and all other HTTP calls other than the "healthPath" will
also respond with 503. The "healthPath" will still respond with 200.)
If "handler" is not nil, the handler will be invoked and the API call
will not return until the handler has returned. Because of that, the
handler should return in a timely manner. (For instance, it should return
in less than 30 seconds if Kubernetes is used unless the "grace period"
is extended.)
This makes this function the right thing to use
as a "preStop" method in Kubernetes, so that the server can take action
after shutdown to indicate that it has been deleted on purpose.
*/
func (s *HTTPScaffold) SetMarkdown(method, path string, handler MarkdownHandler) {
	s.markdownPath = path
	s.markdownMethod = method
	s.markdownHandler = handler
}

/*
SetHealthChecker specifies a function that the scaffold will call every time
"HealthPath" or "ReadyPath" is invoked.
*/
func (s *HTTPScaffold) SetHealthChecker(c HealthChecker) {
	s.healthCheck = c
}

/*
Open opens up the ports that were created when the scaffold was set up.
This method is optional. It may be called before Listen so that we can
retrieve the actual address where the server is listening before we actually
start to listen.
*/
func (s *HTTPScaffold) Open() error {
	s.tracker = startRequestTracker(DefaultGraceTimeout)

	il, err := net.ListenTCP("tcp", &net.TCPAddr{
		Port: s.insecurePort,
	})
	if err != nil {
		return err
	}
	s.insecureListener = il
	defer func() {
		if !s.open {
			il.Close()
		}
	}()

	if s.managementPort >= 0 {
		ml, err := net.ListenTCP("tcp", &net.TCPAddr{
			Port: s.managementPort,
		})
		if err != nil {
			return err
		}
		s.managementListener = ml
		defer func() {
			if !s.open {
				ml.Close()
			}
		}()
	}

	s.open = true
	return nil
}

/*
Listen should be called instead of using the standard "http" and "net"
libraries. It will open a port (or ports) and begin listening for
HTTP traffic. It will block until the server is shut down by
the various methods in this class.
It will use the graceful shutdown logic to ensure that once marked down,
the server will not exit until all the requests have completed,
or until the shutdown timeout has expired.
Like http.Serve, this function will block until we are done serving HTTP.
If "SetInsecurePort" or "SetSecurePort" were not set, then it will listen on
a dynamic port.
Listen will block until the server is shutdown using "Shutdown" or one of
the other shutdown mechanisms. It must not be called until after "Open"
has been called.
If shut down, Listen will return the error that was passed to the "shutdown"
method.
*/
func (s *HTTPScaffold) Listen(baseHandler http.Handler) error {
	if !s.open {
		err := s.Open()
		if err != nil {
			return err
		}
		s.open = true
	}

	// This is the handler that wraps customer API calls with tracking
	trackingHandler := &requestHandler{
		s:     s,
		child: baseHandler,
	}
	mgmtHandler := s.createManagementHandler()

	var mainHandler http.Handler
	if s.managementPort >= 0 {
		// Management on separate port
		mainHandler = trackingHandler
		go http.Serve(s.managementListener, mgmtHandler)
	} else {
		// Management on same port
		mgmtHandler.child = trackingHandler
		mainHandler = mgmtHandler
	}

	go http.Serve(s.insecureListener, mainHandler)

	err := <-s.tracker.C

	s.insecureListener.Close()
	if s.managementListener != nil {
		s.managementListener.Close()
	}

	return err
}

/*
Shutdown indicates that the server should stop handling incoming requests
and exit from the "Serve" call. This may be called automatically by
calling "CatchSignals," or automatically using this call. If
"reason" is nil, a default reason will be assigned.
*/
func (s *HTTPScaffold) Shutdown(reason error) {
	if reason == nil {
		s.tracker.shutdown(ErrManualStop)
	} else {
		s.tracker.shutdown(reason)
	}
}

/*
CatchSignals directs the scaffold to listen for common signals. It catches
three signals. SIGINT (aka control-C) and SIGTERM (what "kill" sends by default)
will cause the program to be marked down, and "SignalCaught" will be returned
by the "Listen" method. SIGHUP ("kill -1" or "kill -HUP") will cause the
stack trace of all the threads to be printed to stderr, just like a Java program.
*/
func (s *HTTPScaffold) CatchSignals() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT)
	signal.Notify(sigChan, syscall.SIGTERM)
	signal.Notify(sigChan, syscall.SIGHUP)

	go func() {
		for {
			sig := <-sigChan
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				s.Shutdown(ErrSignalCaught)
				signal.Reset()
				return
			case syscall.SIGHUP:
				dumpStack()
			}
		}
	}()
}

func dumpStack() {
	stackSize := 32767
	stackBuf := make([]byte, stackSize)
	var w int

	for w < stackSize {
		w = runtime.Stack(stackBuf, true)
		if w == stackSize {
			stackSize *= 2
			stackBuf = make([]byte, stackSize)
		}
	}

	fmt.Fprint(os.Stderr, string(stackBuf[:w]))
}
