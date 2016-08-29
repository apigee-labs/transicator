package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/tylerb/graceful"
)

const (
	defaultPort             = 9000
	defaultCacheSize        = 65536
	gracefulShutdownTimeout = 60 * time.Second
)

func main() {
	exitCode := runMain()
	os.Exit(exitCode)
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage:")
	flag.PrintDefaults()
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Example:")
	fmt.Fprintln(os.Stderr,
		"changeserver -p 9000 -d ./data -u postgres://admin@localhost/postgres -s replication1")
}

func runMain() int {
	var port int
	var dbDir string
	var pgURL string
	var pgSlot string
	var prefix string
	var debug bool
	var help bool

	flag.IntVar(&port, "p", defaultPort, "Listen port")
	flag.StringVar(&dbDir, "d", "", "Location of database files")
	flag.StringVar(&pgURL, "u", "", "URL to connect to Postgres")
	flag.StringVar(&pgSlot, "s", "", "Slot name for Postgres logical replication")
	flag.BoolVar(&debug, "D", false, "Turn on debugging")
	flag.StringVar(&prefix, "P", "", "Optional prefix URL for all API calls")
	flag.BoolVar(&help, "h", false, "Print help message")
	flag.Parse()

	if help || !flag.Parsed() {
		printUsage()
		return 2
	}
	if dbDir == "" || pgURL == "" || pgSlot == "" {
		fmt.Fprintln(os.Stderr, "-d, -u, and -s parameters are all required")
		printUsage()
		return 3
	}

	if debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	mux := http.NewServeMux()

	server, err := startChangeServer(mux, dbDir, pgURL, pgSlot, prefix)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %s\n", err)
		return 4
	}
	defer server.stop()

	listener, err := net.ListenTCP("tcp", &net.TCPAddr{
		Port: port,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %s\n", err)
		return 5
	}
	defer listener.Close()

	svr := &http.Server{
		Handler: mux,
	}

	// This will intercept signals and carefully stop running HTTP transactions,
	// then return so that we can close everything above.
	graceful.Serve(svr, listener, gracefulShutdownTimeout)

	return 0
}
