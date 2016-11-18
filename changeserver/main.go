package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/30x/goscaffold"
	"github.com/Sirupsen/logrus"
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
		"changeserver -p 9000 -d ./data -u postgres://admin:secret@localhost/postgres -s replication1 -m 24h")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "  The value of the \"-m\" parameter for maximum age is a Golang")
	fmt.Fprintln(os.Stderr, "\"duration\": use \"m\", \"s\", and \"h\" for minutes, seconds, and hours")
}

func runMain() int {
	var port int
	var securePort int
	var mgmtPort int
	var dbDir string
	var pgURL string
	var pgSlot string
	var prefix string
	var maxAgeParam string
	var cert string
	var key string
	var debug bool
	var help bool

	flag.IntVar(&port, "p", -1, "Listen port")
	flag.IntVar(&securePort, "sp", -1, "HTTPS Listen port")
	flag.IntVar(&mgmtPort, "mp", -1, "Management port (for health checks)")
	flag.StringVar(&dbDir, "d", "", "Location of database files")
	flag.StringVar(&pgURL, "u", "", "URL to connect to Postgres")
	flag.StringVar(&pgSlot, "s", "", "Slot name for Postgres logical replication")
	flag.StringVar(&maxAgeParam, "m", "", "Purge records older than this age.")
	flag.StringVar(&cert, "cert", "", "TLS certificate PEM file")
	flag.StringVar(&key, "key", "", "TLS key PEM file (must be unencrypted)")
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
	if port < 0 && securePort < 0 {
		fmt.Fprintln(os.Stderr, "Either -p or -sp must be specified")
		printUsage()
		return 3
	}

	var maxAge time.Duration
	var err error
	if maxAgeParam != "" {
		maxAge, err = time.ParseDuration(maxAgeParam)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid value for max age: \"%s\": %s\n",
				maxAgeParam, err)
			printUsage()
			return 4
		}
	}

	if debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	mux := http.NewServeMux()

	server, err := createChangeServer(mux, dbDir, pgURL, pgSlot, prefix)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %s\n", err)
		return 4
	}
	server.start()
	defer server.stop()

	if maxAge > 0 {
		server.startCleanup(maxAge)
	}

	scaf := goscaffold.CreateHTTPScaffold()
	scaf.SetInsecurePort(port)
	scaf.SetSecurePort(securePort)
	if mgmtPort >= 0 {
		scaf.SetManagementPort(mgmtPort)
	}
	scaf.SetCertFile(cert)
	scaf.SetKeyFile(key)
	scaf.CatchSignals()
	scaf.SetHealthPath("/health")
	scaf.SetReadyPath("/ready")
	scaf.SetHealthChecker(server.checkHealth)
	scaf.SetMarkdown("GET", "/markdown", func() {
		// This flag tells the stop code to drop the slot.
		atomic.StoreInt32(&server.dropSlot, 1)
	})

	err = scaf.Listen(mux)
	logrus.Infof("Shutting down: %s\n", err)

	return 0
}
