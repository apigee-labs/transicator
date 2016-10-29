package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"

	"github.com/30x/goscaffold"
	"github.com/Sirupsen/logrus"
)

const (
	defaultPort      = 9000
	defaultCacheSize = 65536
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
	var mgmtPort int
	var dbDir string
	var pgURL string
	var pgSlot string
	var prefix string
	var debug bool
	var help bool

	flag.IntVar(&port, "p", defaultPort, "Listen port")
	flag.IntVar(&mgmtPort, "mp", -1, "Management port (for health checks)")
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

	scaf := goscaffold.CreateHTTPScaffold()
	scaf.SetInsecurePort(port)
	if mgmtPort >= 0 {
		scaf.SetManagementPort(mgmtPort)
	}
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
