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
	"github.com/spf13/viper"
)

const (
	packageName      string = "transicator"
	appName          string = "changeserver"
	defaultCacheSize        = 65536
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
	fmt.Fprintf(os.Stderr,
		"%s -p 9000 -d ./data -u postgres://admin:secret@localhost/postgres -s replication1 -m 24h", appName)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "  The value of the \"-m\" parameter for maximum age is a Golang")
	fmt.Fprintln(os.Stderr, "\"duration\": use \"m\", \"s\", and \"h\" for minutes, seconds, and hours")
}

func runMain() int {

	// Define legacy GO Flags
	flag.Int("p", -1, "Listen port")
	flag.Int("sp", -1, "HTTPS Listen port")
	flag.Int("mp", -1, "Management port (for health checks)")
	flag.String("d", "", "Location of database files")
	flag.String("u", "", "URL to connect to Postgres")
	flag.String("s", "", "Slot name for Postgres logical replication")
	flag.String("m", "", "Purge records older than this age.")
	flag.String("cert", "", "TLS certificate PEM file")
	flag.String("key", "", "TLS key PEM file (must be unencrypted)")
	flag.String("P", "", "Optional prefix URL for all API calls")
	flag.Bool("C", false, fmt.Sprintf("Use a config file named '%s' located in either /etc/%s/, ~/.%s or ./)", appName, packageName, packageName))
	flag.Bool("D", false, "Turn on debugging")
	flag.Bool("h", false, "Print help message")
	flag.Parse()

	err := GetConfig(flag.CommandLine)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}

	// Fetch config values from Viper
	port := viper.GetInt("port")
	securePort := viper.GetInt("securePort")
	mgmtPort := viper.GetInt("mgmtPort")

	dbDir := viper.GetString("dbDir")
	pgURL := viper.GetString("pgURL")
	pgSlot := viper.GetString("pgSlot")
	maxAgeParam := viper.GetString("maxAgeParam")
	cert := viper.GetString("cert")
	key := viper.GetString("key")
	prefix := viper.GetString("prefix")

	debug := viper.GetBool("debug")
	help := viper.GetBool("help")

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
