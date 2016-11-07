package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/30x/goscaffold"
	log "github.com/Sirupsen/logrus"
	"github.com/apigee-labs/transicator/pgclient"
	"github.com/julienschmidt/httprouter"
)

const (
	// Default listen port
	defaultPort = 9090
	// Default timeout for individual Postgres transactions
	defaultPGTimeout = 30 * time.Second
)

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage:")
	flag.PrintDefaults()
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Example:")
	fmt.Fprintln(os.Stderr,
		"snapshotserver -p 9090 -u postgres://user@host/postgres")
}

/*
 * Main entry point
 */
func main() {

	var (
		port     int
		mgmtPort int
		pgURL    string
		debug    bool
		help     bool
	)

	flag.IntVar(&port, "p", defaultPort, "Local Binding port")
	flag.IntVar(&mgmtPort, "mp", -1, "Management port (for health checks)")
	flag.StringVar(&pgURL, "u", "", "URL to connect to Postgres DB")
	flag.BoolVar(&debug, "D", false, "Turn on debugging")
	flag.BoolVar(&help, "h", false, "Print help message")
	flag.Parse()

	if help || !flag.Parsed() {
		printUsage()
		os.Exit(2)
	}
	if pgURL == "" {
		fmt.Fprintln(os.Stderr, "-d, -u, and -s parameters are all required")
		printUsage()
		os.Exit(3)
	}

	if debug {
		log.SetLevel(log.DebugLevel)
	}

	log.Infof("Connecting to Postgres DB %s\n", pgURL)
	db, err := sql.Open("transicator", pgURL)
	if err != nil {
		log.Warnf("Unable to connect to Postgres. Err : %v\n", err)
		return
	}
	err = db.Ping()
	if err != nil {
		log.Warnf("Warning: Postgres DB Err: %v\n", err)
		log.Warn("Continuing anyway...")
	}

	log.Info("Connection to Postgres succeeded.\n")
	pgdriver := db.Driver().(*pgclient.PgDriver)
	pgdriver.SetIsolationLevel("repeatable read")
	pgdriver.SetExtendedColumnNames(true)
	pgdriver.SetReadTimeout(defaultPGTimeout)
	defer db.Close()

	router := httprouter.New()

	router.GET("/scopes/:apidconfigId",
		func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
			GetScopes(w, r, db, p)
		})

	router.GET("/snapshots",
		func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
			GenSnapshot(w, r)
		})

	router.GET("/data",
		func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
			DownloadSnapshot(w, r, db, p)
		})

	scaf := goscaffold.CreateHTTPScaffold()
	scaf.SetInsecurePort(port)
	if mgmtPort >= 0 {
		scaf.SetManagementPort(mgmtPort)
	}
	scaf.CatchSignals()
	scaf.SetHealthPath("/health")
	scaf.SetReadyPath("/ready")
	scaf.SetHealthChecker(func() (goscaffold.HealthStatus, error) {
		return checkHealth(db)
	})
	scaf.SetMarkdown("GET", "/markdown", nil)

	log.Infof("Listening on port %d", port)
	err = scaf.Listen(router)
	log.Infof("Shutting down: %s", err)
}

func checkHealth(db *sql.DB) (goscaffold.HealthStatus, error) {
	row := db.QueryRow("select * from now()")
	var now string
	err := row.Scan(&now)
	if err == nil {
		return goscaffold.OK, nil
	}

	log.Warnf("Not ready: Database error: %s", err)
	// Return a "Not ready" status. That means that the server should not
	// receive calls, but it does not need a restart. Don't return a "failed"
	// status here that would cause a restart. We will be able to reach PG
	// again when it's ready.
	return goscaffold.NotReady, err
}
