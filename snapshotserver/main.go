package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/30x/transicator/pgclient"
	log "github.com/Sirupsen/logrus"
	"github.com/julienschmidt/httprouter"
)

const (
	defaultPort = 9090
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
		port  int
		pgURL string
		debug bool
		help  bool
	)

	flag.IntVar(&port, "p", defaultPort, "Local Binding port")
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

	router.GET("/data/:snapshotid",
		func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
			DownloadSnapshot(w, r, db, p)
		})

	err = http.ListenAndServe(":"+strconv.Itoa(port), router)
	log.Warnf("Is service running already?, Err: %v\n", err)
	os.Exit(4)

}
