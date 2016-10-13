package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/30x/transicator/pgclient"
	"github.com/Sirupsen/logrus"
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
		logrus.SetLevel(logrus.DebugLevel)
	}

	db, err := sql.Open("transicator", pgURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to Postgres. Err : %v\n", err)
		return
	}
	pgdriver := db.Driver().(*pgclient.PgDriver)
	pgdriver.SetIsolationLevel("repeatable read")
	pgdriver.SetExtendedColumnNames(true)
	defer db.Close()

	router := httprouter.New()

	router.GET("/snapshots",
		func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
			GenSnapshot(w, r)
		})

	router.GET("/data/:snapshotid",
		func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
			DownloadSnapshot(w, r, db, p)
		})

	err = http.ListenAndServe(":"+strconv.Itoa(port), router)
	fmt.Fprintf(os.Stderr, "Is service running already?, Err: %v\n", err)
	os.Exit(4)

}
