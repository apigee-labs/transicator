/*
Copyright 2016 The Transicator Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/30x/goscaffold"
	log "github.com/Sirupsen/logrus"
	"github.com/apigee-labs/transicator/pgclient"
	"github.com/julienschmidt/httprouter"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	packageName string = "transicator"
	appName     string = "snapshotserver"
	// Default timeout for individual Postgres transactions
	defaultPGTimeout      = 30 * time.Second
	defaultSelectorColumn = "_change_selector"
)

var selectorColumn = defaultSelectorColumn

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage:")
	pflag.PrintDefaults()
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Postgres URL must be specified.")
	fmt.Fprintln(os.Stderr, "Either one of the \"port\" or \"secureport\" must be specified")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Example:")
	fmt.Fprintln(os.Stderr,
		"snapshotserver -p 9090 -u postgres://user@host/postgres")
}

/*
 * Main entry point
 */
func main() {
	help := false

	setConfigDefaults()
	pflag.BoolVarP(&help, "help", "h", false, "Print help message")

	pflag.Parse()
	if !pflag.Parsed() || help {
		printUsage()
		os.Exit(2)
	}

	err := getConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	// Fetch config values from Viper
	port := viper.GetInt("port")
	securePort := viper.GetInt("securePort")
	mgmtPort := viper.GetInt("mgmtPort")

	pgURL := viper.GetString("pgURL")
	key := viper.GetString("key")
	cert := viper.GetString("cert")

	debug := viper.GetBool("debug")
	selectorColumn = viper.GetString("selectorColumn")

	if pgURL == "" {
		printUsage()
		os.Exit(3)
	}
	if port < 0 && securePort < 0 {
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

	router.GET("/scopes/:apidclusterId",
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
	scaf.SetSecurePort(securePort)
	if mgmtPort >= 0 {
		scaf.SetManagementPort(mgmtPort)
	}
	scaf.SetKeyFile(key)
	scaf.SetCertFile(cert)
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
