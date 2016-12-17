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
package snapshotserver

import (
	"database/sql"
	"errors"
	"net/http"
	"time"

	"github.com/30x/goscaffold"
	log "github.com/Sirupsen/logrus"
	"github.com/apigee-labs/transicator/pgclient"
	"github.com/julienschmidt/httprouter"
	"github.com/spf13/viper"
)

const (
	packageName string = "transicator"
	appName     string = "snapshotserver"
	// Default timeout for individual Postgres transactions
	defaultPGTimeout      = 30 * time.Second
	defaultSelectorColumn = "_change_selector"
	defaultTempDir        = ""
	tempSnapshotPrefix    = "transicatortmp"
	tempSnapshotName      = "snap"
)

// selectorColumn is the name of the database column that distinguishes a scope
var selectorColumn = defaultSelectorColumn

// tempSnapshotDir is where we'll put temporary sqlite files
var tempSnapshotDir = defaultTempDir

// ErrUsage is returned when the user passes incorrect command-line arguments.
var ErrUsage = errors.New("Invalid arguments")

var mainDB *sql.DB

/*
Run starts the snapshot server. It will listen on an HTTP port as directed
by the Viper configuration. This method will block until either there
is an error, or the server is stopped, so a goroutine is recommended
if running this as part of a unit test.
*/
func Run() (*goscaffold.HTTPScaffold, error) {
	err := getConfig()
	if err != nil {
		return nil, err
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
	tempSnapshotDir = viper.GetString("tempdir")

	if pgURL == "" {
		return nil, ErrUsage
	}
	if port < 0 && securePort < 0 {
		return nil, ErrUsage
	}

	if debug {
		log.SetLevel(log.DebugLevel)
	}

	log.Infof("Connecting to Postgres DB %s\n", pgURL)
	mainDB, err = sql.Open("transicator", pgURL)
	if err != nil {
		return nil, errors.New("Unable to initialize database connection")
	}
	err = mainDB.Ping()
	if err != nil {
		log.Warnf("Warning: Postgres DB Err: %v\n", err)
		log.Warn("Continuing anyway...")
	}

	log.Info("Connection to Postgres succeeded.\n")
	pgdriver := mainDB.Driver().(*pgclient.PgDriver)
	pgdriver.SetIsolationLevel("repeatable read")
	pgdriver.SetExtendedColumnNames(true)
	pgdriver.SetReadTimeout(defaultPGTimeout)
	router := httprouter.New()

	router.GET("/scopes/:apidclusterId",
		func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
			GetScopes(w, r, mainDB, p)
		})

	router.GET("/snapshots",
		func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
			GenSnapshot(w, r)
		})

	router.GET("/data",
		func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
			DownloadSnapshot(w, r, mainDB, p)
		})

	scaf := goscaffold.CreateHTTPScaffold()
	scaf.SetInsecurePort(port)
	scaf.SetSecurePort(securePort)
	if mgmtPort >= 0 {
		scaf.SetManagementPort(mgmtPort)
	}
	scaf.SetKeyFile(key)
	scaf.SetCertFile(cert)
	scaf.SetHealthPath("/health")
	scaf.SetReadyPath("/ready")
	scaf.SetHealthChecker(func() (goscaffold.HealthStatus, error) {
		return checkHealth(mainDB)
	})
	scaf.SetMarkdown("GET", "/markdown", nil)

	err = scaf.StartListen(router)
	return scaf, err
}

/*
Close closes the database and does other necessary cleanup.
*/
func Close() {
	if mainDB != nil {
		mainDB.Close()
	}
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
