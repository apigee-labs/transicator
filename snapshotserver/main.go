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
	"github.com/spf13/viper"
)

const (
	packageName string = "transicator"
	appName     string = "snapshotserver"
	// Default timeout for individual Postgres transactions
	defaultPGTimeout  = 30 * time.Second
	defaultScopeField = "_apid_scope"
)

var scopeField = defaultScopeField

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

	// Define legacy GO Flags
	flag.Int("p", -1, "Local Binding port")
	flag.Int("sp", -1, "HTTP listen port")
	flag.Int("mp", -1, "Management port (for health checks)")
	flag.String("u", "", "URL to connect to Postgres DB")
	flag.String("key", "", "TLS key file (must be unencrypted)")
	flag.String("cert", "", "TLS certificate file")
	flag.Bool("C", false, fmt.Sprintf("Use a config file named '%s' located in either /etc/%s/, ~/.%s or ./)", appName, packageName, packageName))
	flag.Bool("D", false, "Turn on debugging")
	flag.Bool("h", false, "Print help message")
	flag.String("S", "", "Set scope field")
	flag.Parse()

	// Viper
	err := getConfig(flag.CommandLine)
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
	help := viper.GetBool("help")
	scopeFieldParam := viper.GetString("scopeField")

	if help || !flag.Parsed() {
		printUsage()
		os.Exit(2)
	}
	if pgURL == "" {
		fmt.Fprintln(os.Stderr, "-u parameters is required")
		printUsage()
		os.Exit(3)
	}
	if port < 0 && securePort < 0 {
		fmt.Fprintln(os.Stderr, "Either -p or -sp is required")
		printUsage()
		os.Exit(3)
	}

	if debug {
		log.SetLevel(log.DebugLevel)
	}

	scopeField = scopeFieldParam

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
