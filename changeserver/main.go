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
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/30x/goscaffold"
	"github.com/Sirupsen/logrus"
	"github.com/apigee-labs/transicator/bootstrap"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"net/url"
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
	pflag.PrintDefaults()
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Example:")
	fmt.Fprintf(os.Stderr,
		"%s -p 9000 -d ./data -u postgres://admin:secret@localhost/postgres -s replication1 -m 24h", appName)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "The \"pgurl\", \"pgslot\", and \"dbdir\" parameters must be set.")
	fmt.Fprintln(os.Stderr, "Either the port or secure port must be set.")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "  The value of the \"-m\" parameter for maximum age is a Golang")
	fmt.Fprintln(os.Stderr, "\"duration\": use \"m\", \"s\", and \"h\" for minutes, seconds, and hours")
}

func runMain() int {
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

	dbDir := viper.GetString("dbDir")
	pgURL := viper.GetString("pgURL")
	pgSlot := viper.GetString("pgSlot")
	maxAgeParam := viper.GetString("maxAgeParam")
	cert := viper.GetString("cert")
	key := viper.GetString("key")
	prefix := viper.GetString("prefix")
	selectorColumnParam := viper.GetString("selectorColumn")

	bootstrapRestoreURI := viper.GetString("boostrapRestoreURI")
	bootstrapBackupURI := viper.GetString("boostrapBackupURI")
	bootstrapID := viper.GetString("boostrapID")
	bootstrapSecret := viper.GetString("boostrapSecret")
	backupInterval := viper.GetDuration("backupInterval")

	debug := viper.GetBool("debug")

	if dbDir == "" || pgURL == "" || pgSlot == "" {
		printUsage()
		return 3
	}
	if port < 0 && securePort < 0 {
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

	if bootstrapRestoreURI != "" {
		_, err := url.Parse(bootstrapRestoreURI)
		fmt.Fprintf(os.Stderr, "Invalid value for boostrapRestoreURI: \"%s\": %s\n",
			bootstrapRestoreURI, err)
		printUsage()
		return 4
	}
	if bootstrapBackupURI != "" {
		_, err := url.Parse(bootstrapBackupURI)
		fmt.Fprintf(os.Stderr, "Invalid value for bootstrapBackupURIParam: \"%s\": %s\n",
			bootstrapBackupURI, err)
		printUsage()
		return 4
	}
	if bootstrapBackupURI != "" || bootstrapBackupURI != "" && (bootstrapID == "" || bootstrapSecret == "") {
		fmt.Fprintln(os.Stderr,
			"Error: bootstrapID and bootstrapSecret are required with boostrapRestoreURI or bootstrapBackupURIParam")
		printUsage()
		return 4
	}

	// Set the global scopeField from server.go to the user supplied value
	selectorColumn = selectorColumnParam

	if debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	if bootstrapBackupURI != "" {
		err := bootstrap.Restore(bootstrapBackupURI, bootstrapID, bootstrapSecret, dbDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error retrieving bootstrap: %s\n", err)
			return 4
		}
	}

	mux := http.NewServeMux()

	server, err := createChangeServer(mux, dbDir, pgURL, pgSlot, prefix,
		bootstrapBackupURI, bootstrapID, bootstrapSecret, backupInterval)
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
