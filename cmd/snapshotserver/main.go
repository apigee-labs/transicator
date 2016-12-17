/*
Copyright 2017 The Transicator Authors

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
	"os"

	"github.com/apigee-labs/transicator/snapshotserver"
	"github.com/spf13/pflag"
)

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

func main() {
	var help bool

	snapshotserver.SetConfigDefaults()
	pflag.BoolVarP(&help, "help", "h", false, "Print help message")

	pflag.Parse()
	if !pflag.Parsed() || help {
		printUsage()
		os.Exit(2)
	}

	listener, err := snapshotserver.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %s\n", err)
		printUsage()
		os.Exit(3)
	}
	listener.WaitForShutdown()
}
