package main

import (
	"flag"
	"fmt"
	"os"
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
	var dbDir string
	var pgURL string
	var pgSlot string
	var help bool

	flag.IntVar(&port, "p", defaultPort, "Listen port")
	flag.StringVar(&dbDir, "d", "", "Location of database files")
	flag.StringVar(&pgURL, "u", "", "URL to connect to Postgres")
	flag.StringVar(&pgSlot, "s", "", "Slot name for Postgres logical replication")
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

	fmt.Fprintln(os.Stderr, "Not done yet")
	return 999
	/*
		server, err := startChangeServer(port, dbDir, pgURL, pgSlot)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error starting server: %s\n", err)
			return 4
		}

		fmt.Fprintln(os.Stderr, "Not done -- need to listen here!")
		return 999
	*/
}
