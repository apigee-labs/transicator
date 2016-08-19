package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"github.com/30x/transicator/replication"
)

func printUsage() {
	fmt.Fprintln(os.Stderr,
		"Usage: replclient <connect string> <slot>")
	fmt.Fprintln(os.Stderr,
		"  connect = \"postgres://[user[:pw]@]host/[database]\"")
	fmt.Fprintln(os.Stderr,
		"  example: \"postgres://postgres@localhost:5432/postgres\"")
}

func main() {
	//log.SetLevel(log.DebugLevel)

	if len(os.Args) != 3 {
		printUsage()
		os.Exit(2)
	}

	connect := os.Args[1]
	slot := os.Args[2]

	repl, err := replication.Start(connect, slot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error starting replicaton: %s\n", err)
		os.Exit(3)
	}

	for {
		change := <-repl.Changes()
		if change.Error == nil {
			dec, decErr := change.Decode()
			if decErr == nil {
				indentBuf := &bytes.Buffer{}
				json.Indent(indentBuf, []byte(change.Data), "", "  ")
				fmt.Println(indentBuf.String())

				repl.Acknowledge(dec.FirstSequence)

			} else {
				fmt.Fprintf(os.Stderr, "Error decoding change: %s\n", decErr)
			}
		} else {
			fmt.Fprintf(os.Stderr, "Replication error: %s\n", change.Error)
		}
	}
}
