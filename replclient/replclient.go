package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"

	"github.com/apigee-internal/transicator/replication"
)

func printUsage() {
	fmt.Fprintln(os.Stderr,
		"Usage: replclient <host> <port> <user> <database> <slot>")
}

func main() {
	//log.SetLevel(log.DebugLevel)

	if len(os.Args) != 6 {
		printUsage()
		os.Exit(2)
	}

	hostName := os.Args[1]
	portStr := os.Args[2]
	user := os.Args[3]
	database := os.Args[4]
	slot := os.Args[5]

	repl, err := replication.Start(
		net.JoinHostPort(hostName, portStr), user, database, slot)
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
