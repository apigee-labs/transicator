package main

import (
	"fmt"
	"io"
	"os"

	"github.com/30x/transicator/pgclient"
	"github.com/30x/transicator/snapshot"
	log "github.com/Sirupsen/logrus"
)

func printUsage() {
	fmt.Fprintln(os.Stderr,
		"Usage: snapclient <connect string> <tenant id> <dest dir>")
	fmt.Fprintln(os.Stderr,
		"  connect = \"postgres://[user[:pw]@]host/[database]\"")
	fmt.Fprintln(os.Stderr,
		"  example: \"postgres://postgres@localhost:5432/postgres\"")
}

func main() {
	//log.SetLevel(log.DebugLevel)

	if len(os.Args) != 4 {
		printUsage()
		os.Exit(2)
	}
	connect := os.Args[1]
	tenantId := os.Args[2]
	destDir := os.Args[3]

	snapInfo, data, err := snapshot.GetTenantSnapshotData(connect, tenantId, pgclient.CopyFormatCsv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetOrgSnapshot error: %v\n", err)
		os.Exit(3)
	}
	if data != nil {
		defer data.Close()
	}
	log.Infof("Got snapshot: %s", snapInfo)
	if err = os.MkdirAll(destDir, 0700); err != nil {
		fmt.Fprintf(os.Stderr, "Failed dest directory creation: %v\n", err)
		return
	}
	destFileName := tenantId + ".csv"
	destFilePath := destDir + "/" + destFileName
	f, err := os.Create(destFilePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed dest file creation: %v\n", err)
	}

	if _, err := io.Copy(f, data); err != nil {
		fmt.Fprintf(os.Stderr, "Failed dest file copy: %v\n", err)
	}
	log.Infof("Snapshot complete at %s", destFilePath)
	return
}
