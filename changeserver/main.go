package main

import "os"

func main() {
	exitCode := runMain()
	os.Exit(exitCode)
}

func runMain() int {
  var port int
  var dbDir string
  var pgHost string
  var pgUser string
  var pgDB string
  var pgSlot string
  var help bool

  flag.IntVar(&port, 
}
