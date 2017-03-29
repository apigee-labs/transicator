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
	"flag"
	"fmt"
	"os"

	"github.com/apigee-labs/transicator/pgclient"
)

func main() {
	var port int

	flag.IntVar(&port, "p", 5432, "Port to listen on")
	flag.Parse()
	if !flag.Parsed() {
		flag.Usage()
		return
	}

	mock, err := pgclient.NewMockServer(port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating server: %s\n", err)
		return
	}

	fmt.Printf("Mock server listening at %s\n", mock.Address())

	stopChan := make(chan bool)
	<-stopChan
}
