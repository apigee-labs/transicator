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
package pgclient

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const (
	postgresScheme  = "postgres"
	alternateScheme = "postgresql"
	defaultHost     = "localhost"
	defaultPort     = 5432
	defaultDatabase = "postgres"
)

var hostPortExp = regexp.MustCompile("(.+):([0-9]+)$")

type connectInfo struct {
	host     string
	port     int
	database string
	user     string
	creds    string
	ssl      bool
	options  map[string]string
}

/*
parseConnectString parses a postgres connection string in the style of JDBC
into something we can use internally.
see:
https://jdbc.postgresql.org/documentation/80/connect.html
*/
func parseConnectString(c string) (*connectInfo, error) {
	p, err := url.Parse(c)
	if err != nil {
		return nil, err
	}

	if p.Scheme != postgresScheme && p.Scheme != alternateScheme {
		return nil, fmt.Errorf("Invalid scheme %s", p.Scheme)
	}

	var hostName string
	var portNum int

	match := hostPortExp.FindStringSubmatch(p.Host)
	if match == nil {
		if p.Host == "" {
			hostName = defaultHost
			portNum = defaultPort
		} else {
			hostName = p.Host
			portNum = defaultPort
		}
	} else {
		hostName = match[1]
		portStr := match[2]
		portNum, err = strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("Invalid port %s: %s", portStr, err)
		}
	}

	var database string
	if p.Path == "" || p.Path == "/" {
		database = defaultDatabase
	} else {
		database = p.Path[1:]
	}

	// Parse all "query parameters" into options
	opts := make(map[string]string)
	for paramName := range p.Query() {
		opts[paramName] = p.Query().Get(paramName)
	}

	var user string
	var pw string
	ssl := false

	if p.User != nil {
		user = p.User.Username()
		pw, _ = p.User.Password()
	}

	// "user" and "password" can override what we set before
	if opts["user"] != "" {
		user = opts["user"]
		delete(opts, "user")
	}
	if opts["password"] != "" {
		pw = opts["password"]
		delete(opts, "password")
	}

	if opts["ssl"] != "" {
		ssl = strings.EqualFold(opts["ssl"], "true")
		delete(opts, "ssl")
	}

	return &connectInfo{
		host:     hostName,
		port:     portNum,
		database: database,
		user:     user,
		creds:    pw,
		ssl:      ssl,
		options:  opts,
	}, nil
}
