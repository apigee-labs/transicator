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
	"time"
)

const (
	postgresScheme  = "postgres"
	alternateScheme = "postgresql"
	defaultHost     = "localhost"
	defaultPort     = "5432"
	defaultDatabase = "postgres"
)

type sslMode int

const (
	sslPrefer     sslMode = 0
	sslDisable    sslMode = 1
	sslAllow      sslMode = 2
	sslRequire    sslMode = 3
	sslVerifyCA   sslMode = 4
	sslVerifyFull sslMode = 5
)

var hostPortExp = regexp.MustCompile("(.+):([0-9]+)$")

type connectInfo struct {
	host           string
	port           int
	database       string
	user           string
	creds          string
	ssl            sslMode
	connectTimeout *time.Duration
	keepAlive      bool
	keepAliveIdle  *time.Duration
	options        map[string]string
}

/*
parseConnectString supports the style of URL that the standard "libpq"
supports.
https://www.postgresql.org/docs/9.6/static/libpq-connect.html
Status:

host: Supported
hostaddr: Not supported
port: Supported
dbname: Supported
user: Supported
password: Supported
connect_timeout: Not supported
client_encoding: Not supported
options: Not supported
application_name: Not supported
fallback_application_name: Not supported
keepalives, keepalives_idle: Supported
keepalives_interval, keepalives_count: Ignored (no Go platform support)
tty: Ignored (as per docs)
sslmode: Supported, not not for "verify"
requiressl: Supported
ssl: Supported (true == "require", false == "prefer")
sslcompression: Not supported
sslcert, sslkey: Not supported
sslrootcert, sslcrl: Not supported
requirepeer: Not supported
gsslib: Not supported
service: Not supported
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
	var portName string

	match := hostPortExp.FindStringSubmatch(p.Host)
	if match == nil {
		if p.Host == "" {
			hostName = defaultHost
			portName = defaultPort
		} else {
			hostName = p.Host
			portName = defaultPort
		}
	} else {
		hostName = match[1]
		portName = match[2]
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
	sslMode := sslPrefer
	keepAlive := true
	var keepAliveIdle *time.Duration
	var connectTimeout *time.Duration

	if p.User != nil {
		user = p.User.Username()
		pw, _ = p.User.Password()
	}

	// Many query parameter options override existing settings
	if opts["user"] != "" {
		user = opts["user"]
		delete(opts, "user")
	}
	if opts["password"] != "" {
		pw = opts["password"]
		delete(opts, "password")
	}
	if opts["host"] != "" {
		hostName = opts["host"]
		delete(opts, "host")
	}
	if opts["port"] != "" {
		portName = opts["port"]
		delete(opts, "port")
	}
	if opts["dbname"] != "" {
		database = opts["dbname"]
		delete(opts, "dbname")
	}
	if opts["ssl"] != "" {
		if strings.EqualFold(opts["ssl"], "true") {
			sslMode = sslRequire
		}
		delete(opts, "ssl")
	}
	if opts["requiressl"] != "" {
		if opts["requiressl"] == "1" {
			sslMode = sslRequire
		}
		delete(opts, "ssl")
	}
	if opts["sslmode"] != "" {
		switch opts["sslmode"] {
		case "disable":
			sslMode = sslDisable
		case "allow":
			sslMode = sslAllow
		case "prefer":
			sslMode = sslPrefer
		case "require":
			sslMode = sslRequire
		case "verify-ca":
			sslMode = sslVerifyCA
		case "verify-full":
			sslMode = sslVerifyFull
		default:
			return nil, fmt.Errorf("Invalid ssl mode \"%s\"", opts["sslmode"])
		}
		delete(opts, "sslmode")
	}
	if opts["keepalives"] != "" {
		if opts["keepalives"] == "1" {
			keepAlive = true
		} else {
			keepAlive = false
		}
		delete(opts, "keepalives")
	}
	if opts["keepalives_idle"] != "" {
		var secs int
		secs, err = strconv.Atoi(opts["keepalives_idle"])
		if err != nil {
			return nil, fmt.Errorf("Invalid keepalive interval %s: %s\n", opts["keepalives_idle"], err)
		}
		kil := time.Duration(secs) * time.Second
		keepAliveIdle = &kil
	}
	if opts["connect_timeout"] != "" {
		var secs int
		secs, err = strconv.Atoi(opts["connect_timeout"])
		if err != nil {
			return nil, fmt.Errorf("Invalid connect timeout %s: %s\n", opts["connect_timeout"], err)
		}
		ct := time.Duration(secs) * time.Second
		connectTimeout = &ct
	}

	portNum, err := strconv.Atoi(portName)
	if err != nil {
		return nil, fmt.Errorf("Invalid port %s: %s", portName, err)
	}

	return &connectInfo{
		host:           hostName,
		port:           portNum,
		database:       database,
		user:           user,
		creds:          pw,
		ssl:            sslMode,
		keepAlive:      keepAlive,
		keepAliveIdle:  keepAliveIdle,
		connectTimeout: connectTimeout,
		options:        opts,
	}, nil
}
