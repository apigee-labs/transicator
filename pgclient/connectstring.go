package pgclient

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
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
	options  map[string]string
}

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

	opts := make(map[string]string)
	for paramName := range p.Query() {
		opts[paramName] = p.Query().Get(paramName)
	}

	var user string
	var pw string

	if p.User != nil {
		user = p.User.Username()
		pw, _ = p.User.Password()
	}

	return &connectInfo{
		host:     hostName,
		port:     portNum,
		database: database,
		user:     user,
		creds:    pw,
		options:  opts,
	}, nil
}
