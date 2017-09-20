// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package goscaffold

import (
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var qParamRe = regexp.MustCompile("^q=([0-9\\.]+)$")

type acceptCriterion struct {
	major      string
	minor      string
	precedence float32
	origOrder  int
	match      string
}
type acceptCriteria []acceptCriterion

/*
SelectMediaType matches a set of candidate media types against the
Accept header in an HTTP request. It does this using the rules from
RFC2616 section 1.4.
If no Accept header is present, then the first choice in the
"choices" array is returned.
If multiple choices match, then we will take the first one specified
in the "Accept" header.
If there are no compatible media types, then an empty string
is returned.
Only the first "Accept" header on the request is considered.
*/
func SelectMediaType(req *http.Request, choices []string) string {
	hdr := strings.TrimSpace(req.Header.Get("Accept"))
	if hdr == "" || hdr == "*" || hdr == "*/*" {
		if len(choices) >= 1 {
			return choices[0]
		}
		return ""
	}

	// Parse accept header and parse the criteria
	candidates := parseAcceptHeader(hdr)

	// For each choice, assign the best match (least wildcardy)
	matches := make(map[string]*acceptCriterion)
	for _, choice := range choices {
		for c := range candidates {
			crit := candidates[c]
			if crit.matches(choice) {
				if matches[choice] == nil ||
					matches[choice].level() < crit.level() {
					matches[choice] = &acceptCriterion{
						minor:      crit.minor,
						major:      crit.major,
						precedence: crit.precedence,
						origOrder:  crit.origOrder,
						match:      choice,
					}
				}
			}
		}
	}

	if len(matches) == 0 {
		return ""
	}

	// Sort the matches now by precedence level and original order
	var sortedMatches acceptCriteria
	for _, v := range matches {
		sortedMatches = append(sortedMatches, *v)
	}
	sortedMatches.sort()

	return sortedMatches[0].match
}

func parseAcceptHeader(hdr string) acceptCriteria {
	var ret acceptCriteria
	parts := strings.Split(hdr, ",")
	for i, part := range parts {
		candidate := parseAcceptPart(part, i)
		ret = append(ret, candidate)
	}
	return ret
}

/*
parseAcceptPart parses a single section of the header and extracts the
"q" parameter.
*/
func parseAcceptPart(part string, order int) acceptCriterion {
	params := strings.Split(part, ";")
	finalType := strings.TrimSpace(params[0])
	var precedence float32 = 1.0

	for _, param := range params[1:] {
		match := qParamRe.FindStringSubmatch(strings.TrimSpace(param))
		if match == nil {
			finalType += ";" + param
		} else {
			qVal, err := strconv.ParseFloat(match[1], 32)
			if err == nil {
				precedence = float32(qVal)
			}
		}
	}

	splitType := strings.SplitN(finalType, "/", 2)

	return acceptCriterion{
		major:      splitType[0],
		minor:      splitType[1],
		precedence: precedence,
		origOrder:  order,
	}
}

/*
matches matches a candidate media type with a criterion.
*/
func (a acceptCriterion) matches(t string) bool {
	st := strings.SplitN(t, "/", 2)

	if a.major != "*" && a.major != st[0] {
		return false
	}
	if a.minor != "*" && a.minor != st[1] {
		return false
	}
	return true
}
func (a acceptCriterion) level() int {
	if a.minor == "*" {
		if a.major == "*" {
			return 0
		}
		return 1
	}
	return 2
}

/*
sortCandidates sorts accept header candidates in order of:
1) Precedence (from the "q" parameter)
2) Original order in accept header
3) Stable sort otherwise
*/
func (c acceptCriteria) sort() {
	sort.Stable(c)
}

func (c acceptCriteria) Less(i, j int) bool {
	// Higher precedence goes first
	if c[i].precedence > c[j].precedence {
		return true
	}
	if c[i].precedence == c[j].precedence &&
		c[i].origOrder < c[j].origOrder {
		return true
	}
	return false
}

func (c acceptCriteria) Len() int {
	return len(c)
}

func (c acceptCriteria) Swap(i, j int) {
	tmp := c[i]
	c[i] = c[j]
	c[j] = tmp
}
