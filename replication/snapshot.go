/*
Copyright 2016 Google Inc.

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
package replication

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

/*
Snapshot represents a Postgres snapshot. As per the Postgres docs, it is
constructed from a string with the values "xmin:xmax:xip1,xip2...xipn".
Each represents a transaction id, which is 32 bits and may roll over in the
lifetime of the database.
*/
type Snapshot struct {
	Xmin uint64
	Xmax uint64
	Xips map[uint64]bool
}

var snapRe = regexp.MustCompile("^([0-9]*):([0-9]*):(([0-9],?)+)?$")

/*
MakeSnapshot parses the snapshot specified in the form
"xmin:xmax:xip1,xip2...xipn" into a Snapshot object.
*/
func MakeSnapshot(snap string) (*Snapshot, error) {
	pre := snapRe.FindStringSubmatch(snap)
	if pre == nil {
		return nil, errors.New("Invalid snapshot")
	}

	xmin, err := strconv.ParseUint(pre[1], 10, 64)
	if err != nil {
		return nil, err
	}
	xmax, err := strconv.ParseUint(pre[2], 10, 64)
	if err != nil {
		return nil, err
	}

	xipss := strings.Split(pre[3], ",")
	var xips map[uint64]bool

	for _, s := range xipss {
		if s == "" {
			continue
		}
		if xips == nil {
			xips = make(map[uint64]bool)
		}
		ip, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, err
		}
		xips[ip] = true
	}

	return &Snapshot{
		Xmin: xmin,
		Xmax: xmax,
		Xips: xips,
	}, nil
}

/*
Contains tells us whether a particular transaction's changes would
be visible in the specified snapshot. It tests if they are within
the range xmin:xmax and or they were not in the "xips" list.
If this returns true, then for a given snapshot ID and TXID, the change
would be visible at the time that the snapshot was made.
*/
func (s *Snapshot) Contains(txid uint64) bool {
	if txid < s.Xmin {
		return true
	}
	if txid >= s.Xmax {
		return false
	}
	if s.Xips[txid] {
		return false
	}

	return true
}
