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
	Xmin uint32
	Xmax uint32
	Xips map[uint32]bool
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

	xmin, err := strconv.ParseUint(pre[1], 10, 32)
	if err != nil {
		return nil, err
	}
	xmax, err := strconv.ParseUint(pre[2], 10, 32)
	if err != nil {
		return nil, err
	}

	xipss := strings.Split(pre[3], ",")
	var xips map[uint32]bool

	for _, s := range xipss {
		if s == "" {
			continue
		}
		if xips == nil {
			xips = make(map[uint32]bool)
		}
		ip, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return nil, err
		}
		xips[uint32(ip)] = true
	}

	return &Snapshot{
		Xmin: uint32(xmin),
		Xmax: uint32(xmax),
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
func (s *Snapshot) Contains(txid uint32) bool {
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
