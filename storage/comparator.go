package storage

import (
	"fmt"
	"github.com/apigee-labs/transicator/common"
	"strings"
)

// Comparator names are persisted so should be consistent
const (
	EntryComparatorName    = "transicator-entries-v1"
	SequenceComparatorName = "transicator-sequence-v1"
)

var entryComparator = new(entryCmp)
var sequenceComparator = new(sequenceCmp)

type entryCmp struct {
}

/*
Compare tests the order of two keys in the "entries" collection. Keys are sorted
primarily in scope order, then by LSN, then by index within the LSN. This
allows searches to be linear for a given scope.
*/
func (c entryCmp) Compare(a, b []byte) int {
	aScope, aLsn, aIndex, err := keyToLsnAndOffset(a)
	if err != nil {
		panic(fmt.Sprintf("Error parsing database key: %s", err))
	}
	bScope, bLsn, bIndex, err := keyToLsnAndOffset(b)
	if err != nil {
		panic(fmt.Sprintf("Error parsing database key: %s", err))
	}

	scopeCmp := strings.Compare(aScope, bScope)
	if scopeCmp == 0 {
		if aLsn < bLsn {
			return -1
		} else if aLsn > bLsn {
			return 1
		}

		if aIndex < bIndex {
			return -1
		} else if aIndex > bIndex {
			return 1
		}
		return 0
	}
	return scopeCmp
}

/*
Name is part of the comparator interface.
*/
func (c entryCmp) Name() string {
	return EntryComparatorName
}

type sequenceCmp struct {
}

func (s sequenceCmp) Compare(a, b []byte) int {
	s1, err := common.ParseSequenceBytes(a)
	if err != nil {
		panic(fmt.Sprintf("Error parsing sequence: %s", err))
	}
	s2, err := common.ParseSequenceBytes(b)
	if err != nil {
		panic(fmt.Sprintf("Error parsing sequence: %s", err))
	}

	return s1.Compare(s2)
}

func (s sequenceCmp) Name() string {
	return SequenceComparatorName
}
