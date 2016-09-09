package common

import "encoding/json"

const (
	indentPrefix = ""
	indent       = "  "
)

/*
UnmarshalSnapshot turns a set of JSON into an entire snapshot.
*/
func UnmarshalSnapshot(data []byte) (*Snapshot, error) {
	var s Snapshot
	err := json.Unmarshal(data, &s)
	if err == nil {
		return &s, nil
	}
	return nil, err
}

/*
Marshal turns a snapshot into formatted, indented JSON. It will panic
on a marshaling error.
*/
func (s *Snapshot) Marshal() []byte {
	data, err := json.MarshalIndent(s, indentPrefix, indent)
	if err == nil {
		return data
	}
	panic(err.Error())
}

/*
UnmarshalChangeList turns a set of JSON into an entire change list.
*/
func UnmarshalChangeList(data []byte) (*ChangeList, error) {
	var l ChangeList
	err := json.Unmarshal(data, &l)
	if err == nil {
		return &l, nil
	}
	return nil, err
}

/*
Marshal turns a snapshot into formatted, indented JSON. It will panic
on a marshaling error.
*/
func (l *ChangeList) Marshal() []byte {
	data, err := json.MarshalIndent(l, indentPrefix, indent)
	if err == nil {
		return data
	}
	panic(err.Error())
}
