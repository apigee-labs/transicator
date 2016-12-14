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
package common

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
)

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
UnmarshalSnapshotProto unmarshals a snapshot that was written using the
Streaming snapshot interface in protobuf format into a single object.
This method should not be used for a very large snapshot because it will
result in the entire snapshot contents being read into memory.
For flexibility, this reads from a Reader so that there is no need
to make extra copies.
*/
func UnmarshalSnapshotProto(r io.Reader) (*Snapshot, error) {
	sr, err := CreateSnapshotReader(r)
	if err != nil {
		return nil, err
	}

	s := Snapshot{
		SnapshotInfo: sr.SnapshotInfo(),
		Timestamp:    sr.Timestamp(),
	}

	var curTable *Table

	// Read the snapshot in streaming form, and build it all into a single
	// Snapshot object.
	for sr.Next() {
		e := sr.Entry()
		switch e.(type) {
		case TableInfo:
			// Since curTable is a pointer, we have to do the append at the end
			if curTable != nil {
				s.Tables = append(s.Tables, *curTable)
			}
			ti := e.(TableInfo)
			curTable = &Table{
				Name: ti.Name,
			}
		case Row:
			r := e.(Row)
			curTable.Rows = append(curTable.Rows, r)
		case error:
			return nil, err
		default:
			return nil, fmt.Errorf("Unexpected type %T in snapshot", e)
		}
	}

	if curTable != nil {
		s.Tables = append(s.Tables, *curTable)
	}

	return &s, nil
}

/*
Marshal turns a snapshot into formatted, indented JSON. It will panic
on a marshaling error.
*/
func (s *Snapshot) Marshal() []byte {
	data, err := json.MarshalIndent(s.stringify(), indentPrefix, indent)
	if err == nil {
		return data
	}
	panic(err.Error())
}

/*
stringify ensures that all the values in the table are strings.
*/
func (s *Snapshot) stringify() *Snapshot {
	var nt []Table
	for _, t := range s.Tables {
		ntt := t.stringify()
		nt = append(nt, ntt)
	}
	ns := *s
	ns.Tables = nt
	return &ns
}

func (t Table) stringify() Table {
	var nr []Row
	for _, r := range t.Rows {
		nr = append(nr, r.stringify())
	}

	nt := t
	nt.Rows = nr
	return nt
}

/*
MarshalProto uses the streaming replication option to write out an existing
Snapshot as a protocol buffer. It should not be used for large
snapshots because it will result in the entire snapshot being
written into memory.
It returns an error if there is an error writing to the actual Writer.
*/
func (s *Snapshot) MarshalProto(w io.Writer) error {
	sw, err := CreateSnapshotWriter(
		s.Timestamp, s.SnapshotInfo, w)
	if err != nil {
		return err
	}

	for _, table := range s.Tables {
		if len(table.Rows) == 0 {
			continue
		}

		var cols []ColumnInfo
		for rowName, rowVal := range table.Rows[0] {
			col := ColumnInfo{
				Name: rowName,
				Type: rowVal.Type,
			}
			cols = append(cols, col)
		}
		err = sw.StartTable(table.Name, cols)
		if err != nil {
			return err
		}

		for _, row := range table.Rows {
			var vals []interface{}
			for _, colInfo := range cols {
				rv := row[colInfo.Name]
				var val interface{}
				if rv != nil {
					val = rv.Value
				}
				vals = append(vals, val)
			}
			err = sw.WriteRow(vals)
			if err != nil {
				return err
			}
		}

		sw.EndTable()
	}

	return nil
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
UnmarshalChangeListProto does the same unmarshaling but from a protobuf.
*/
func UnmarshalChangeListProto(data []byte) (*ChangeList, error) {
	var clpb ChangeListPb
	err := proto.Unmarshal(data, &clpb)
	if err != nil {
		return nil, err
	}

	cl := ChangeList{
		FirstSequence: clpb.GetFirstSequence(),
		LastSequence:  clpb.GetLastSequence(),
	}

	for _, cpb := range clpb.GetChanges() {
		c := unconvertChangeProto(cpb)
		cl.Changes = append(cl.Changes, *c)
	}

	return &cl, nil
}

/*
Marshal turns a change list into formatted, indented JSON. It will panic
on a marshaling error.
*/
func (l *ChangeList) Marshal() []byte {
	data, err := json.MarshalIndent(l.stringify(), indentPrefix, indent)
	if err == nil {
		return data
	}
	panic(err.Error())
}

/*
stringify turns the change list into one in which all values are represented
as strings.
*/
func (l *ChangeList) stringify() *ChangeList {
	var nc []Change
	for _, c := range l.Changes {
		nch := c.stringify()
		nc = append(nc, *nch)
	}

	rl := *l
	rl.Changes = nc
	return &rl
}

/*
MarshalProto does the same as Marshal but it makes a protobf.
*/
func (l *ChangeList) MarshalProto() []byte {
	pb := &ChangeListPb{
		FirstSequence: proto.String(l.FirstSequence),
		LastSequence:  proto.String(l.LastSequence),
	}

	for _, c := range l.Changes {
		cpb := c.convertProto()
		pb.Changes = append(pb.Changes, cpb)
	}

	buf, err := proto.Marshal(pb)
	if err != nil {
		panic(err.Error())
	}
	return buf
}

/*
UnmarshalChange just handles a single "Change." This is what we get from the
Postgres replication stream.
*/
func UnmarshalChange(data []byte) (*Change, error) {
	var c Change
	err := json.Unmarshal(data, &c)
	if err == nil {
		return &c, nil
	}
	return nil, err
}

/*
UnmarshalChangeProto turns a protobuf version of a Change into a Change.
*/
func UnmarshalChangeProto(data []byte) (*Change, error) {
	var cp ChangePb
	err := proto.Unmarshal(data, &cp)
	if err != nil {
		return nil, err
	}

	return unconvertChangeProto(&cp), nil
}

func unconvertChangeProto(cp *ChangePb) *Change {
	c := &Change{
		Operation:      Operation(cp.GetOperation()),
		Table:          cp.GetTable(),
		Sequence:       cp.GetSequence(),
		CommitSequence: cp.GetCommitSequence(),
		CommitIndex:    cp.GetCommitIndex(),
		ChangeSequence: cp.GetChangeSequence(),
		Timestamp:      cp.GetTimestamp(),
	}

	if cp.GetTransactionIDEpoch() > 0 {
		c.TransactionID = cp.GetTransactionIDEpoch()
	} else {
		c.TransactionID = uint64(cp.GetTransactionID())
	}

	if len(cp.GetNewColumns()) > 0 {
		c.NewRow = makeRow(cp.GetNewColumns())
	}
	if len(cp.GetOldColumns()) > 0 {
		c.OldRow = makeRow(cp.GetOldColumns())
	}
	return c
}

func makeRow(cols []*ColumnPb) Row {
	row := make(map[string]*ColumnVal)
	for _, colPb := range cols {
		cv := &ColumnVal{
			Type:  colPb.GetType(),
			Value: unwrapColumnVal(colPb.GetValue()),
		}
		row[colPb.GetName()] = cv
	}
	return row
}

func unwrapColumnVal(v *ValuePb) interface{} {
	if v == nil {
		return nil
	}
	pv := v.GetValue()
	if pv == nil {
		return nil
	}
	switch pv.(type) {
	case *ValuePb_String_:
		return v.GetString_()
	case *ValuePb_Int:
		return v.GetInt()
	case *ValuePb_Uint:
		return v.GetUint()
	case *ValuePb_Double:
		return v.GetDouble()
	case *ValuePb_Bool:
		return v.GetBool()
	case *ValuePb_Bytes:
		return v.GetBytes()
	case *ValuePb_Timestamp:
		return PgTimestampToTime(v.GetTimestamp())
	default:
		panic("Invalid data type in protobuf")
	}
}

/*
Marshal turns a Change into JSON.
*/
func (c *Change) Marshal() []byte {
	data, err := json.MarshalIndent(c.stringify(), indentPrefix, indent)
	if err == nil {
		return data
	}
	panic(err.Error())
}

/*
stringify turns all values into a string so that JSON encoding is consistent.
*/
func (c *Change) stringify() *Change {
	r := *c
	if r.NewRow != nil {
		r.NewRow = c.NewRow.stringify()
	}
	if r.OldRow != nil {
		r.OldRow = c.OldRow.stringify()
	}
	return &r
}

func (r Row) stringify() Row {
	nr := make(map[string]*ColumnVal)
	for k, v := range r {
		nv := *v
		if nv.Value == nil {
			nv.Value = nil
		} else {
			nv.Value = nv.String()
		}
		nr[k] = &nv
	}
	return Row(nr)
}

/*
MarshalProto turns a Change into a protobuf.
*/
func (c *Change) MarshalProto() []byte {
	buf, err := proto.Marshal(c.convertProto())
	if err != nil {
		panic(err.Error())
	}
	return buf
}

func (c *Change) convertProto() *ChangePb {
	cp := &ChangePb{
		Operation: proto.Int32(int32(c.Operation)),
		Table:     proto.String(c.Table),
	}
	if c.Sequence != "" {
		cp.Sequence = proto.String(c.Sequence)
	}
	if c.CommitSequence != 0 {
		cp.CommitSequence = proto.Uint64(c.CommitSequence)
	}
	if c.ChangeSequence != 0 {
		cp.ChangeSequence = proto.Uint64(c.ChangeSequence)
	}
	if c.CommitIndex != 0 {
		cp.CommitIndex = proto.Uint32(c.CommitIndex)
	}
	if c.TransactionID != 0 {
		cp.TransactionIDEpoch = proto.Uint64(c.TransactionID)
	}
	if c.Timestamp != 0 {
		cp.Timestamp = proto.Int64(c.Timestamp)
	}
	cp.NewColumns = unmakeRow(c.NewRow)
	cp.OldColumns = unmakeRow(c.OldRow)
	return cp
}

func unmakeRow(row Row) []*ColumnPb {
	var cols []*ColumnPb
	for name, v := range row {
		cpb := &ColumnPb{
			Name: proto.String(name),
		}
		if v != nil {
			v := &ValuePb{
				Value: convertParameter(v.Value),
			}
			cpb.Value = v
		}
		if v.Type != 0 {
			cpb.Type = proto.Int32(v.Type)
		}
		cols = append(cols, cpb)
	}
	return cols
}

/*
AddTables is a helper function that inserts Tables in to a existing Snapshot
*/
func (s *Snapshot) AddTables(tb Table) []Table {
	s.Tables = append(s.Tables, tb)
	return s.Tables
}

/*
AddRowstoTable is a helper function that inserts rows to an existing table
*/
func (t *Table) AddRowstoTable(rv Row) []Row {
	t.Rows = append(t.Rows, rv)
	return t.Rows
}
