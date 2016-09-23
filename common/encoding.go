package common

import (
	"encoding/json"

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
	c := &Change{
		Operation:      Operation(cp.GetOperation()),
		Table:          cp.GetTable(),
		Sequence:       cp.GetSequence(),
		CommitSequence: cp.GetCommitSequence(),
		CommitIndex:    cp.GetCommitIndex(),
		ChangeSequence: cp.GetChangeSequence(),
		TransactionID:  cp.GetTransactionID(),
	}

	if len(cp.GetNewColumns()) > 0 {
		c.NewRow = makeRow(cp.GetNewColumns())
	}
	if len(cp.GetOldColumns()) > 0 {
		c.OldRow = makeRow(cp.GetOldColumns())
	}
	return c, nil
}

func makeRow(cols []*ColumnPb) Row {
	row := make(map[string]*ColumnVal)
	for _, colPb := range cols {
		cv := &ColumnVal{
			Value: colPb.GetValue(),
			Type:  colPb.GetType(),
		}
		row[colPb.GetName()] = cv
	}
	return row
}

/*
Marshal turns a Change into JSON.
*/
func (c *Change) Marshal() []byte {
	data, err := json.MarshalIndent(c, indentPrefix, indent)
	if err == nil {
		return data
	}
	panic(err.Error())
}

/*
MarshalProto turns a Change into a protobuf.
*/
func (c *Change) MarshalProto() []byte {
	cp := ChangePb{
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
		cp.TransactionID = proto.Uint32(c.TransactionID)
	}
	cp.NewColumns = unmakeRow(c.NewRow)
	cp.OldColumns = unmakeRow(c.OldRow)

	buf, err := proto.Marshal(&cp)
	if err != nil {
		panic(err.Error())
	}
	return buf
}

func unmakeRow(row Row) []*ColumnPb {
	var cols []*ColumnPb
	for name, v := range row {
		cpb := &ColumnPb{
			Name:  proto.String(name),
			Value: proto.String(v.Value),
		}
		if v.Type != 0 {
			cpb.Type = proto.Int32(v.Type)
		}
		cols = append(cols, cpb)
	}
	return cols
}

/*
Helper function that inserts Tables in to a existing Snapshot
*/
func (sd *Snapshot) AddTables(tb Table) []Table {
	sd.Tables = append(sd.Tables, tb)
	return sd.Tables
}

/*
Helper function that inserts rows to an existing table
*/
func (sid *Table) AddRowstoTable(rv Row) []Row {
	sid.Rows = append(sid.Rows, rv)
	return sid.Rows
}
