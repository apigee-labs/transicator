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
package common

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/golang/protobuf/proto"
)

/*
A SnapshotWriter allows a snapshot to be constructed by writing to it one
row and one table at a time. It contains some sanity checks to make sure
that the writer uses it appropriately but in general it's important to
be careful.
*/
type SnapshotWriter struct {
	writer       io.Writer
	tableWriting bool
	numColumns   int
}

/*
CreateSnapshotWriter creates a new SnapshotWriter, with the specified
Postgres timestap (from "now()") and snapshot specification
(from "txid_current_snapshot()").
*/
func CreateSnapshotWriter(timestamp, snapshotInfo string,
	writer io.Writer) (*SnapshotWriter, error) {
	hdr := &SnapshotHeaderPb{
		Timestamp: proto.String(timestamp),
		Snapshot:  proto.String(snapshotInfo),
	}

	w := &SnapshotWriter{
		writer: writer,
	}

	err := w.writeProto(hdr)
	if err != nil {
		return nil, err
	}
	return w, nil
}

/*
StartTable tells the reader that it's time to start work on a new table.
It is an error to start a table when a previous table has not been ended.
*/
func (w *SnapshotWriter) StartTable(tableName string, cols []ColumnInfo) error {
	if w.tableWriting {
		return errors.New("Cannot start a new table because last one isn't finished")
	}
	w.tableWriting = true
	w.numColumns = len(cols)

	var colPb []*ColumnPb
	for _, col := range cols {
		newCol := &ColumnPb{
			Name: proto.String(col.Name),
			Type: proto.Int32(col.Type),
		}
		colPb = append(colPb, newCol)
	}

	table := &TableHeaderPb{
		Name:    proto.String(tableName),
		Columns: colPb,
	}
	tableMsg := &StreamMessagePb_Table{
		Table: table,
	}
	msg := &StreamMessagePb{
		Message: tableMsg,
	}
	return w.writeProto(msg)
}

/*
EndTable ends data for the current table. It is an error to end the table when
StartTable was not called.
*/
func (w *SnapshotWriter) EndTable() error {
	if !w.tableWriting {
		return errors.New("Cannot end a table because none has started")
	}
	w.tableWriting = false
	return nil
}

/*
WriteRow writes the values of a single column. It is an error to call this
if StartTable was not called. It is also an error if the length of
"columnValues" does not match the list of names passed to "StartTable."
Values must be primitive types:
* integer types
* float types
* bool
* string
* []byte
*/
func (w *SnapshotWriter) WriteRow(columnValues []interface{}) error {
	if !w.tableWriting {
		return errors.New("Cannot write a row because no table was started")
	}
	if len(columnValues) != w.numColumns {
		return errors.New("Write must include consistent number of columns")
	}

	var columns []*ValuePb
	for _, v := range columnValues {
		col := convertParameter(v)
		colVal := &ValuePb{
			Value: col,
		}
		columns = append(columns, colVal)
	}

	row := &RowPb{
		Values: columns,
	}
	rowMsg := &StreamMessagePb_Row{
		Row: row,
	}
	msg := &StreamMessagePb{
		Message: rowMsg,
	}
	return w.writeProto(msg)
}

func (w *SnapshotWriter) writeProto(msg proto.Message) error {
	buf, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	bufLen := int32(len(buf))
	err = binary.Write(w.writer, networkByteOrder, bufLen)
	if err != nil {
		return err
	}
	_, err = w.writer.Write(buf)
	return err
}

/*
A SnapshotReader reads a snapshot produced by a SnapshotWriter.
The snapshot is read one table and one row at a time by calling "Next()".
*/
type SnapshotReader struct {
	reader    io.Reader
	timestamp string
	snapshot  string
	curBuf    []byte
	savedErr  error
	curTable  *TableInfo
}

/*
CreateSnapshotReader creates a reader.
*/
func CreateSnapshotReader(r io.Reader) (*SnapshotReader, error) {
	rdr := &SnapshotReader{
		reader: r,
	}

	buf, err := rdr.readBuf()
	if err != nil {
		return nil, err
	}

	var hdrPb SnapshotHeaderPb
	err = proto.Unmarshal(buf, &hdrPb)
	if err != nil {
		return nil, err
	}
	rdr.snapshot = hdrPb.GetSnapshot()
	rdr.timestamp = hdrPb.GetTimestamp()
	return rdr, nil
}

/*
Timestamp returns the time (in postgres "now()") format when the snapshot
was created.
*/
func (r *SnapshotReader) Timestamp() string {
	return r.timestamp
}

/*
SnapshotInfo returns the information from "txid_current_snapshot()".
*/
func (r *SnapshotReader) SnapshotInfo() string {
	return r.snapshot
}

/*
Next positions the snapshot reader on the next record. To read the snapshot,
a reader must first call "Next," then call "Entry" to get the entry
for processing. The reader should continue this process until Next
returns "false."
*/
func (r *SnapshotReader) Next() bool {
	buf, err := r.readBuf()
	if err == io.EOF {
		return false
	} else if err != nil {
		r.savedErr = err
		return true
	}
	r.curBuf = buf
	return true
}

/*
Entry returns the current entry. It can be one of three things:
1) A TableInfo, which tells us to start writing to a new table, or
2) a Row, which denotes what you think it does.
3) an error, which indicates that we got incomplete data.
It is an error to call this function if "Next" was never called.
It is also an error to call this function once "Next" returned false.
Finally, it is an error to continue processing after this function
has returned an error.
*/
func (r *SnapshotReader) Entry() interface{} {
	if r.savedErr != nil {
		return r.savedErr
	}
	if r.curBuf == nil {
		return errors.New("Incorrect call sequence")
	}

	var msg StreamMessagePb
	err := proto.Unmarshal(r.curBuf, &msg)
	if err != nil {
		return err
	}

	row := msg.GetRow()
	table := msg.GetTable()

	if table == nil {
		if r.curTable == nil {
			return errors.New("Invalid stream: Got rows before table")
		}
		if len(r.curTable.Columns) != len(row.Values) {
			return errors.New("Invalid stream: Received incorrect number of columns")
		}

		ri := make(map[string]*ColumnVal)
		for i, col := range r.curTable.Columns {
			cv := &ColumnVal{
				Type:  col.Type,
				Value: unwrapColumnVal(row.Values[i]),
			}
			ri[col.Name] = cv
		}
		return Row(ri)
	}

	// Return information on the new table, plus keep it for column processing
	var cols []ColumnInfo
	for _, ti := range table.GetColumns() {
		col := ColumnInfo{
			Name: ti.GetName(),
			Type: ti.GetType(),
		}
		cols = append(cols, col)
	}
	ti := TableInfo{
		Name:    table.GetName(),
		Columns: cols,
	}
	r.curTable = &ti
	return ti
}

func (r *SnapshotReader) readBuf() ([]byte, error) {
	var bufLen int32
	err := binary.Read(r.reader, networkByteOrder, &bufLen)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, bufLen)
	_, err = io.ReadFull(r.reader, buf)
	if err == nil {
		return buf, nil
	}
	return nil, err
}
