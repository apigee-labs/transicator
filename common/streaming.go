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
ColumnInfo contains information about a column -- its name and Postgres
data type.
*/
type ColumnInfo struct {
	Name     string
	DataType int32
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
/*
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
			Type: proto.Int32(col.DataType),
		}
		colPb = append(colPb, newCol)
	}

	table := &TableHeaderPb{
		Name:    proto.String(tableName),
		Columns: colPb,
	}
	tableMsg := &StreamMessage_Table{
		Table: table,
	}
	msg := &StreamMessage{
		Message: tableMsg,
	}
	return w.writeProto(msg)
}
*/

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
Use "" for a null column.
*/
/*
func (w *SnapshotWriter) WriteRow(columnValues []string) error {
	if !w.tableWriting {
		return errors.New("Cannot write a row because no table was started")
	}
	if len(columnValues) != w.numColumns {
		return errors.New("Write must include consistent number of columns")
	}

	row := &RowPb{
		Values: columnValues,
	}
	rowMsg := &StreamMessage_Row{
		Row: row,
	}
	msg := &StreamMessage{
		Message: rowMsg,
	}
	return w.writeProto(msg)
}
*/

func (w *SnapshotWriter) writeProto(msg proto.Message) error {
	buf, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	err = binary.Write(w.writer, networkByteOrder, len(buf))
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
	columns   []ColumnInfo
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
Next returns the next item from the snapshot. It can be one of two things:
1) A *TableInfo, which tells us to start writing to a new table, or
2) a *Row, which denotes what you think it does.
3) EOF, which indicates that we have reached the end
4) an error, which indicates that we got incomplete data.
*/
func (r *SnapshotReader) Next() (interface{}, error) {
	/*buf, err := r.rdr.readBuf()
	if err != nil {
		// We will pickup EOF here!
		return nil, err
	}

	var msg StreamMessage
	err = proto.Unmarshal(buf, &msg)
	if err != nil {
		return nil, err
	}

	row := msg.GetRow()
	table := msg.GetTable()

	if table == nil {
		row := make(map[string]*ColumnVal)
		// TODO stuff

	} else {
		// TODO return table info stuff
		// TODO save column info for later
	}
	*/
	return nil, nil
}

func (r *SnapshotReader) readBuf() ([]byte, error) {
	var bufLen int
	err := binary.Read(r.reader, networkByteOrder, &bufLen)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, bufLen)
	_, err = r.reader.Read(buf)
	if err == nil {
		return buf, nil
	}
	return nil, err
}
