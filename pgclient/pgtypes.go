package pgclient

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

/*
PgType represents a Postgres type OID.
*/
type PgType int

//go:generate stringer -type PgType .

// Constants for well-known OIDs that we care about
const (
	Bytea       PgType = 17
	Int8        PgType = 20
	Int2        PgType = 21
	Int4        PgType = 23
	OID         PgType = 26
	Float4      PgType = 700
	Float8      PgType = 701
	TimestampTZ PgType = 1184
)

/*
isBinary returns true if the type should be represented as a []byte,
and not converted into a string. Anything that returns "true" here
needs special handling in the other conversion functions below.
*/
func (t PgType) isBinary() bool {
	switch t {
	case Bytea, Int2, Int4, Int8, OID:
		return true
	default:
		return false
	}
}

/*
convertParameterValue is used to convert input values to a string so
that they may be passed on to SQL.
*/
func convertParameterValue(t PgType, v driver.Value) ([]byte, error) {
	switch t {
	// Integer types are first converted into an int64 if at all possible,
	// then converted further into the proper binary format
	case Int2:
		return convertInt2Param(convertIntParam(v))
	case Int4, OID:
		return convertInt4Param(convertIntParam(v))
	case Int8:
		return convertInt8Param(convertIntParam(v))
	default:
		return convertStringParam(v)
	}
}

/*
convertStringParam handles any fields that are send to the PG server as
a string by doing standard string conversion. It also handles "bytea"
fields by just sending raw bytes.
*/
func convertStringParam(v driver.Value) ([]byte, error) {
	switch v.(type) {
	case int64:
		return []byte(strconv.FormatInt(v.(int64), 10)), nil
	case float64:
		return []byte(strconv.FormatFloat(v.(float64), 'f', -1, 64)), nil
	case bool:
		return []byte(strconv.FormatBool(v.(bool))), nil
	case string:
		return []byte(v.(string)), nil
	case time.Time:
		return []byte(v.(time.Time).Format(pgTimeFormat)), nil
	case []byte:
		return v.([]byte), nil
	default:
		return nil, errors.New("Invalid value type")
	}
}

/*
convertIntParam does whatever it can to turn the given value into an
int64.
*/
func convertIntParam(v driver.Value) (int64, error) {
	switch v.(type) {
	case int64:
		return v.(int64), nil
	case bool:
		if v.(bool) {
			return 1, nil
		}
		return 0, nil
	case float64:
		fv := v.(float64)
		if math.Floor(fv) == fv {
			return int64(fv), nil
		}
		return 0, errors.New("Invalid floating-point value for integer column")
	case string:
		iv, err := strconv.ParseInt(v.(string), 10, 64)
		if err == nil {
			return iv, nil
		}
		return 0, errors.New("Invalid string value for integer column")
	case time.Time:
		return v.(time.Time).UnixNano(), nil
	default:
		return 0, errors.New("Invalid value type for integer column")
	}
}

func convertInt2Param(iv int64, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}

	if iv > math.MaxInt16 && iv < math.MinInt16 {
		return nil, errors.New("Value out of range for int2 column")
	}

	buf := &bytes.Buffer{}
	siv := int16(iv)
	binary.Write(buf, networkByteOrder, siv)
	return buf.Bytes(), nil
}

func convertInt4Param(iv int64, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}

	if iv > math.MaxInt32 && iv < math.MinInt32 {
		return nil, errors.New("Value out of range for int4 column")
	}

	buf := &bytes.Buffer{}
	siv := int32(iv)
	binary.Write(buf, networkByteOrder, siv)
	return buf.Bytes(), nil
}

func convertInt8Param(iv int64, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}

	buf := &bytes.Buffer{}
	binary.Write(buf, networkByteOrder, iv)
	return buf.Bytes(), nil
}

/*
convertColumnValue converts the raw bytes from Postgres to a value.
If "isBinary" returned false for a particular PgType, then we are expecting
a string, which we return as a []byte per the "driver" contract.
Otherwise, we have to convert from whatever type we got to a proper value.
*/
func convertColumnValue(t PgType, b []byte) driver.Value {
	switch t {
	// Integer types were returned in binary format, so we must read them
	// as such.
	case Int2:
		buf := bytes.NewBuffer(b)
		var si2 int16
		binary.Read(buf, networkByteOrder, &si2)
		return int64(si2)
	case Int4, OID:
		buf := bytes.NewBuffer(b)
		var si4 int32
		binary.Read(buf, networkByteOrder, &si4)
		return int64(si4)
	case Int8:
		buf := bytes.NewBuffer(b)
		var si8 int64
		binary.Read(buf, networkByteOrder, &si8)
		return si8
	case TimestampTZ:
		// Timestamps are returned as strings. Parse them into a Time value
		// in case the user wants that instead of a string.
		tm, err := time.Parse(pgTimeFormat, string(b))
		if err == nil {
			return tm
		}
		return []byte(fmt.Sprintf("Invalid timestamp %s", string(b)))
	default:
		// This may have been a "bytea" column, in which case we must return the
		// raw bytes. Otherwise, the database returned a string here, and
		// the "sql" package will convert the raw bytes
		// into whatever type the user asked for using string parsing.
		return b
	}
}
