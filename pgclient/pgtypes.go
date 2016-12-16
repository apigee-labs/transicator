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
package pgclient

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/apigee-labs/transicator/common"
)

const (
	pgTimeFormat = "2006-01-02 15:04:05-07"
)

/*
isBinaryValue returns true if the type should be represented as a []byte
when returned from the driver,
and not converted into a string. Anything that returns "true" here
needs special handling in the other conversion functions below.
*/
func (t PgType) isBinaryValue() bool {
	switch t {
	case Bytea, Int2, Int4, Int8, OID, Timestamp, TimestampTZ:
		return true
	default:
		return false
	}
}

/*
isBinaryParameter returns true if the type should be represented in a binary format
when we send it as a parameter to Postgres, as opposed to a string.
*/
func (t PgType) isBinaryParameter(a driver.Value) bool {
	switch t {
	case Bytea, Int2, Int4, Int8, OID:
		return true
	case Timestamp, TimestampTZ:
		switch a.(type) {
		case time.Time:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

/*
convertParameterValue is used to convert input values to a byte array so
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
	case Timestamp, TimestampTZ:
		return convertTimestampParam(v)
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
convertTimestamp will produce a binary-format timestamp if the input is a
time.Time, and otherwise will produce a string.
*/
func convertTimestampParam(v driver.Value) ([]byte, error) {
	switch v.(type) {
	case time.Time:
		return convertInt8Param(common.TimeToPgTimestamp(v.(time.Time)), nil)
	default:
		return convertStringParam(v)
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
		if b == nil {
			return nil
		}
		buf := bytes.NewBuffer(b)
		var si2 int16
		binary.Read(buf, networkByteOrder, &si2)
		return int64(si2)
	case Int4, OID:
		if b == nil {
			return nil
		}
		buf := bytes.NewBuffer(b)
		var si4 int32
		binary.Read(buf, networkByteOrder, &si4)
		return int64(si4)
	case Int8:
		if b == nil {
			return nil
		}
		buf := bytes.NewBuffer(b)
		var si8 int64
		binary.Read(buf, networkByteOrder, &si8)
		return si8
	case Timestamp, TimestampTZ:
		if b == nil {
			return nil
		}
		buf := bytes.NewBuffer(b)
		var ts8 int64
		binary.Read(buf, networkByteOrder, &ts8)
		return common.PgTimestampToTime(ts8)
	default:
		// This may have been a "bytea" column, in which case we must return the
		// raw bytes. Otherwise, the database returned a string or nil here, and
		// the "sql" package will convert the raw bytes
		// into whatever type the user asked for using string parsing.
		return b
	}
}
