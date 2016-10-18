package common

import (
	"errors"
	"fmt"
	"strconv"
)

/*
Get places the value of the column into the specified interface if possible.
"d" must be set to one of:
* *string
* *[]byte
* *int16, *int32, *int64, *uint16, *uint32, *uint64
* *float64, *float32
* *bool
*
* If the conversion is not possible, then an error will be returned.
*/
func (v ColumnVal) Get(d interface{}) error {
	switch v.Value.(type) {
	case string:
		return getString(d, v.Value.(string))
	case int64:
		return getInt(d, v.Value.(int64))
	case uint64:
		return getUint(d, v.Value.(uint64))
	case float64:
		return getFloat(d, v.Value.(float64))
	case bool:
		return getBool(d, v.Value.(bool))
	case []byte:
		return getBytes(d, v.Value.([]byte))
	default:
		return errors.New("Value not of expected type")
	}
}

/*
Get retrieves the value of the specified type just like "get", but it sets the
target to the empty value if the column is not present.
*/
func (r Row) Get(name string, d interface{}) error {
	col := r[name]
	if col != nil {
		return col.Get(d)
	}

	switch d.(type) {
	case *string:
		*(d.(*string)) = ""
	case *[]byte:
		*(d.(*[]byte)) = nil
	default:
		return getInt(d, 0)
	}
	return nil
}

/*
String converts the value of the column into a string. Binary data will
be encoded using base64.
*/
func (v ColumnVal) String() string {
	var s string
	v.Get(&s)
	return s
}

func getString(d interface{}, s string) error {
	switch d.(type) {
	case *string:
		*(d.(*string)) = s
	case *int64:
		iv, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		*(d.(*int64)) = iv
	case *int32:
		iv, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return err
		}
		*(d.(*int32)) = int32(iv)
	case *int:
		iv, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return err
		}
		*(d.(*int)) = int(iv)
	case *int16:
		iv, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			return err
		}
		*(d.(*int16)) = int16(iv)
	case *uint64:
		iv, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return err
		}
		*(d.(*uint64)) = iv
	case *uint32:
		iv, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return err
		}
		*(d.(*uint32)) = uint32(iv)
	case *uint:
		iv, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return err
		}
		*(d.(*uint)) = uint(iv)
	case *uint16:
		iv, err := strconv.ParseUint(s, 10, 16)
		if err != nil {
			return err
		}
		*(d.(*uint16)) = uint16(iv)
	case *float64:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		*(d.(*float64)) = v
	case *float32:
		v, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return err
		}
		*(d.(*float32)) = float32(v)
	case *bool:
		v, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		*(d.(*bool)) = v
	case *[]byte:
		*(d.(*[]byte)) = []byte(s)
	default:
		return fmt.Errorf("Invalid conversion: Can't convert string to %T", d)
	}
	return nil
}

func getInt(d interface{}, i int64) error {
	switch d.(type) {
	case *string:
		*(d.(*string)) = strconv.FormatInt(i, 10)
	case *int64:
		*(d.(*int64)) = i
	case *int32:
		*(d.(*int32)) = int32(i)
	case *int:
		*(d.(*int)) = int(i)
	case *int16:
		*(d.(*int16)) = int16(i)
	case *uint64:
		*(d.(*uint64)) = uint64(i)
	case *uint32:
		*(d.(*uint32)) = uint32(i)
	case *uint:
		*(d.(*uint)) = uint(i)
	case *uint16:
		*(d.(*uint16)) = uint16(i)
	case *float64:
		*(d.(*float64)) = float64(i)
	case *float32:
		*(d.(*float32)) = float32(i)
	case *bool:
		if i == 0 {
			*(d.(*bool)) = false
		} else {
			*(d.(*bool)) = true
		}
	default:
		return fmt.Errorf("Invalid conversion: Can't convert int64 to %T", d)
	}
	return nil
}

func getUint(d interface{}, i uint64) error {
	switch d.(type) {
	case *string:
		*(d.(*string)) = strconv.FormatUint(i, 10)
	case *int64:
		*(d.(*int64)) = int64(i)
	case *int32:
		*(d.(*int32)) = int32(i)
	case *int:
		*(d.(*int)) = int(i)
	case *int16:
		*(d.(*int16)) = int16(i)
	case *uint64:
		*(d.(*uint64)) = i
	case *uint32:
		*(d.(*uint32)) = uint32(i)
	case *uint:
		*(d.(*uint)) = uint(i)
	case *uint16:
		*(d.(*uint16)) = uint16(i)
	case *float64:
		*(d.(*float64)) = float64(i)
	case *float32:
		*(d.(*float32)) = float32(i)
	case *bool:
		if i == 0 {
			*(d.(*bool)) = false
		} else {
			*(d.(*bool)) = true
		}
	default:
		return fmt.Errorf("Invalid conversion: Can't convert uint64 to %T", d)
	}
	return nil
}

func getFloat(d interface{}, i float64) error {
	switch d.(type) {
	case *string:
		*(d.(*string)) = strconv.FormatFloat(i, 'G', -1, 64)
	case *int64:
		*(d.(*int64)) = int64(i)
	case *int32:
		*(d.(*int32)) = int32(i)
	case *int:
		*(d.(*int)) = int(i)
	case *int16:
		*(d.(*int16)) = int16(i)
	case *uint64:
		*(d.(*uint64)) = uint64(i)
	case *uint32:
		*(d.(*uint32)) = uint32(i)
	case *uint:
		*(d.(*uint)) = uint(i)
	case *uint16:
		*(d.(*uint16)) = uint16(i)
	case *float64:
		*(d.(*float64)) = float64(i)
	case *float32:
		*(d.(*float32)) = float32(i)
	case *bool:
		if i == 0 {
			*(d.(*bool)) = false
		} else {
			*(d.(*bool)) = true
		}
	default:
		return fmt.Errorf("Invalid conversion: Can't convert float64 to %T", d)
	}
	return nil
}

func isTrue(b bool) int {
	if b {
		return 1
	}
	return 0
}

func getBool(d interface{}, b bool) error {
	switch d.(type) {
	case *string:
		*(d.(*string)) = strconv.FormatBool(b)
	case *int64:
		*(d.(*int64)) = int64(isTrue(b))
	case *int32:
		*(d.(*int32)) = int32(isTrue(b))
	case *int:
		*(d.(*int)) = int(isTrue(b))
	case *int16:
		*(d.(*int16)) = int16(isTrue(b))
	case *uint64:
		*(d.(*uint64)) = uint64(isTrue(b))
	case *uint32:
		*(d.(*uint32)) = uint32(isTrue(b))
	case *uint:
		*(d.(*uint)) = uint(isTrue(b))
	case *uint16:
		*(d.(*uint16)) = uint16(isTrue(b))
	case *float64:
		*(d.(*float64)) = float64(isTrue(b))
	case *float32:
		*(d.(*float32)) = float32(isTrue(b))
	case *bool:
		*(d.(*bool)) = b
	default:
		return fmt.Errorf("Invalid conversion: Can't convert bool to %T", d)
	}
	return nil
}

func getBytes(d interface{}, b []byte) error {
	switch d.(type) {
	case *string:
		*(d.(*string)) = string(b)
	case *[]byte:
		*(d.(*[]byte)) = b
	default:
		return fmt.Errorf("Invalid conversion: Can't convert bytes to %T", d)
	}
	return nil
}

/*
convertParameter takes any type of primitive field and converts it to
something that we can place inside a protobuf. Since we control the
encoding, it panics if an unknown data type comes up.
*/
func convertParameter(v interface{}) isValuePb_Value {
	if v == nil {
		return nil
	}
	switch v.(type) {
	case *interface{}:
		// This makes it easier to scan rows from an existing SQL driver
		return convertParameter(*(v.(*interface{})))
	case string:
		return &ValuePb_String_{
			String_: v.(string),
		}
	case []byte:
		return &ValuePb_Bytes{
			Bytes: v.([]byte),
		}
	case bool:
		return &ValuePb_Bool{
			Bool: v.(bool),
		}
	case int16:
		return &ValuePb_Int{
			Int: int64(v.(int16)),
		}
	case int32:
		return &ValuePb_Int{
			Int: int64(v.(int32)),
		}
	case int:
		return &ValuePb_Int{
			Int: int64(v.(int)),
		}
	case int64:
		return &ValuePb_Int{
			Int: v.(int64),
		}
	case uint16:
		return &ValuePb_Uint{
			Uint: uint64(v.(uint16)),
		}
	case uint32:
		return &ValuePb_Uint{
			Uint: uint64(v.(uint32)),
		}
	case uint:
		return &ValuePb_Uint{
			Uint: uint64(v.(uint)),
		}
	case uint64:
		return &ValuePb_Uint{
			Uint: v.(uint64),
		}
	case float32:
		return &ValuePb_Double{
			Double: float64(v.(float32)),
		}
	case float64:
		return &ValuePb_Double{
			Double: v.(float64),
		}
	default:
		panic(fmt.Sprintf("Can't convert value %v type %T for protobuf", v, v))
	}
}
