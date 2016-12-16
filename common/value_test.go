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
	"bytes"
	"strconv"
	"testing/quick"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

/*
Tests for value conversion. What fun.
*/

var _ = Describe("Value conversion tests", func() {
	It("bool", func() {
		Expect(quick.Check(testBool, nil)).Should(Succeed())
	})

	It("int64", func() {
		Expect(quick.Check(func(iv int64) bool {
			c := &Change{
				NewRow: Row{
					"val": &ColumnVal{
						Value: iv,
					},
				},
			}
			col := marshUnmarsh(c)
			return testInt(col, iv, 64)
		}, nil)).Should(Succeed())
	})

	It("int32", func() {
		Expect(quick.Check(func(iv int32) bool {
			c := &Change{
				NewRow: Row{
					"val": &ColumnVal{
						Value: iv,
					},
				},
			}
			col := marshUnmarsh(c)
			return testInt(col, int64(iv), 32)
		}, nil)).Should(Succeed())
	})

	It("int16", func() {
		Expect(quick.Check(func(iv int16) bool {
			c := &Change{
				NewRow: Row{
					"val": &ColumnVal{
						Value: iv,
					},
				},
			}
			col := marshUnmarsh(c)
			return testInt(col, int64(iv), 16)
		}, nil)).Should(Succeed())
	})

	It("uint64", func() {
		Expect(quick.Check(func(iv uint64) bool {
			c := &Change{
				NewRow: Row{
					"val": &ColumnVal{
						Value: iv,
					},
				},
			}
			col := marshUnmarsh(c)
			return testUint(col, iv, 64)
		}, nil)).Should(Succeed())
	})

	It("uint32", func() {
		Expect(quick.Check(func(iv uint32) bool {
			c := &Change{
				NewRow: Row{
					"val": &ColumnVal{
						Value: iv,
					},
				},
			}
			col := marshUnmarsh(c)
			return testUint(col, uint64(iv), 32)
		}, nil)).Should(Succeed())
	})

	It("uint16", func() {
		Expect(quick.Check(func(iv uint16) bool {
			c := &Change{
				NewRow: Row{
					"val": &ColumnVal{
						Value: iv,
					},
				},
			}
			col := marshUnmarsh(c)
			return testUint(col, uint64(iv), 16)
		}, nil)).Should(Succeed())
	})

	It("float32", func() {
		Expect(quick.Check(func(iv float32) bool {
			c := &Change{
				NewRow: Row{
					"val": &ColumnVal{
						Value: iv,
					},
				},
			}
			col := marshUnmarsh(c)
			return testFloat(col, float64(iv), 32)
		}, nil)).Should(Succeed())
	})

	It("float64", func() {
		Expect(quick.Check(func(iv float64) bool {
			c := &Change{
				NewRow: Row{
					"val": &ColumnVal{
						Value: iv,
					},
				},
			}
			col := marshUnmarsh(c)
			return testFloat(col, iv, 64)
		}, nil)).Should(Succeed())
	})

	It("string", func() {
		Expect(quick.Check(testString, nil)).Should(Succeed())
	})

	It("bytes", func() {
		Expect(quick.Check(testBytes, nil)).Should(Succeed())
	})

	It("timestamp", func() {
		now := time.Now()
		c := &Change{
			NewRow: Row{
				"val": &ColumnVal{
					Value: now,
				},
			},
		}

		nv := marshUnmarsh(c)

		var rts time.Time
		err := nv.Get(&rts)
		Expect(err).Should(Succeed())
		// Go timestamp has microseconds but not PG timestamp
		Expect(rts.UnixNano()).Should(BeNumerically("~", now.UnixNano(), 1000))

		var tss string
		err = nv.Get(&tss)
		Expect(err).Should(Succeed())
		pts, err := time.Parse("2006-01-02 15:04:05.99999999 -0700 MST", tss)
		Expect(err).Should(Succeed())
		Expect(pts.UnixNano()).Should(BeNumerically("~", now.UnixNano(), 1000))
	})

	It("Get", func() {
		c := &Change{
			NewRow: Row{
				"val": &ColumnVal{
					Value: int64(123),
				},
			},
		}

		var sv string
		err := c.NewRow.Get("val", &sv)
		Expect(err).Should(Succeed())
		Expect(sv).Should(Equal("123"))

		var iv int64
		err = c.NewRow.Get("val", &iv)
		Expect(err).Should(Succeed())
		Expect(iv).Should(Equal(int64(123)))
	})

	It("Get Null", func() {
		c := &Change{
			NewRow: Row{},
		}

		var iv6 int16 = 123
		err := c.NewRow.Get("val", &iv6)
		Expect(err).Should(Succeed())
		Expect(iv6).Should(BeZero())

		var uv6 uint16 = 123
		err = c.NewRow.Get("val", &uv6)
		Expect(err).Should(Succeed())
		Expect(iv6).Should(BeZero())

		var fv float64 = 3.14
		err = c.NewRow.Get("val", &fv)
		Expect(err).Should(Succeed())
		Expect(fv).Should(BeZero())

		bv := []byte("something")
		err = c.NewRow.Get("val", &bv)
		Expect(err).Should(Succeed())
		Expect(bv).Should(BeNil())

		sv := "something"
		err = c.NewRow.Get("val", &sv)
		Expect(err).Should(Succeed())
		Expect(sv).Should(Equal(""))
	})

	It("Timestamp sanity checks", func() {
		unixEpoch, err :=
			time.Parse("1/2/2006 15:04:05 -0700", "1/1/1970 00:00:00 -0000")
		Expect(err).Should(Succeed())
		Expect(unixEpoch.Unix()).Should(BeZero())

		pgEpoch, err :=
			time.Parse("1/2/2006 15:04:05 -0700", "1/1/2000 00:00:00 -0000")
		Expect(err).Should(Succeed())
		Expect(pgEpoch.UnixNano()).Should(BeEquivalentTo(postgresEpochNanos))
	})

	It("Timestamp conversion", func() {
		now := time.Now()
		nowRounded := time.Unix(0, now.UnixNano()-now.UnixNano()%1000)
		r := PgTimestampToTime(TimeToPgTimestamp(nowRounded))
		Expect(r.UnixNano()).Should(Equal(nowRounded.UnixNano()))
		Expect(r).Should(Equal(nowRounded))
	})
})

func marshUnmarsh(c *Change) *ColumnVal {
	buf := c.MarshalProto()
	after, err := UnmarshalChangeProto(buf)
	Expect(err).Should(Succeed())
	col := after.NewRow["val"]
	Expect(col).ShouldNot(BeNil())
	return col
}

func testString(val string) bool {
	before := Change{
		NewRow: Row{
			"val": &ColumnVal{
				Value: val,
			},
		},
	}

	col := marshUnmarsh(&before)

	var ss string
	err := col.Get(&ss)
	Expect(err).Should(Succeed())
	Expect(ss).Should(Equal(val))

	var bs []byte
	err = col.Get(&bs)
	Expect(err).Should(Succeed())
	Expect(bytes.Equal(bs, []byte(val))).Should(BeTrue())
	return true
}

func testBytes(val []byte) bool {
	before := Change{
		NewRow: Row{
			"val": &ColumnVal{
				Value: val,
			},
		},
	}

	col := marshUnmarsh(&before)

	var ss string
	err := col.Get(&ss)
	Expect(err).Should(Succeed())
	Expect(bytes.Equal([]byte(ss), val)).Should(BeTrue())

	var bs []byte
	err = col.Get(&bs)
	Expect(err).Should(Succeed())
	Expect(bytes.Equal(bs, val)).Should(BeTrue())
	return true
}

func testBool(val bool) bool {
	before := Change{
		NewRow: Row{
			"val": &ColumnVal{
				Value: val,
			},
		},
	}

	col := marshUnmarsh(&before)

	var bv bool
	err := col.Get(&bv)
	Expect(err).Should(Succeed())
	Expect(bv).Should(Equal(val))

	var iv int
	var sv string
	if val {
		iv = 1
		sv = "true"
	} else {
		iv = 0
		sv = "false"
	}

	var iv6 int16
	err = col.Get(&iv6)
	Expect(err).Should(Succeed())
	Expect(iv6).Should(BeEquivalentTo(iv))

	var iv3 int32
	err = col.Get(&iv3)
	Expect(err).Should(Succeed())
	Expect(iv3).Should(BeEquivalentTo(iv))

	var iv4 int64
	err = col.Get(&iv4)
	Expect(err).Should(Succeed())
	Expect(iv4).Should(BeEquivalentTo(iv))

	var uv6 uint16
	err = col.Get(&uv6)
	Expect(err).Should(Succeed())
	Expect(uv6).Should(BeEquivalentTo(iv))

	var uv3 uint32
	err = col.Get(&uv3)
	Expect(err).Should(Succeed())
	Expect(uv3).Should(BeEquivalentTo(iv))

	var uv4 uint64
	err = col.Get(&uv4)
	Expect(err).Should(Succeed())
	Expect(uv4).Should(BeEquivalentTo(iv))

	var fv3 float32
	err = col.Get(&fv3)
	Expect(err).Should(Succeed())
	Expect(fv3).Should(BeEquivalentTo(iv))

	var fv4 float64
	err = col.Get(&fv4)
	Expect(err).Should(Succeed())
	Expect(fv4).Should(BeEquivalentTo(iv))

	var svv string
	err = col.Get(&svv)
	Expect(err).Should(Succeed())
	Expect(svv).Should(Equal(sv))

	var yv []byte
	err = col.Get(&yv)
	Expect(err).ShouldNot(Succeed())

	return true
}

func testInt(col *ColumnVal, iv int64, bits int) bool {

	var bv bool
	err := col.Get(&bv)
	Expect(err).Should(Succeed())
	if iv == 0 {
		Expect(bv).Should(BeFalse())
	} else {
		Expect(bv).Should(BeTrue())
	}

	sv := strconv.FormatInt(iv, 10)
	var svv string
	err = col.Get(&svv)
	Expect(err).Should(Succeed())
	Expect(svv).Should(Equal(sv))

	if bits < 32 {
		var iv6 int16
		err = col.Get(&iv6)
		Expect(err).Should(Succeed())
		Expect(iv6).Should(BeEquivalentTo(iv))
	}

	if bits < 64 {
		var iv3 int32
		err = col.Get(&iv3)
		Expect(err).Should(Succeed())
		Expect(iv3).Should(BeEquivalentTo(iv))

		var ii3 int
		err = col.Get(&ii3)
		Expect(err).Should(Succeed())
		Expect(ii3).Should(BeEquivalentTo(iv))

		var fv3 float32
		err = col.Get(&fv3)
		Expect(err).Should(Succeed())
		Expect(fv3).Should(BeEquivalentTo(float32(iv)))
	}

	var iv4 int64
	err = col.Get(&iv4)
	Expect(err).Should(Succeed())
	Expect(iv4).Should(BeEquivalentTo(iv))

	var fv4 float64
	err = col.Get(&fv4)
	Expect(err).Should(Succeed())
	Expect(fv4).Should(BeEquivalentTo(float64(iv)))

	var yv []byte
	err = col.Get(&yv)
	Expect(err).ShouldNot(Succeed())

	return true
}

func testUint(col *ColumnVal, iv uint64, bits int) bool {
	var bv bool
	err := col.Get(&bv)
	Expect(err).Should(Succeed())
	if iv == 0 {
		Expect(bv).Should(BeFalse())
	} else {
		Expect(bv).Should(BeTrue())
	}

	sv := strconv.FormatUint(iv, 10)
	var svv string
	err = col.Get(&svv)
	Expect(err).Should(Succeed())
	Expect(svv).Should(Equal(sv))

	if bits < 32 {
		var iv6 uint16
		err = col.Get(&iv6)
		Expect(err).Should(Succeed())
		Expect(iv6).Should(BeEquivalentTo(iv))
	}

	if bits < 64 {
		var iv3 uint32
		err = col.Get(&iv3)
		Expect(err).Should(Succeed())
		Expect(iv3).Should(BeEquivalentTo(iv))

		var fv3 float32
		err = col.Get(&fv3)
		Expect(err).Should(Succeed())
		Expect(fv3).Should(BeEquivalentTo(float32(iv)))
	}

	var iv4 uint64
	err = col.Get(&iv4)
	Expect(err).Should(Succeed())
	Expect(iv4).Should(BeEquivalentTo(iv))

	var fv4 float64
	err = col.Get(&fv4)
	Expect(err).Should(Succeed())
	Expect(fv4).Should(BeEquivalentTo(float64(iv)))

	var yv []byte
	err = col.Get(&yv)
	Expect(err).ShouldNot(Succeed())

	return true
}

func testFloat(col *ColumnVal, iv float64, bits int) bool {

	var bv bool
	err := col.Get(&bv)
	Expect(err).Should(Succeed())
	if iv == 0.0 {
		Expect(bv).Should(BeFalse())
	} else {
		Expect(bv).Should(BeTrue())
	}

	if bits < 64 {
		var iv3 int32
		err = col.Get(&iv3)
		Expect(err).Should(Succeed())
		Expect(iv3).Should(BeEquivalentTo(int32(iv)))

		var fv3 float32
		err = col.Get(&fv3)
		Expect(err).Should(Succeed())
		Expect(fv3).Should(BeEquivalentTo(float32(iv)))
	} else {
		sv := strconv.FormatFloat(iv, 'G', -1, bits)
		var svv string
		err = col.Get(&svv)
		Expect(err).Should(Succeed())
		Expect(svv).Should(Equal(sv))
	}

	var iv4 int64
	err = col.Get(&iv4)
	Expect(err).Should(Succeed())
	Expect(iv4).Should(BeEquivalentTo(int64(iv)))

	var fv4 float64
	err = col.Get(&fv4)
	Expect(err).Should(Succeed())
	Expect(fv4).Should(BeEquivalentTo(iv))

	var yv []byte
	err = col.Get(&yv)
	Expect(err).ShouldNot(Succeed())

	return true
}
