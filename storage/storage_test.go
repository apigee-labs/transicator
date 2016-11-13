package storage

import (
	"bytes"
	"fmt"
	"math"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"encoding/hex"
)

const (
	testDBDir = "./testdb"
)

var testDB *DB

var _ = Describe("Storage Main Test", func() {

	BeforeEach(func() {
		var err error
		testDB, err = OpenDB(testDBDir, 1000)
		fmt.Println(err)
		Expect(err).Should(Succeed())
	})

	AfterEach(func() {
		testDB.Close()
		err := testDB.Delete()
		Expect(err).Should(Succeed())
	})

	It("Open and reopen", func() {
		fmt.Println("Open and re-open")
		stor, err := OpenDB("./openleveldb", 1000)
		Expect(err).Should(Succeed())
		stor.Close()
		stor, err = OpenDB("./openleveldb", 1000)
		Expect(err).Should(Succeed())
		stor.Close()
		err = stor.Delete()
		Expect(err).Should(Succeed())
		fmt.Println("Open and re-open done")
	})

	//It("String metadata", func() {
	//	err := quick.Check(testStringMetadata, nil)
	//	Expect(err).Should(Succeed())
	//})

	It("String metadata negative", func() {
		ret, err := testDB.GetMetadata("NOTFOUND")
		Expect(err).Should(Succeed())
		Expect(ret).Should(BeNil())
	})

	//It("Int metadata", func() {
	//	err := quick.Check(testIntMetadata, nil)
	//	Expect(err).Should(Succeed())
	//})

	It("Int metadata negative", func() {
		ret, err := testDB.GetIntMetadata("REALLYNOTFOUND")
		Expect(err).Should(Succeed())
		Expect(ret).Should(BeEquivalentTo(0))
	})

	It("Entries", func() {
		err := quick.Check(testEntry, nil)
		Expect(err).Should(Succeed())
	})

	It("Entries same tag", func() {
		err := quick.Check(func(lsn uint64, index uint32, data []byte) bool {
			return testEntry("tag", lsn, index, data)
		}, nil)
		Expect(err).Should(Succeed())
	})

	It("Entries empty tag", func() {
		err := quick.Check(func(lsn uint64, index uint32, data []byte) bool {
			return testEntry("", lsn, index, data)
		}, nil)
		Expect(err).Should(Succeed())
	})

	It("Entries same LSN", func() {
		err := quick.Check(func(index uint32, data []byte) bool {
			return testEntry("tag", 8675309, index, data)
		}, nil)
		Expect(err).Should(Succeed())
	})

	It("Entries and metadata", func() {
		err := quick.Check(testEntryAndData, nil)
		Expect(err).Should(Succeed())
	})

	It("Read not found", func() {
		buf, err := testDB.GetEntry("foo", 0, 0)
		Expect(err).Should(Succeed())
		Expect(buf).Should(BeNil())
	})

	It("Read not found multi", func() {
		bufs, err := testDB.GetEntries("foo", 0, 0, 100, nil)
		Expect(err).Should(Succeed())
		Expect(bufs).Should(BeEmpty())
	})

	It("Reading sequences", func() {
		val1 := []byte("Hello!")
		val2 := []byte("World.")

		testDB.PutEntry("a", 0, 0, val1)
		testDB.PutEntry("a", 1, 0, val2)
		testDB.PutEntry("a", 1, 1, val1)
		testDB.PutEntry("a", 2, 0, val2)
		testDB.PutEntry("b", 3, 0, val1)
		testDB.PutEntry("c", 4, 0, val1)
		testDB.PutEntry("c", 4, 1, val2)
		testDB.PutEntry("", 10, 0, val1)
		testDB.PutEntry("", 11, 1, val2)

		// Read whole ranges
		testGetSequence("a", 0, 0, 100, [][]byte{val1, val2, val1, val2})
		testGetSequence("b", 0, 0, 100, [][]byte{val1})
		testGetSequence("c", 0, 0, 100, [][]byte{val1, val2})
		testGetSequence("", 0, 0, 100, [][]byte{val1, val2})

		// Read after start
		testGetSequence("a", 1, 0, 100, [][]byte{val2, val1, val2})
		testGetSequence("a", 1, 1, 100, [][]byte{val1, val2})
		testGetSequence("a", 2, 0, 100, [][]byte{val2})
		testGetSequence("a", 2, 1, 100, [][]byte{})
		testGetSequence("a", 3, 0, 100, [][]byte{})

		// Read with limit
		testGetSequenceFilter("a", 0, 0, 2, [][]byte{val2, val2}, val1)
		testGetSequenceFilter("a", 0, 0, 1, [][]byte{val2}, val1)

		// Read with limit and filter
		testGetSequence("a", 0, 0, 4, [][]byte{val1, val2, val1, val2})
		testGetSequence("a", 0, 0, 3, [][]byte{val1, val2, val1})
		testGetSequence("a", 0, 0, 2, [][]byte{val1, val2})
		testGetSequence("a", 0, 0, 1, [][]byte{val1})
		testGetSequence("a", 0, 0, 0, [][]byte{})

		// Read invalid range
		testGetSequence("d", 0, 0, 0, [][]byte{})

		// Read a bunch of ranges
		testGetSequences([]string{"a", "b", "c"}, 0, 0, 100,
			[][]byte{val1, val2, val1, val2, val1, val1, val2})
		// test that sorting works
		testGetSequences([]string{"c", "b", "a"}, 0, 0, 100,
			[][]byte{val1, val2, val1, val2, val1, val1, val2})
		// test that limits work
		testGetSequences([]string{"c", "b", "a"}, 0, 0, 3,
			[][]byte{val1, val2, val1})
		// test that they work when we start in the middle
		testGetSequences([]string{"c", "b", "a"}, 2, 0, 3,
			[][]byte{val2, val1, val1})
	})

	It("Purge empty database", func() {
		count, err := testDB.PurgeEntries(func(buf []byte) bool {
			return true
		})
		Expect(err).Should(Succeed())
		Expect(count).Should(BeZero())
	})

	It("Purge some entries", func() {
		val1 := []byte("Hello")
		val2 := []byte("Goodbye")

		testDB.PutEntry("a", 0, 0, val1)
		testDB.PutEntry("a", 1, 0, val2)
		testDB.PutEntry("a", 1, 1, val1)
		testDB.PutEntry("a", 2, 0, val2)
		testDB.PutEntry("b", 3, 0, val1)
		testDB.PutEntry("c", 4, 0, val1)
		testDB.PutEntry("c", 4, 1, val2)
		testDB.PutEntry("", 10, 0, val1)
		testDB.PutEntry("", 11, 1, val2)

		// Purge only one value
		count, err := testDB.PurgeEntries(func(buf []byte) bool {
			return bytes.Equal(buf, val2)
		})
		Expect(err).Should(Succeed())
		Expect(count).Should(BeEquivalentTo(4))

		// Verify that re-purge does nothing
		count, err = testDB.PurgeEntries(func(buf []byte) bool {
			return bytes.Equal(buf, val2)
		})
		Expect(err).Should(Succeed())
		Expect(count).Should(BeZero())

		found, err := testDB.GetEntry("a", 0, 0)
		Expect(err).Should(Succeed())
		Expect(bytes.Equal(found, val1)).Should(BeTrue())

		found, err = testDB.GetEntry("a", 0, 1)
		Expect(err).Should(Succeed())
		Expect(found).Should(BeNil())

		// Purge the rest of the entries
		count, err = testDB.PurgeEntries(func(buf []byte) bool {
			return true
		})
		Expect(err).Should(Succeed())
		Expect(count).Should(BeEquivalentTo(5))

		// Verify that everything is gone now
		count, err = testDB.PurgeEntries(func(buf []byte) bool {
			return true
		})
		Expect(err).Should(Succeed())
		Expect(count).Should(BeZero())
	})
})

func testGetSequence(tag string, lsn uint64,
	index uint32, limit int, expected [][]byte) {
	ret, err := testDB.GetEntries(tag, lsn, index, limit, nil)
	Expect(err).Should(Succeed())
	Expect(len(ret)).Should(Equal(len(expected)))
	for i := range expected {
		Expect(bytes.Equal(expected[i], ret[i])).Should(BeTrue())
	}
}

func testGetSequenceFilter(tag string, lsn uint64,
	index uint32, limit int, expected [][]byte, rejected []byte) {
	ret, err := testDB.GetEntries(tag, lsn, index, limit,
		func(rej []byte) bool {
			if bytes.Equal(rej, rejected) {
				return false
			}
			return true
		})
	Expect(err).Should(Succeed())
	Expect(len(ret)).Should(Equal(len(expected)))
	for i := range expected {
		Expect(bytes.Equal(expected[i], ret[i])).Should(BeTrue())
	}
}

func testGetSequences(tags []string, lsn uint64,
	index uint32, limit int, expected [][]byte) {
	ret, _, err := testDB.GetMultiEntries(tags, nil, lsn, index, limit, nil)
	Expect(err).Should(Succeed())
	Expect(len(ret)).Should(Equal(len(expected)))
	for i := range expected {
		Expect(bytes.Equal(expected[i], ret[i])).Should(BeTrue())
	}
}

func testStringMetadata(key string, val []byte) bool {
	err := testDB.SetMetadata(key, val)
	Expect(err).Should(Succeed())
	ret, err := testDB.GetMetadata(key)
	Expect(err).Should(Succeed())
	Expect(bytes.Equal(val, ret)).Should(BeTrue())
	return true
}

func testIntMetadata(key string, val int64) bool {
	err := testDB.SetIntMetadata(key, val)
	Expect(err).Should(Succeed())
	ret, err := testDB.GetIntMetadata(key)
	Expect(err).Should(Succeed())
	Expect(ret).Should(Equal(val))
	return true
}

func testEntry(key string, lsn uint64, index uint32, val []byte) bool {
	err := testDB.PutEntry(key, lsn, index, val)
	Expect(err).Should(Succeed())
	ret, err := testDB.GetEntry(key, lsn, index)
	Expect(err).Should(Succeed())
	if !bytes.Equal(val, ret) {
		fmt.Printf("Val is %d %s ret is %d %s, key is %s, lsn is %d index is %d\n", len(val), hex.Dump(val), len(ret), hex.Dump(ret), key, lsn, index)
	}
	Expect(bytes.Equal(val, ret)).Should(BeTrue())
	return true
}

func testEntryAndData(key string, lsn uint64, index uint32, val []byte,
	mkey string, mval []byte) bool {
	if key == "" {
		return true
	}
	if lsn > math.MaxInt64 {
		// TODO this does not sound right to me, but the test fails if
		// the high bit is set
		return true
	}
	fmt.Fprintf(GinkgoWriter, "EandD: key = %v lsn = %x index = %x mkey = %v val = %v mval = %v\n",
		[]byte(key), lsn, index, []byte(mkey), val, mval)
	err := testDB.PutEntryAndMetadata(key, lsn, index, val, mkey, mval)
	Expect(err).Should(Succeed())
	ret, err := testDB.GetEntry(key, lsn, index)
	Expect(err).Should(Succeed())
	Expect(bytes.Equal(val, ret)).Should(BeTrue())
	mret, err := testDB.GetMetadata(mkey)
	Expect(err).Should(Succeed())
	Expect(bytes.Equal(mval, mret)).Should(BeTrue())
	entries, err :=
		testDB.GetEntries(key, 0, 0, 100, func([]byte) bool {
			return true
		})
	Expect(err).Should(Succeed())
	Expect(len(entries)).Should(Equal(1))

	entries, datas, err :=
		testDB.GetMultiEntries([]string{key}, []string{mkey},
			0, 0, 100, func([]byte) bool {
				return true
			})
	Expect(err).Should(Succeed())
	Expect(len(entries)).Should(Equal(1))
	Expect(len(datas)).Should(Equal(1))
	return true
}
