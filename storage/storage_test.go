package storage

import (
	"bytes"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	testDBDir = "./testdb"
)

var testDB *DB

var _ = Describe("Storage Main Test", func() {

	BeforeEach(func() {
		var err error
		testDB, err = OpenDB(testDBDir, 1000)
		Expect(err).Should(Succeed())
	})

	AfterEach(func() {
		testDB.Close()
		err := testDB.Delete()
		Expect(err).Should(Succeed())
	})

	It("Open and reopen", func() {
		stor, err := OpenDB("./openleveldb", 1000)
		Expect(err).Should(Succeed())
		stor.Close()
		stor, err = OpenDB("./openleveldb", 1000)
		Expect(err).Should(Succeed())
		stor.Close()
		err = stor.Delete()
		Expect(err).Should(Succeed())
	})

	It("String metadata", func() {
		err := quick.Check(testStringMetadata, nil)
		Expect(err).Should(Succeed())
	})

	It("String metadata negative", func() {
		ret, err := testDB.GetMetadata("NOTFOUND")
		Expect(err).Should(Succeed())
		Expect(ret).Should(BeNil())
	})

	It("Int metadata", func() {
		err := quick.Check(testIntMetadata, nil)
		Expect(err).Should(Succeed())
	})

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
		err := quick.Check(func(lsn int64, index int32, data []byte) bool {
			return testEntry("tag", lsn, index, data)
		}, nil)
		Expect(err).Should(Succeed())
	})

	It("Entries empty tag", func() {
		err := quick.Check(func(lsn int64, index int32, data []byte) bool {
			return testEntry("", lsn, index, data)
		}, nil)
		Expect(err).Should(Succeed())
	})

	It("Entries same LSN", func() {
		err := quick.Check(func(index int32, data []byte) bool {
			return testEntry("tag", 8675309, index, data)
		}, nil)
		Expect(err).Should(Succeed())
	})

	It("Entries and metadata", func() {
		err := quick.Check(testEntryAndData, nil)
		Expect(err).Should(Succeed())
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
})

func testGetSequence(tag string, lsn int64,
	index int32, limit int, expected [][]byte) {
	ret, err := testDB.GetEntries(tag, lsn, index, limit)
	Expect(err).Should(Succeed())
	Expect(len(ret)).Should(Equal(len(expected)))
	for i := range expected {
		Expect(bytes.Equal(expected[i], ret[i])).Should(BeTrue())
	}
}

func testGetSequences(tags []string, lsn int64,
	index int32, limit int, expected [][]byte) {
	ret, err := testDB.GetMultiEntries(tags, lsn, index, limit)
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

func testEntry(key string, lsn int64, index int32, val []byte) bool {
	err := testDB.PutEntry(key, lsn, index, val)
	Expect(err).Should(Succeed())
	ret, err := testDB.GetEntry(key, lsn, index)
	Expect(err).Should(Succeed())
	Expect(bytes.Equal(val, ret)).Should(BeTrue())
	return true
}

func testEntryAndData(key string, lsn int64, index int32, val []byte,
	mkey string, mval int64) bool {
	err := testDB.PutEntryAndMetadata(key, lsn, index, val, mkey, mval)
	Expect(err).Should(Succeed())
	ret, err := testDB.GetEntry(key, lsn, index)
	Expect(err).Should(Succeed())
	Expect(bytes.Equal(val, ret)).Should(BeTrue())
	mret, err := testDB.GetIntMetadata(mkey)
	Expect(err).Should(Succeed())
	Expect(mret).Should(Equal(mval))
	return true
}
