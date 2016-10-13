package common

import (
	"io"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Streaming tests", func() {
	var pr io.ReadCloser
	var pw io.WriteCloser
	var w *SnapshotWriter
	var r *SnapshotReader
	var startTime = time.Now().String()

	BeforeEach(func() {
		var err error
		pr, pw = MakeNBPipe()
		w, err = CreateSnapshotWriter(startTime, "1:2:3", pw)
		Expect(err).Should(Succeed())
		r, err = CreateSnapshotReader(pr)
		Expect(err).Should(Succeed())
	})

	AfterEach(func() {
		pr.Close()
		pw.Close()
	})

	It("Basic stream", func() {
		Expect(r.Timestamp()).Should(Equal(startTime))
		Expect(r.SnapshotInfo()).Should(Equal("1:2:3"))
		pw.Close()

		n := r.Next()
		Expect(n).Should(Equal(io.EOF))
	})

	It("One table", func() {
		cols := []ColumnInfo{
			ColumnInfo{
				Name: "id",
				Type: 1043,
			},
			ColumnInfo{
				Name: "val",
				Type: 20,
			},
		}

		err := w.StartTable("table1", cols)
		Expect(err).Should(Succeed())
		err = w.WriteRow([]interface{}{"one", 123})
		Expect(err).Should(Succeed())
		err = w.WriteRow([]interface{}{"two", uint64(456)})
		Expect(err).Should(Succeed())
		err = w.EndTable()
		Expect(err).Should(Succeed())
		err = pw.Close()
		Expect(err).Should(Succeed())

		n := r.Next()
		ti := n.(TableInfo)
		Expect(ti.Name).Should(Equal("table1"))
		Expect(len(ti.Columns)).Should(Equal(2))
		Expect(ti.Columns[0].Name).Should(Equal("id"))
		Expect(ti.Columns[1].Name).Should(Equal("val"))
		Expect(ti.Columns[0].Type).Should(BeEquivalentTo(1043))
		Expect(ti.Columns[1].Type).Should(BeEquivalentTo(20))

		n = r.Next()
		row := n.(Row)
		var id string
		var val int
		err = row.Get("id", &id)
		Expect(err).Should(Succeed())
		Expect(id).Should(Equal("one"))
		err = row.Get("val", &val)
		Expect(err).Should(Succeed())
		Expect(val).Should(Equal(123))

		n = r.Next()
		row = n.(Row)
		err = row.Get("id", &id)
		Expect(err).Should(Succeed())
		Expect(id).Should(Equal("two"))
		err = row.Get("val", &val)
		Expect(err).Should(Succeed())
		Expect(val).Should(Equal(456))

		n = r.Next()
		Expect(n).Should(Equal(io.EOF))
	})
})
