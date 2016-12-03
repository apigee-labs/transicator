package storage

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	benchDBDir = "./benchdata"
	largeDBDir = "./benchlargedata"
	firstKey   = "firstlsn"
	lastKey    = "lasttlsn"
)

var largeInit = &sync.Once{}
var largeDB *DB
var largeScopes []string
var largeScopeNames []string

func BenchmarkInserts(b *testing.B) {
	db, err := OpenDB(benchDBDir)
	if err != nil {
		b.Fatalf("Error on open: %s\n", err)
	}
	defer func() {
		db.Close()
		db.Delete()
	}()

	scopes, _ := makeScopeList(100, 10000, 1000, b.N)
	b.Logf("Created %d scopes\n", len(scopes))
	b.Logf("Running %d insert iterations\n", b.N)
	b.ResetTimer()
	doInserts(db, scopes, b.N)
}

func BenchmarkRandomReads(b *testing.B) {
	largeInit.Do(func() {
		initLargeDB(b)
	})

	if b.N > len(largeScopes) {
		b.Fatalf("Too many iterations: %d\n", b.N)
	}
	b.Logf("Reading %d iterations\n", b.N)
	b.ResetTimer()

	plsns := rand.Perm(len(largeScopes))
	for i := 0; i < b.N; i++ {
		_, err := largeDB.GetEntry(largeScopes[plsns[i]], uint64(plsns[i]), 0)
		if err != nil {
			b.Fatalf("Error on read: %s\n", err)
		}
	}
}

func BenchmarkSequence0To100(b *testing.B) {
	largeInit.Do(func() {
		initLargeDB(b)
	})

	if b.N > len(largeScopes) {
		b.Fatalf("Too many iterations: %d\n", b.N)
	}
	b.Logf("Reading %d sequences\n", b.N)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		scope := largeScopes[rand.Intn(len(largeScopeNames))]
		entries, err := largeDB.GetEntries(scope, 0, 0, 100, nil)
		if err != nil {
			b.Fatalf("Error on read: %s\n", err)
		}
		if len(entries) == 0 {
			b.Fatal("Expected at least one entry")
		}
	}
}

func BenchmarkSequence0To100WithMetadata(b *testing.B) {
	largeInit.Do(func() {
		initLargeDB(b)
	})

	if b.N > len(largeScopes) {
		b.Fatalf("Too many iterations: %d\n", b.N)
	}
	b.Logf("Reading %d sequences\n", b.N)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		scope := largeScopes[rand.Intn(len(largeScopeNames))]
		entries, meta, err := largeDB.GetMultiEntries(
			[]string{scope}, []string{firstKey, lastKey}, 0, 0, 100, nil)
		if err != nil {
			b.Fatalf("Error on read: %s\n", err)
		}
		if len(entries) == 0 {
			b.Fatal("Expected at least one entry")
		}
		if string(meta[0]) != "0" {
			b.Fatalf("Expected zero in first key: %s\n", string(meta[0]))
		}
		if string(meta[1]) == "0" {
			b.Fatalf("Expected non-zero in last key: %s\n", string(meta[0]))
		}
	}
}

func BenchmarkSequenceAfterEnd(b *testing.B) {
	largeInit.Do(func() {
		initLargeDB(b)
	})

	if b.N > len(largeScopes) {
		b.Fatalf("Too many iterations: %d\n", b.N)
	}
	b.Logf("Reading %d sequences after end\n", b.N)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		scope := largeScopes[rand.Intn(len(largeScopeNames))]
		entries, err := largeDB.GetEntries(scope, uint64(len(largeScopes)+1), 0, 100, nil)
		if err != nil {
			b.Fatalf("Error on read: %s\n", err)
		}
		if len(entries) != 0 {
			b.Fatalf("Expected no entries, got %d\n", len(entries))
		}
	}
}

func initLargeDB(b *testing.B) {
	var err error
	largeDB, err = OpenDB(largeDBDir)
	if err != nil {
		b.Fatalf("Error on open: %s\n", err)
	}

	largeScopes, largeScopeNames = makeScopeList(100, 10000, 1000, 1)
	b.Logf("Inserting %d records\n", len(largeScopes))
	doInserts(largeDB, largeScopes, len(largeScopes))
	err = largeDB.SetMetadata(firstKey, []byte("0"))
	if err != nil {
		b.Fatalf("Error setting metadata: %s\n", err)
	}
}

func doInserts(db *DB, scopes []string, iterations int) {
	var seq uint64

	for i := 0; i < iterations; i++ {
		seq++
		bod := []byte(fmt.Sprintf("seq-%d", seq))
		err := db.PutEntryAndMetadata(
			scopes[i], seq, 0, bod, lastKey, []byte(strconv.FormatUint(seq, 10)))
		if err != nil {
			panic(fmt.Sprintf("Error on insert: %s\n", err))
		}
	}
}

var _ = Describe("Bench checks", func() {
	It("Permuted scope list", func() {
		sl, _ := makeScopeList(0, 0, 0, 0)
		Expect(sl).Should(BeEmpty())
		sl, sn := makeScopeList(100, 10000, 1000, 200000)
		Expect(len(sl)).Should(BeNumerically(">=", 1000))
		Expect(len(sn)).Should(Equal(100))
	})
})

func makeScopeList(numScopes, stddev, mean, minSize int) ([]string, []string) {
	var rawScopes []string
	var scopeNames []string

	for len(rawScopes) < minSize {
		for sc := 0; sc < numScopes; sc++ {
			scopeName := fmt.Sprintf("Scope-%d", sc)
			scopeNames = append(scopeNames, scopeName)
			rv := math.Abs(rand.NormFloat64()*float64(stddev) + float64(mean))
			count := int(rv)
			for cc := 0; cc < count; cc++ {
				rawScopes = append(rawScopes, scopeName)
			}
		}
	}

	permuted := make([]string, len(rawScopes))
	pix := rand.Perm(len(permuted))

	for i, p := range pix {
		permuted[i] = rawScopes[p]
	}

	return permuted, scopeNames
}
