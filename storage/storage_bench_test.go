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
package storage

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/tecbot/gorocksdb"
)

const (
	benchDBDir = "./benchdata"
	largeDBDir = "./benchlargedata"
	cleanDBDir = "./cleanlargedata"
	firstKey   = "firstlsn"
	lastKey    = "lasttlsn"
)

var largeInit = &sync.Once{}
var cleanInit = &sync.Once{}
var largeDB, cleanDB *DB
var largeScopes, cleanScopes []string
var largeScopeNames, cleanScopeNames []string

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
		scope := largeScopeNames[rand.Intn(len(largeScopeNames))]
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
		scope := largeScopeNames[rand.Intn(len(largeScopeNames))]
		entries, _, _, err := largeDB.GetMultiEntries(
			[]string{scope}, 0, 0, 100, nil)
		if err != nil {
			b.Fatalf("Error on read: %s\n", err)
		}
		if len(entries) == 0 {
			b.Fatal("Expected at least one entry")
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
		scope := largeScopeNames[rand.Intn(len(largeScopeNames))]
		entries, err := largeDB.GetEntries(scope, uint64(len(largeScopes)+1), 0, 100, nil)
		if err != nil {
			b.Fatalf("Error on read: %s\n", err)
		}
		if len(entries) != 0 {
			b.Fatalf("Expected no entries, got %d\n", len(entries))
		}
	}
}

func BenchmarkSequence0To100WithMetadataAfterClean(b *testing.B) {
	largeInit.Do(func() {
		initLargeDB(b)
	})

	cleanInit.Do(func() {
		purgeNRecords(b, cleanDB, len(cleanScopes) / 2)
	})

	b.Logf("Reading %d sequences\n", b.N)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		scope := cleanScopeNames[rand.Intn(len(cleanScopeNames))]
		_, _, _, err := cleanDB.GetMultiEntries(
			[]string{scope}, 0, 0, 100, nil)
		if err != nil {
			b.Fatalf("Error on read: %s\n", err)
		}
	}
}

func BenchmarkSequence0To100WithMetadataAfterCleanCompact(b *testing.B) {
	largeInit.Do(func() {
		initLargeDB(b)
	})

	cleanInit.Do(func() {
		purgeNRecords(b, cleanDB, len(cleanScopes) / 2)
	})

	b.Logf("Compacting database\n")
	cleanDB.db.CompactRangeCF(cleanDB.entriesCF, gorocksdb.Range{})
	b.Logf("Reading %d sequences\n", b.N)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		scope := cleanScopeNames[rand.Intn(len(cleanScopeNames))]
		_, _, _, err := cleanDB.GetMultiEntries(
			[]string{scope}, 0, 0, 100, nil)
		if err != nil {
			b.Fatalf("Error on read: %s\n", err)
		}
	}
}

func purgeNRecords(b *testing.B, db *DB, toPurge int) {
	b.Logf("Cleaning the up to %d records\n", toPurge)
	pc := 0
	purged, err := db.PurgeEntries(func (b []byte) bool {
		if pc < toPurge {
			pc++
			return true
		}
		return false
	})
	if err != nil {
		b.Fatalf("Error on purge: %s\n", err)
	}
	b.Logf("Cleaned %d.\n", purged)
}

func initLargeDB(b *testing.B) {
	largeScopes, largeScopeNames = makeScopeList(100, 10000, 1000, 1)
	largeDB = initDB(b, largeDBDir, largeScopes)
	cleanScopes, cleanScopeNames = makeScopeList(100, 10000, 1000, 1)
	cleanDB = initDB(b, cleanDBDir, cleanScopes)
}

func initDB(b *testing.B, dir string, insertScopes []string) *DB {
	db, err := OpenDB(dir)
	if err != nil {
		b.Fatalf("Error on open: %s\n", err)
	}

	b.Logf("Inserting %d records\n", len(insertScopes))
	doInserts(db, insertScopes, len(insertScopes))
	return db
}

func doInserts(db *DB, scopes []string, iterations int) {
	var seq uint64

	for i := 0; i < iterations; i++ {
		seq++
		bod := []byte(fmt.Sprintf("seq-%d", seq))
		err := db.PutEntry(
			scopes[i], seq, 0, bod)
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
