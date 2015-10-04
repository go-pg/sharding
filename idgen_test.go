package sharding_test

import (
	"sync"
	"testing"
	"time"

	"gopkg.in/go-pg/sharding.v1"
)

func TestTime(t *testing.T) {
	tm := time.Date(2010, time.January, 01, 0, 0, 0, 0, time.UTC)
	g := sharding.NewIdGen(0)
	prev := int64(-1)
	for i := 0; i < 35; i++ {
		next := g.NextTime(tm)
		if next <= prev {
			t.Errorf("%s: next=%d, prev=%d", tm, next, prev)
		}
		prev = next
		tm = tm.AddDate(1, 0, 0)
	}
}

func TestSequence(t *testing.T) {
	g := sharding.NewIdGen(0)
	tm := time.Now()

	var prev int64
	for i := 0; i < 4096; i++ {
		next := g.NextTime(tm)
		if next <= prev {
			t.Errorf("iter %d: next=%d, prev=%d", i, next, prev)
		}
		prev = next
	}
}

func TestIdGen(t *testing.T) {
	N := 50000
	tm := time.Now()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	gen1 := sharding.NewIdGen(1)
	v1 := make([]int64, N)
	go func() {
		for i := 0; i < N; i++ {
			v1[i] = gen1.NextTime(tm)
		}
		wg.Done()
	}()

	gen2 := sharding.NewIdGen(2)
	v2 := make([]int64, N)
	go func() {
		for i := 0; i < N; i++ {
			v2[i] = gen2.NextTime(tm)
		}
		wg.Done()
	}()

	wg.Wait()

	for i := 0; i < N; i++ {
		for j := 0; j < N; j++ {
			if v1[i] == v2[j] {
				t.Fatalf("same numbers: %d and %d", v1[i], v2[j])
			}
		}
	}
}

func TestSplit(t *testing.T) {
	tm := time.Now()
	for shard := int64(0); shard < 2048; shard++ {
		gen := sharding.NewIdGen(shard)
		id := gen.NextTime(tm)
		gotTm, gotShard, gotSeq := sharding.SplitId(id)
		if gotTm.Unix() != tm.Unix() {
			t.Errorf("got %s, expected %s", gotTm, tm)
		}
		if gotShard != shard {
			t.Errorf("got %d, expected %d", gotShard, shard)
		}
		if gotSeq != 0 {
			t.Errorf("got %d, expected 1", gotSeq)
		}
	}
}
