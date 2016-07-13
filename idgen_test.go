package sharding_test

import (
	"testing"
	"time"

	"gopkg.in/go-pg/sharding.v4"
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

func TestShard(t *testing.T) {
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

func TestCollision(t *testing.T) {
	tm := time.Now()
	m := map[int64]struct{}{}

	gen := sharding.NewIdGen(1)
	for i := 0; i < 4096; i++ {
		id := gen.NextTime(tm)
		_, ok := m[id]
		if ok {
			t.Fatalf("collision for %d", id)
		}
		m[id] = struct{}{}
	}

	gen = sharding.NewIdGen(2)
	for i := 0; i < 4096; i++ {
		id := gen.NextTime(tm)
		_, ok := m[id]
		if ok {
			t.Fatalf("collision for %d", id)
		}
		m[id] = struct{}{}
	}
}
