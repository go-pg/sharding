package sharding_test

import (
	"math"
	"testing"
	"time"

	"gopkg.in/go-pg/sharding.v4"
)

func TestTime(t *testing.T) {
	g := sharding.NewIdGen(2049)
	prev := int64(math.MinInt64)
	for i := 1976; i <= 2044; i++ {
		tm := time.Date(i, time.January, 01, 0, 0, 0, 0, time.UTC)
		next := g.NextTime(tm)
		if next <= prev {
			t.Errorf("%s: next=%d, prev=%d", tm, next, prev)
		}
		prev = next
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
	max := g.MaxTime(tm)

	var prev int64
	for i := 0; i < 4096; i++ {
		next := g.NextTime(tm)
		if next <= prev {
			t.Errorf("iter %d: next=%d prev=%d", i, next, prev)
		}
		if next > max {
			t.Errorf("iter %d: next=%d max=%d", i, next, max)
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
