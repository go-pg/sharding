package sharding_test

import (
	"math"
	"testing"
	"time"

	"github.com/go-pg/sharding/v7"
)

func TestMinIdMaxId(t *testing.T) {
	tm := time.Unix(1262304000, 0)

	minId := sharding.MinId(tm)
	const wantedMinId = 0
	if minId != wantedMinId {
		t.Errorf("got %d, wanted %d", minId, wantedMinId)
	}

	maxId := sharding.MaxId(tm)
	const wantedMaxId = 8388607
	if maxId != wantedMaxId {
		t.Errorf("got %d, wanted %d", maxId, wantedMaxId)
	}
}

func TestNextTime(t *testing.T) {
	minTime := time.Date(1975, time.February, 28, 4, 6, 12, 224000000, time.UTC)

	var tests = []struct {
		tm       time.Time
		wantedId int64
	}{
		{
			tm:       minTime.Add(-2 * time.Millisecond),
			wantedId: math.MinInt64,
		},
		{
			tm:       minTime.Add(-time.Millisecond),
			wantedId: math.MinInt64,
		},
		{
			tm:       minTime,
			wantedId: math.MinInt64,
		},
		{
			tm:       minTime.Add(time.Millisecond),
			wantedId: math.MinInt64 + 1<<23,
		},
		{
			tm:       minTime.Add(2 * time.Millisecond),
			wantedId: math.MinInt64 + 2<<23,
		},
		{
			tm:       minTime.Add(time.Hour),
			wantedId: (math.MinInt64 + 3600000<<23),
		},
	}

	for _, test := range tests {
		minId := sharding.NewShardIdGen(0, nil).NextId(test.tm)
		if minId != test.wantedId {
			t.Errorf("got %d, wanted %d", minId, test.wantedId)
		}
	}
}

func TestNextTimeBounds(t *testing.T) {
	gen := sharding.NewShardIdGen(2049, nil)
	prev := int64(math.MinInt64)
	for i := 1976; i <= 2044; i++ {
		tm := time.Date(i, time.January, 01, 0, 0, 0, 0, time.UTC)
		next := gen.NextId(tm)
		if next <= prev {
			t.Errorf("%s: next=%d, prev=%d", tm, next, prev)
		}
		prev = next
	}
}

func TestShard(t *testing.T) {
	tm := time.Now()
	for shard := int64(0); shard < 2048; shard++ {
		gen := sharding.NewShardIdGen(shard, nil)
		id := gen.NextId(tm)
		gotTm, gotShard, gotSeq := gen.SplitId(id)
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
	gen := sharding.NewShardIdGen(0, nil)
	tm := time.Now()
	max := gen.MaxId(tm)

	var prev int64
	for i := 0; i < 4096; i++ {
		next := gen.NextId(tm)
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
	const n = 4096

	tm := time.Now()
	m := make(map[int64]struct{}, 2*n)

	gen := sharding.NewShardIdGen(0, nil)
	for i := 0; i < n; i++ {
		id := gen.NextId(tm)
		_, ok := m[id]
		if ok {
			t.Fatalf("collision for %d", id)
		}
		m[id] = struct{}{}
	}

	gen = sharding.NewShardIdGen(1, nil)
	for i := 0; i < n; i++ {
		id := gen.NextId(tm)
		_, ok := m[id]
		if ok {
			t.Fatalf("collision for %d", id)
		}
		m[id] = struct{}{}
	}
}
