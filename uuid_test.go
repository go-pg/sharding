package sharding_test

import (
	"testing"
	"time"

	"gopkg.in/go-pg/sharding.v4"
)

func TestUUIDTime(t *testing.T) {
	shard := int64(2047)
	for i := 0; i < 100000; i++ {
		tm := time.Date(i, time.January, 1, 0, 0, 0, 0, time.UTC)
		uuid := sharding.NewUUID(shard, tm)
		gotShard, gotTm := uuid.Split()
		if tm.Unix() != gotTm.Unix() {
			t.Fatalf("got time %s, wanted %s", tm, gotTm)
		}
		if gotShard != shard {
			t.Fatalf("got shard %d, wanted %d", gotShard, shard)
		}
	}
}

func TestUUIDShard(t *testing.T) {
	tm := time.Now()
	for shard := int64(0); shard < 2048; shard++ {
		uuid := sharding.NewUUID(shard, tm)
		gotShard, gotTm := uuid.Split()
		if tm.Unix() != gotTm.Unix() {
			t.Fatalf("got time %s, wanted %s", tm, gotTm)
		}
		if gotShard != shard {
			t.Fatalf("got shard %d, wanted %d", gotShard, shard)
		}
	}
}

func TestUUIDCollision(t *testing.T) {
	tm := time.Now()
	shard := int64(2047)
	m := map[string]struct{}{}
	for i := 0; i < 1e6; i++ {
		uuid := sharding.NewUUID(shard, tm)
		_, ok := m[string(uuid)]
		if ok {
			t.Fatalf("collision for %s", uuid)
		}
		m[string(uuid)] = struct{}{}
	}
}
