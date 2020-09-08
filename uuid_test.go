package sharding_test

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/go-pg/sharding/v8"
)

func TestUUIDParse(t *testing.T) {
	sharding.SetUUIDRand(rand.New(rand.NewSource(0)))

	tm := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	uuid := sharding.NewUUID(0, tm)
	got := uuid.String()
	wanted := "00035d01-3b37-e000-0000-fdc2fa2ffcc0"
	if got != wanted {
		t.Fatalf("got %q, wanted %q", got, wanted)
	}

	parsed, err := sharding.ParseUUID([]byte(got))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(parsed[:], uuid[:]) {
		t.Fatalf("got %x, wanted %x", parsed, uuid)
	}
}

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
	m := map[[16]byte]struct{}{}
	for i := 0; i < 1e6; i++ {
		uuid := sharding.NewUUID(shard, tm)
		_, ok := m[uuid]
		if ok {
			t.Fatalf("collision for %s", uuid)
		}
		m[uuid] = struct{}{}
	}
}
