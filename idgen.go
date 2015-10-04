package sharding

import (
	"sync/atomic"
	"time"
)

const (
	epoch     = int64(1262304000000) // 2010-01-01 00:00:00 +0000 UTC
	shardMask = int64(1<<11) - 1     // 2047
	seqMask   = int64(1<<12) - 1     // 4095
)

// IdGen generates sortable unique int64 numbers that consist of:
// - 41 bits for time in milliseconds.
// - 11 bits that represent the shard id.
// - 12 bits that represent an auto-incrementing sequence.
//
// That means that for 35 years we can generate 4096 ids per
// millisecond for 2048 shards.
type IdGen struct {
	seq   int64
	shard int64
}

// NewIdGen returns id generator for the shard.
func NewIdGen(shard int64) *IdGen {
	return &IdGen{
		shard: shard % (shardMask + 1),
	}
}

// NextTime returns increasing id for the time. Note that you can only
// generate 4096 unique numbers per millisecond.
func (g *IdGen) NextTime(tm time.Time) int64 {
	seq := atomic.AddInt64(&g.seq, 1) - 1
	id := tm.UnixNano()/int64(time.Millisecond) - epoch
	id <<= 23
	id |= g.shard << 12
	id |= seq % (seqMask + 1)
	return id
}

// Next acts like NextTime, but returns id for current time.
func (g *IdGen) Next() int64 {
	return g.NextTime(time.Now())
}

// SplitId splits id into time, shard id, and sequence id.
func SplitId(id int64) (tm time.Time, shardId int64, seqId int64) {
	ms := int64(id>>23) + epoch
	sec := ms / 1000
	tm = time.Unix(sec, (ms-sec*1000)*int64(time.Millisecond))
	shardId = (id >> 12) & shardMask
	seqId = id & seqMask
	return
}
