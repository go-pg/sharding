package sharding

import (
	"math"
	"sync/atomic"
	"time"
)

var (
	_epoch       = time.Date(2010, time.January, 01, 00, 0, 0, 0, time.UTC)
	DefaultIDGen = NewIDGen(41, 11, 12, _epoch)
)

type IDGen struct {
	shardBits uint
	seqBits   uint
	epoch     int64 // in milliseconds
	minTime   time.Time
	shardMask int64
	seqMask   int64
}

func NewIDGen(timeBits, shardBits, seqBits uint, epoch time.Time) *IDGen {
	if timeBits+shardBits+seqBits != 64 {
		panic("timeBits + shardBits + seqBits != 64")
	}

	dur := time.Duration(1) << (timeBits - 1) * time.Millisecond
	return &IDGen{
		shardBits: shardBits,
		seqBits:   seqBits,
		epoch:     epoch.UnixNano() / int64(time.Millisecond),
		minTime:   epoch.Add(-dur),
		shardMask: int64(1)<<shardBits - 1,
		seqMask:   int64(1)<<seqBits - 1,
	}
}

func (g *IDGen) NumShards() int {
	return int(g.shardMask) + 1
}

// MakeId returns an id for the time. Note that you can only
// generate 4096 unique numbers per millisecond.
func (g *IDGen) MakeID(tm time.Time, shard, seq int64) int64 {
	if tm.Before(g.minTime) {
		return int64(math.MinInt64)
	}

	id := tm.UnixNano()/int64(time.Millisecond) - g.epoch
	id <<= g.shardBits + g.seqBits
	id |= shard << g.seqBits
	id |= seq % (g.seqMask + 1)
	return id
}

// MinID returns min id for the time.
func (g *IDGen) MinID(tm time.Time) int64 {
	return g.MakeID(tm, 0, 0)
}

// MaxID returns max id for the time.
func (g *IDGen) MaxID(tm time.Time) int64 {
	return g.MakeID(tm, g.shardMask, g.seqMask)
}

// SplitID splits id into time, shard id, and sequence id.
func (g *IDGen) SplitID(id int64) (tm time.Time, shardID int64, seqID int64) {
	ms := id>>(g.shardBits+g.seqBits) + g.epoch
	sec := ms / 1000
	tm = time.Unix(sec, (ms-sec*1000)*int64(time.Millisecond))
	shardID = (id >> g.seqBits) & g.shardMask
	seqID = id & g.seqMask
	return
}

//------------------------------------------------------------------------------

// IDGen generates sortable unique int64 numbers that consist of:
// - 41 bits for time in milliseconds.
// - 11 bits for shard id.
// - 12 bits for auto-incrementing sequence.
//
// As a result we can generate 4096 ids per millisecond for each of 2048 shards.
// Minimum supported time is 1975-02-28, maximum is 2044-12-31.
type ShardIDGen struct {
	shard int64
	seq   int64
	gen   *IDGen
}

// NewShardIDGen returns id generator for the shard.
func NewShardIDGen(shard int64, gen *IDGen) *ShardIDGen {
	if gen == nil {
		gen = DefaultIDGen
	}
	return &ShardIDGen{
		shard: shard % int64(gen.NumShards()),
		gen:   gen,
	}
}

// NextID returns incremental id for the time. Note that you can only
// generate 4096 unique numbers per millisecond.
func (g *ShardIDGen) NextID(tm time.Time) int64 {
	seq := atomic.AddInt64(&g.seq, 1) - 1
	return g.gen.MakeID(tm, g.shard, seq)
}

// MinId returns min id for the time.
func (g *ShardIDGen) MinID(tm time.Time) int64 {
	return g.gen.MakeID(tm, g.shard, 0)
}

// MaxId returns max id for the time.
func (g *ShardIDGen) MaxID(tm time.Time) int64 {
	return g.gen.MakeID(tm, g.shard, g.gen.seqMask)
}

// SplitID splits id into time, shard id, and sequence id.
func (g *ShardIDGen) SplitID(id int64) (tm time.Time, shardID int64, seqID int64) {
	return g.gen.SplitID(id)
}
