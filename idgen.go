package sharding

import (
	"math"
	"sync/atomic"
	"time"
)

const maxShards = 2048

type IdGen struct {
	shardBits uint
	seqBits   uint
	epoch     int64 // in milliseconds
	minTime   time.Time
	shardMask int64
	seqMask   int64
}

func NewIdGen(timeBits, shardBits, seqBits uint, epoch time.Time) *IdGen {
	if timeBits+shardBits+seqBits != 64 {
		panic("timeBits + shardBits + seqBits != 64")
	}

	dur := time.Duration(1) << (timeBits - 1) * time.Millisecond
	return &IdGen{
		shardBits: shardBits,
		seqBits:   seqBits,
		epoch:     epoch.UnixNano() / int64(time.Millisecond),
		minTime:   epoch.Add(-dur),
		shardMask: int64(1)<<shardBits - 1,
		seqMask:   int64(1)<<seqBits - 1,
	}
}

// NextId returns incremental id for the time. Note that you can only
// generate 4096 unique numbers per millisecond.
func (g *IdGen) NextId(tm time.Time, shard, seq int64) int64 {
	if tm.Before(g.minTime) {
		return int64(math.MinInt64)
	}

	id := tm.UnixNano()/int64(time.Millisecond) - g.epoch
	id <<= g.shardBits + g.seqBits
	id |= shard << g.seqBits
	id |= seq % (g.seqMask + 1)
	return id
}

// MaxId returns max id for the time.
func (g *IdGen) MaxId(tm time.Time, shard int64) int64 {
	id := tm.UnixNano()/int64(time.Millisecond) - g.epoch
	id <<= g.shardBits + g.seqBits
	id |= shard << g.seqBits
	id |= g.seqMask
	return id
}

// SplitId splits id into time, shard id, and sequence id.
func (g *IdGen) SplitId(id int64) (tm time.Time, shardId int64, seqId int64) {
	ms := id>>(g.shardBits+g.seqBits) + g.epoch
	sec := ms / 1000
	tm = time.Unix(sec, (ms-sec*1000)*int64(time.Millisecond))
	shardId = (id >> g.seqBits) & g.shardMask
	seqId = id & g.seqMask
	return
}

//------------------------------------------------------------------------------

var (
	epoch        = time.Date(2010, time.January, 01, 00, 0, 0, 0, time.UTC)
	defaultIdGen = NewIdGen(41, 11, 12, epoch)
)

// SplitId splits id into time, shard id, and sequence id.
func SplitId(id int64) (tm time.Time, shardId int64, seqId int64) {
	return defaultIdGen.SplitId(id)
}

// MinId returns min id for the time.
func MinId(tm time.Time) int64 {
	return defaultIdGen.NextId(tm, 0, 0)
}

// MaxId returns max id for the time.
func MaxId(tm time.Time) int64 {
	return defaultIdGen.MaxId(tm, defaultIdGen.shardMask)
}

//------------------------------------------------------------------------------

// IdGen generates sortable unique int64 numbers that consist of:
// - 41 bits for time in milliseconds.
// - 11 bits for shard id.
// - 12 bits for auto-incrementing sequence.
//
// As a result we can generate 4096 ids per millisecond for each of 2048 shards.
// Minimum supported time is 1975-02-28, maximum is 2044-12-31.
type ShardIdGen struct {
	shard int64
	seq   int64
	gen   *IdGen
}

// NewShardIdGen returns id generator for the shard.
func NewShardIdGen(shard int64, gen *IdGen) *ShardIdGen {
	if gen == nil {
		gen = defaultIdGen
	}
	return &ShardIdGen{
		shard: shard % (int64(1) << gen.shardBits),
		gen:   gen,
	}
}

// NextId returns incremental id for the time. Note that you can only
// generate 4096 unique numbers per millisecond.
func (g *ShardIdGen) NextId(tm time.Time) int64 {
	seq := atomic.AddInt64(&g.seq, 1) - 1
	return g.gen.NextId(tm, g.shard, seq)
}

// MaxId returns max id for the time.
func (g *ShardIdGen) MaxId(tm time.Time) int64 {
	return g.gen.MaxId(tm, g.shard)
}

// SplitId splits id into time, shard id, and sequence id.
func (g *ShardIdGen) SplitId(id int64) (tm time.Time, shardId int64, seqId int64) {
	return g.gen.SplitId(id)
}
