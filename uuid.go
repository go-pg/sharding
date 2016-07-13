package sharding

import (
	"encoding/binary"
	"encoding/hex"
	"math/rand"
	"time"
)

type UUID []byte

func NewUUID(shard int64, tm time.Time) UUID {
	shard = shard % 2048

	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[:8], uint64(unixMicrosecond(tm)))
	rand.Read(b[8:])
	b[8] = (b[8] &^ 0x7) | byte(shard>>8)
	b[9] = byte(shard)
	return b
}

func (u UUID) Split() (shardId int64, tm time.Time) {
	b := []byte(u)
	tm = fromUnixMicrosecond(int64(binary.BigEndian.Uint64(b[:8])))
	shardId |= (int64(b[8]) & 0x7) << 8
	shardId |= int64(b[9])
	return
}

func (u UUID) String() string {
	b := make([]byte, 36)
	hex.Encode(b[0:8], u[0:4])
	b[8] = '-'
	hex.Encode(b[9:13], u[4:6])
	b[13] = '-'
	hex.Encode(b[14:18], u[6:8])
	b[18] = '-'
	hex.Encode(b[19:23], u[8:10])
	b[23] = '-'
	hex.Encode(b[24:], u[10:])
	return string(b)
}

func unixMicrosecond(tm time.Time) int64 {
	return tm.Unix()*1e6 + int64(tm.Nanosecond())/1e3
}

func fromUnixMicrosecond(n int64) time.Time {
	secs := n / 1e6
	return time.Unix(secs, (n-secs*1e6)*1e3)
}
