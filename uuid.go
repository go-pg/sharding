package sharding

import (
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/go-pg/pg/types"
)

const uuidLen = 16
const uuidHexLen = 36

var (
	uuidRandMu sync.Mutex
	uuidRand   = rand.New(rand.NewSource(time.Now().UnixNano()))
)

type UUID [uuidLen]byte

var _ types.ValueAppender = (*UUID)(nil)
var _ sql.Scanner = (*UUID)(nil)
var _ driver.Valuer = (*UUID)(nil)

func NewUUID(shardId int64, tm time.Time) UUID {
	shardId = shardId % int64(DefaultIdGen.NumShards())

	var u UUID
	binary.BigEndian.PutUint64(u[:8], uint64(unixMicrosecond(tm)))
	uuidRandMu.Lock()
	uuidRand.Read(u[8:])
	uuidRandMu.Unlock()
	u[8] = (u[8] &^ 0x7) | byte(shardId>>8)
	u[9] = byte(shardId)
	return u
}

func ParseUUID(b []byte) (UUID, error) {
	var u UUID
	if len(b) != uuidHexLen {
		return u, fmt.Errorf("sharding: invalid UUID: %s", b)
	}
	_, err := hex.Decode(u[:4], b[:8])
	if err != nil {
		return u, err
	}
	_, err = hex.Decode(u[4:6], b[9:13])
	if err != nil {
		return u, err
	}
	_, err = hex.Decode(u[6:8], b[14:18])
	if err != nil {
		return u, err
	}
	_, err = hex.Decode(u[8:10], b[19:23])
	if err != nil {
		return u, err
	}
	_, err = hex.Decode(u[10:], b[24:])
	if err != nil {
		return u, err
	}
	return u, nil
}

func (u *UUID) IsZero() bool {
	if u == nil {
		return true
	}
	for _, c := range u {
		if c != 0 {
			return false
		}
	}
	return true
}

func (u *UUID) Split() (shardId int64, tm time.Time) {
	tm = fromUnixMicrosecond(int64(binary.BigEndian.Uint64(u[:8])))
	shardId |= (int64(u[8]) & 0x7) << 8
	shardId |= int64(u[9])
	return
}

func (u *UUID) ShardId() int64 {
	shardId, _ := u.Split()
	return shardId
}

func (u *UUID) Time() time.Time {
	_, tm := u.Split()
	return tm
}

func (u UUID) String() string {
	b, _ := u.AppendValue(nil, 0)
	return string(b)
}

func (u UUID) AppendValue(b []byte, quote int) ([]byte, error) {
	if u.IsZero() {
		return types.AppendNull(b, quote), nil
	}

	if quote == 2 {
		b = append(b, '"')
	} else if quote == 1 {
		b = append(b, '\'')
	}

	b = append(b, make([]byte, uuidHexLen)...)

	bb := b[len(b)-uuidHexLen:]
	hex.Encode(bb[:8], u[:4])
	bb[8] = '-'
	hex.Encode(bb[9:13], u[4:6])
	bb[13] = '-'
	hex.Encode(bb[14:18], u[6:8])
	bb[18] = '-'
	hex.Encode(bb[19:23], u[8:10])
	bb[23] = '-'
	hex.Encode(bb[24:], u[10:])

	if quote == 2 {
		b = append(b, '"')
	} else if quote == 1 {
		b = append(b, '\'')
	}

	return b, nil
}

func (u UUID) Value() (driver.Value, error) {
	return u.String(), nil
}

func (u *UUID) Scan(b interface{}) error {
	if b == nil {
		for i := range u {
			u[i] = 0
		}
		return nil
	}

	uuid, err := ParseUUID(b.([]byte))
	if err != nil {
		return err
	}

	for i, c := range uuid {
		u[i] = c
	}

	return nil
}

func unixMicrosecond(tm time.Time) int64 {
	return tm.Unix()*1e6 + int64(tm.Nanosecond())/1e3
}

func fromUnixMicrosecond(n int64) time.Time {
	secs := n / 1e6
	return time.Unix(secs, (n-secs*1e6)*1e3)
}
