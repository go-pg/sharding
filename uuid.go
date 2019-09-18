package sharding

import (
	"database/sql"
	"database/sql/driver"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/go-pg/pg/v9/types"
)

const (
	uuidLen    = 16
	uuidHexLen = 36
)

var (
	uuidRandMu sync.Mutex
	uuidRand   = rand.New(rand.NewSource(time.Now().UnixNano()))
)

type UUID [uuidLen]byte

func NewUUID(shardID int64, tm time.Time) UUID {
	shardID = shardID % int64(globalIDGen.NumShards())

	var u UUID
	binary.BigEndian.PutUint64(u[:8], uint64(unixMicrosecond(tm)))
	uuidRandMu.Lock()
	uuidRand.Read(u[8:])
	uuidRandMu.Unlock()
	u[8] = (u[8] &^ 0x7) | byte(shardID>>8)
	u[9] = byte(shardID)
	return u
}

func ParseUUID(b []byte) (UUID, error) {
	var u UUID
	err := u.UnmarshalText(b)
	return u, err
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

func (u *UUID) Split() (shardID int64, tm time.Time) {
	tm = fromUnixMicrosecond(int64(binary.BigEndian.Uint64(u[:8])))
	shardID |= (int64(u[8]) & 0x7) << 8
	shardID |= int64(u[9])
	return
}

func (u *UUID) ShardID() int64 {
	shardID, _ := u.Split()
	return shardID
}

func (u *UUID) Time() time.Time {
	_, tm := u.Split()
	return tm
}

func (u UUID) String() string {
	b := appendHex(nil, u[:])
	return string(b)
}

var _ types.ValueAppender = (*UUID)(nil)

func (u UUID) AppendValue(b []byte, quote int) ([]byte, error) {
	if u.IsZero() {
		return types.AppendNull(b, quote), nil
	}

	if quote == 2 {
		b = append(b, '"')
	} else if quote == 1 {
		b = append(b, '\'')
	}

	b = appendHex(b, u[:])

	if quote == 2 {
		b = append(b, '"')
	} else if quote == 1 {
		b = append(b, '\'')
	}

	return b, nil
}

var _ driver.Valuer = (*UUID)(nil)

func (u UUID) Value() (driver.Value, error) {
	return u.String(), nil
}

var _ sql.Scanner = (*UUID)(nil)

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

var _ encoding.BinaryMarshaler = (*UUID)(nil)

func (u UUID) MarshalBinary() ([]byte, error) {
	return u[:], nil
}

var _ encoding.BinaryUnmarshaler = (*UUID)(nil)

func (u *UUID) UnmarshalBinary(b []byte) error {
	if len(b) != uuidLen {
		return fmt.Errorf("sharding: invalid UUID: %q", b)
	}
	copy(u[:], b)
	return nil
}

var _ encoding.TextMarshaler = (*UUID)(nil)

func (u UUID) MarshalText() ([]byte, error) {
	return appendHex(nil, u[:]), nil
}

var _ encoding.TextUnmarshaler = (*UUID)(nil)

func (u *UUID) UnmarshalText(b []byte) error {
	if len(b) == uuidHexLen-4 {
		_, err := hex.Decode(u[:], b)
		return err
	}

	if len(b) != uuidHexLen {
		return fmt.Errorf("sharding: invalid UUID: %q", b)
	}
	_, err := hex.Decode(u[:4], b[:8])
	if err != nil {
		return err
	}
	_, err = hex.Decode(u[4:6], b[9:13])
	if err != nil {
		return err
	}
	_, err = hex.Decode(u[6:8], b[14:18])
	if err != nil {
		return err
	}
	_, err = hex.Decode(u[8:10], b[19:23])
	if err != nil {
		return err
	}
	_, err = hex.Decode(u[10:], b[24:])
	if err != nil {
		return err
	}
	return nil
}

var _ json.Marshaler = (*UUID)(nil)

func (u UUID) MarshalJSON() ([]byte, error) {
	if u.IsZero() {
		return []byte("null"), nil
	}

	b := make([]byte, 0, uuidHexLen+2)
	b = append(b, '"')
	b = appendHex(b, u[:])
	b = append(b, '"')
	return b, nil
}

var _ json.Unmarshaler = (*UUID)(nil)

func (u *UUID) UnmarshalJSON(b []byte) error {
	if len(b) >= 2 {
		b = b[1 : len(b)-1]
	}
	return u.UnmarshalText(b)
}

func unixMicrosecond(tm time.Time) int64 {
	return tm.Unix()*1e6 + int64(tm.Nanosecond())/1e3
}

func fromUnixMicrosecond(n int64) time.Time {
	secs := n / 1e6
	return time.Unix(secs, (n-secs*1e6)*1e3)
}

func appendHex(b []byte, u []byte) []byte {
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
	return b
}
