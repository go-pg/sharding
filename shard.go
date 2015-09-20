package sharding

import (
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"gopkg.in/pg.v3"
)

var Statter statsd.Statter

func init() {
	Statter, _ = statsd.NewNoop()
}

type Shard struct {
	num    int64
	DB     *pg.DB
	oldnew []string
	repl   *strings.Replacer
}

func NewShard(num int64, db *pg.DB, oldnew ...string) *Shard {
	return &Shard{
		num:    num,
		DB:     db,
		oldnew: oldnew,
		repl:   strings.NewReplacer(oldnew...),
	}
}

func (shard *Shard) Num() int64 {
	return shard.num
}

func (shard *Shard) Name() string {
	return "shard" + strconv.FormatInt(shard.num, 10)
}

func (shard *Shard) String() string {
	return shard.Name()
}

func (shard *Shard) UseTimeout(d time.Duration) *Shard {
	newShard := *shard
	newShard.DB = shard.DB.UseTimeout(d)
	return &newShard
}

func (shard *Shard) replaceVars(q string, args []interface{}) (string, error) {
	fq, err := pg.FormatQ(q, args...)
	if err != nil {
		return "", err
	}
	q = shard.repl.Replace(string(fq))
	return q, nil
}

func (shard *Shard) Exec(q string, args ...interface{}) (*pg.Result, error) {
	q, err := shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	res, err := shard.DB.Exec(q)
	dur := int64(time.Since(start) / time.Millisecond)
	Statter.Timing("db.exec", dur, 1)
	return res, err
}

func (shard *Shard) ExecOne(q string, args ...interface{}) (*pg.Result, error) {
	q, err := shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	res, err := shard.DB.ExecOne(q)
	dur := int64(time.Since(start) / time.Millisecond)
	Statter.Timing("db.exec", dur, 1)
	return res, err
}

func (shard *Shard) Query(coll pg.Collection, q string, args ...interface{}) (*pg.Result, error) {
	q, err := shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	res, err := shard.DB.Query(coll, q)
	dur := int64(time.Since(start) / time.Millisecond)
	Statter.Timing("db.query", dur, 1)
	return res, err
}

func (shard *Shard) QueryOne(record interface{}, q string, args ...interface{}) (*pg.Result, error) {
	q, err := shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	res, err := shard.DB.QueryOne(record, q)
	dur := int64(time.Since(start) / time.Millisecond)
	Statter.Timing("db.query", dur, 1)
	return res, err
}

func (shard *Shard) CopyFrom(r io.Reader, q string, args ...interface{}) (*pg.Result, error) {
	q, err := shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	return shard.DB.CopyFrom(r, q)
}

func (shard *Shard) CopyTo(w io.WriteCloser, q string, args ...interface{}) (*pg.Result, error) {
	q, err := shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	return shard.DB.CopyTo(w, q)
}

type Tx struct {
	shard *Shard
	Tx    *pg.Tx
}

func (shard *Shard) Begin() (*Tx, error) {
	tx, err := shard.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{
		shard: shard,
		Tx:    tx,
	}, nil
}

func (tx *Tx) Commit() error {
	return tx.Tx.Commit()
}

func (tx *Tx) Rollback() error {
	return tx.Tx.Rollback()
}

func (tx *Tx) Exec(q string, args ...interface{}) (*pg.Result, error) {
	q, err := tx.shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	return tx.Tx.Exec(q)
}

func (tx *Tx) ExecOne(q string, args ...interface{}) (*pg.Result, error) {
	q, err := tx.shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	return tx.Tx.ExecOne(q)
}

func (tx *Tx) Query(coll pg.Collection, q string, args ...interface{}) (*pg.Result, error) {
	q, err := tx.shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	return tx.Tx.Query(coll, q)
}

func (tx *Tx) QueryOne(record interface{}, q string, args ...interface{}) (*pg.Result, error) {
	q, err := tx.shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	return tx.Tx.QueryOne(record, q)
}
