package sharding

import (
	"io"
	"strconv"
	"strings"
	"time"

	"gopkg.in/pg.v3"
)

// Shard represents logical shard in Cluster and is responsible for
// executing queries against the shard. Shard provides go-pg
// compatible API and replaces following patterns in the query:
// - SHARD is replaced with shard name, e.g. shard1234.
// - SHARD_ID is replaced with shard id, .e.g. 1234.
type Shard struct {
	id     int64
	DB     *pg.DB
	oldnew []string
	repl   *strings.Replacer
}

func NewShard(id int64, db *pg.DB, oldnew ...string) *Shard {
	return &Shard{
		id:     id,
		DB:     db,
		oldnew: oldnew,
		repl:   strings.NewReplacer(oldnew...),
	}
}

func (shard *Shard) Id() int64 {
	return shard.id
}

func (shard *Shard) Name() string {
	return "shard" + strconv.FormatInt(shard.id, 10)
}

func (shard *Shard) String() string {
	return shard.Name()
}

// WithTimeout is an alias for pg.DB.WithTimeout.
func (shard *Shard) UseTimeout(d time.Duration) *Shard {
	newShard := *shard
	newShard.DB = shard.DB.WithTimeout(d)
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

// Exec is an alias for pg.DB.Exec.
func (shard *Shard) Exec(q string, args ...interface{}) (pg.Result, error) {
	q, err := shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	return shard.DB.Exec(q)
}

// ExecOne is an alias for pg.DB.ExecOne.
func (shard *Shard) ExecOne(q string, args ...interface{}) (pg.Result, error) {
	q, err := shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	return shard.DB.ExecOne(q)
}

// Query is an alias for pg.DB.Query.
func (shard *Shard) Query(coll interface{}, q string, args ...interface{}) (pg.Result, error) {
	q, err := shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	return shard.DB.Query(coll, q)
}

// QueryOne is an alias for pg.DB.QueryOne.
func (shard *Shard) QueryOne(record interface{}, q string, args ...interface{}) (pg.Result, error) {
	q, err := shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	return shard.DB.QueryOne(record, q)
}

// CopyFrom is an alias for pg.DB.CopyFrom.
func (shard *Shard) CopyFrom(r io.Reader, q string, args ...interface{}) (pg.Result, error) {
	q, err := shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	return shard.DB.CopyFrom(r, q)
}

// CopyTo is an alias for pg.DB.CopyTo.
func (shard *Shard) CopyTo(w io.WriteCloser, q string, args ...interface{}) (pg.Result, error) {
	q, err := shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	return shard.DB.CopyTo(w, q)
}

// Tx is an alias for pg.Tx and provides same API.
type Tx struct {
	shard *Shard
	Tx    *pg.Tx
}

// Begin is an alias for pg.DB.Begin.
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

// Commit is an alias for pg.Tx.Commit.
func (tx *Tx) Commit() error {
	return tx.Tx.Commit()
}

// Rollback is an alias for pg.Tx.Rollback.
func (tx *Tx) Rollback() error {
	return tx.Tx.Rollback()
}

// Exec is an alias for pg.Tx.Exec.
func (tx *Tx) Exec(q string, args ...interface{}) (pg.Result, error) {
	q, err := tx.shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	return tx.Tx.Exec(q)
}

// ExecOne is an alias for pg.Tx.ExecOne.
func (tx *Tx) ExecOne(q string, args ...interface{}) (pg.Result, error) {
	q, err := tx.shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	return tx.Tx.ExecOne(q)
}

// Query is an alias for pg.Tx.Query.
func (tx *Tx) Query(coll interface{}, q string, args ...interface{}) (pg.Result, error) {
	q, err := tx.shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	return tx.Tx.Query(coll, q)
}

// QueryOne is an alias for pg.Tx.QueryOne.
func (tx *Tx) QueryOne(record interface{}, q string, args ...interface{}) (pg.Result, error) {
	q, err := tx.shard.replaceVars(q, args)
	if err != nil {
		return nil, err
	}
	return tx.Tx.QueryOne(record, q)
}
