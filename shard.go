package sharding

import (
	"io"
	"strconv"
	"time"

	"gopkg.in/pg.v4"
	"gopkg.in/pg.v4/orm"
	"gopkg.in/pg.v4/types"
)

// Shard represents logical shard in Cluster and is responsible for
// executing queries against the shard. Shard provides go-pg
// compatible API and replaces following patterns in the query:
// - ?shard is replaced with shard name, e.g. shard1234.
// - ?shard_id is replaced with shard id, .e.g. 1234.
type Shard struct {
	id    int64
	DB    *pg.DB
	fmter orm.Formatter
}

func NewShard(id int64, db *pg.DB) *Shard {
	shard := &Shard{
		id: id,
		DB: db,
	}
	shard.fmter.SetParam("shard", pg.Q(shard.Name()))
	shard.fmter.SetParam("shard_id", shard.Id())
	return shard
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

// Exec is an alias for pg.DB.Exec.
func (shard *Shard) Exec(query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: shard.fmter,
	}
	return shard.DB.Exec(q, params...)
}

// ExecOne is an alias for pg.DB.ExecOne.
func (shard *Shard) ExecOne(query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: shard.fmter,
	}
	return shard.DB.ExecOne(q, params...)
}

// Query is an alias for pg.DB.Query.
func (shard *Shard) Query(model, query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: shard.fmter,
	}
	return shard.DB.Query(model, q, params...)
}

// QueryOne is an alias for pg.DB.QueryOne.
func (shard *Shard) QueryOne(model, query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: shard.fmter,
	}
	return shard.DB.QueryOne(model, q, params...)
}

func (shard *Shard) Model(model interface{}) *orm.Query {
	return orm.NewQuery(shard, model)
}

func (shard *Shard) Create(model interface{}) error {
	return orm.Create(shard, model)
}

func (shard *Shard) Update(model interface{}) error {
	return orm.Update(shard, model)
}

func (shard *Shard) Delete(model interface{}) error {
	return orm.Delete(shard, model)
}

// CopyFrom is an alias for pg.DB.CopyFrom.
func (shard *Shard) CopyFrom(r io.Reader, query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: shard.fmter,
	}
	return shard.DB.CopyFrom(r, q, params...)
}

// CopyTo is an alias for pg.DB.CopyTo.
func (shard *Shard) CopyTo(w io.WriteCloser, query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: shard.fmter,
	}
	return shard.DB.CopyTo(w, q, params...)
}

func (shard *Shard) FormatQuery(dst []byte, query string, params ...interface{}) []byte {
	return shard.fmter.Append(dst, query, params...)
}

// Tx is an alias for pg.Tx and provides same API.
type Tx struct {
	Shard *Shard
	Tx    *pg.Tx
}

// Begin is an alias for pg.DB.Begin.
func (shard *Shard) Begin() (*Tx, error) {
	tx, err := shard.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{
		Shard: shard,
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
func (tx *Tx) Exec(query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: tx.Shard.fmter,
	}
	return tx.Tx.Exec(q, params...)
}

// ExecOne is an alias for pg.Tx.ExecOne.
func (tx *Tx) ExecOne(query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: tx.Shard.fmter,
	}
	return tx.Tx.ExecOne(q, params...)
}

// Query is an alias for pg.Tx.Query.
func (tx *Tx) Query(model, query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: tx.Shard.fmter,
	}
	return tx.Tx.Query(model, q, params...)
}

// QueryOne is an alias for pg.Tx.QueryOne.
func (tx *Tx) QueryOne(model, query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: tx.Shard.fmter,
	}
	return tx.Tx.QueryOne(model, q, params...)
}

func (tx *Tx) FormatQuery(dst []byte, query string, params ...interface{}) []byte {
	return tx.Shard.FormatQuery(dst, query, params...)
}
