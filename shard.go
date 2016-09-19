package sharding

import (
	"io"
	"strconv"
	"time"

	"gopkg.in/pg.v5"
	"gopkg.in/pg.v5/orm"
	"gopkg.in/pg.v5/types"
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
	shard.fmter.SetParam("shard", types.F(shard.Name()))
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

// WithTimeout returns a Shard that uses d as the read/write timeout.
func (shard *Shard) WithTimeout(d time.Duration) *Shard {
	newShard := *shard
	newShard.DB = shard.DB.WithTimeout(d)
	return &newShard
}

// Exec executes a query ignoring returned rows. The params are for any
// placeholder parameters in the query.
func (shard *Shard) Exec(query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: shard.fmter,
	}
	return shard.DB.Exec(q, params...)
}

// ExecOne acts like Exec, but query must affect only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (shard *Shard) ExecOne(query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: shard.fmter,
	}
	return shard.DB.ExecOne(q, params...)
}

// Query executes a query that returns rows, typically a SELECT.
// The params are for any placeholder parameters in the query.
func (shard *Shard) Query(model, query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: shard.fmter,
	}
	return shard.DB.Query(model, q, params...)
}

// QueryOne acts like Query, but query must return only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (shard *Shard) QueryOne(model, query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: shard.fmter,
	}
	return shard.DB.QueryOne(model, q, params...)
}

// CopyFrom copies data from reader to a table.
func (shard *Shard) CopyFrom(r io.Reader, query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: shard.fmter,
	}
	return shard.DB.CopyFrom(r, q, params...)
}

// CopyTo copies data from a table to writer.
func (shard *Shard) CopyTo(w io.WriteCloser, query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: shard.fmter,
	}
	return shard.DB.CopyTo(w, q, params...)
}

// Model returns new query for the model.
func (shard *Shard) Model(model ...interface{}) *orm.Query {
	return orm.NewQuery(shard, model...)
}

// Select selects the model by primary key.
func (shard *Shard) Select(model interface{}) error {
	return orm.Select(shard, model)
}

// Insert inserts the model updating primary keys if they are empty.
func (shard *Shard) Insert(model ...interface{}) error {
	return orm.Insert(shard, model...)
}

// Update updates the model by primary key.
func (shard *Shard) Update(model interface{}) error {
	return orm.Update(shard, model)
}

// Delete deletes the model by primary key.
func (shard *Shard) Delete(model interface{}) error {
	return orm.Delete(shard, model)
}

func (shard *Shard) FormatQuery(dst []byte, query string, params ...interface{}) []byte {
	return shard.fmter.Append(dst, query, params...)
}

// Tx is an in-progress database transaction.
//
// A transaction must end with a call to Commit or Rollback.
//
// After a call to Commit or Rollback, all operations on the transaction fail
// with ErrTxDone.
//
// The statements prepared for a transaction by calling the transaction's
// Prepare or Stmt methods are closed by the call to Commit or Rollback.
type Tx struct {
	Shard *Shard
	Tx    *pg.Tx
}

// Begin starts a transaction. Most callers should use RunInTransaction instead.
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

// RunInTransaction runs a function in a transaction. If function
// returns an error transaction is rollbacked, otherwise transaction
// is committed.
func (shard *Shard) RunInTransaction(fn func(*Tx) error) error {
	tx, err := shard.Begin()
	if err != nil {
		return err
	}

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Commit commits the transaction.
func (tx *Tx) Commit() error {
	return tx.Tx.Commit()
}

// Rollback aborts the transaction.
func (tx *Tx) Rollback() error {
	return tx.Tx.Rollback()
}

// Exec executes a query with the given parameters in a transaction.
func (tx *Tx) Exec(query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: tx.Shard.fmter,
	}
	return tx.Tx.Exec(q, params...)
}

// ExecOne acts like Exec, but query must affect only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (tx *Tx) ExecOne(query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: tx.Shard.fmter,
	}
	return tx.Tx.ExecOne(q, params...)
}

// Query executes a query with the given parameters in a transaction.
func (tx *Tx) Query(model, query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: tx.Shard.fmter,
	}
	return tx.Tx.Query(model, q, params...)
}

// QueryOne acts like Query, but query must return only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (tx *Tx) QueryOne(model, query interface{}, params ...interface{}) (*types.Result, error) {
	q := shardQuery{
		query: query,
		fmter: tx.Shard.fmter,
	}
	return tx.Tx.QueryOne(model, q, params...)
}

// Model returns new query for the model.
func (tx *Tx) Model(model ...interface{}) *orm.Query {
	return orm.NewQuery(tx, model...)
}

// Select selects the model by primary key.
func (tx *Tx) Select(model interface{}) error {
	return orm.Select(tx, model)
}

// Insert inserts the model updating primary keys if they are empty.
func (tx *Tx) Insert(model ...interface{}) error {
	return orm.Insert(tx, model...)
}

// Update updates the model by primary key.
func (tx *Tx) Update(model interface{}) error {
	return orm.Update(tx, model)
}

// Delete deletes the model by primary key.
func (tx *Tx) Delete(model interface{}) error {
	return orm.Delete(tx, model)
}

func (tx *Tx) FormatQuery(dst []byte, query string, params ...interface{}) []byte {
	return tx.Shard.FormatQuery(dst, query, params...)
}
