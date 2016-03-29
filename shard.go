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
// - SHARD is replaced with shard name, e.g. shard1234.
// - SHARD_ID is replaced with shard id, .e.g. 1234.
type Shard struct {
	id  int
	DB  *pg.DB
	fmt orm.Formatter
}

func NewShard(id int, db *pg.DB) *Shard {
	shard := &Shard{
		id: id,
		DB: db,
	}
	shard.fmt.SetParam("shard", pg.F(shard.Name()))
	shard.fmt.SetParam("shard_id", shard.Id())
	return shard
}

func (shard *Shard) Id() int {
	return shard.id
}

func (shard *Shard) Name() string {
	return "shard" + strconv.Itoa(shard.id)
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
func (shard *Shard) Exec(query interface{}, params ...interface{}) (types.Result, error) {
	q := shardQuery{
		query: query,
		fmt:   shard.fmt,
	}
	return shard.DB.Exec(q)
}

// ExecOne is an alias for pg.DB.ExecOne.
func (shard *Shard) ExecOne(query interface{}, params ...interface{}) (types.Result, error) {
	q := shardQuery{
		query: query,
		fmt:   shard.fmt,
	}
	return shard.DB.ExecOne(q)
}

// Query is an alias for pg.DB.Query.
func (shard *Shard) Query(model, query interface{}, params ...interface{}) (types.Result, error) {
	q := shardQuery{
		query: query,
		fmt:   shard.fmt,
	}
	return shard.DB.Query(model, q)
}

// QueryOne is an alias for pg.DB.QueryOne.
func (shard *Shard) QueryOne(model, query interface{}, params ...interface{}) (types.Result, error) {
	q := shardQuery{
		query: query,
		fmt:   shard.fmt,
	}
	return shard.DB.QueryOne(model, q)
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
func (shard *Shard) CopyFrom(r io.Reader, query interface{}, params ...interface{}) (types.Result, error) {
	q := shardQuery{
		query: query,
		fmt:   shard.fmt,
	}
	return shard.DB.CopyFrom(r, q)
}

// CopyTo is an alias for pg.DB.CopyTo.
func (shard *Shard) CopyTo(w io.WriteCloser, query interface{}, params ...interface{}) (types.Result, error) {
	q := shardQuery{
		query: query,
		fmt:   shard.fmt,
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
func (tx *Tx) Exec(query interface{}, params ...interface{}) (types.Result, error) {
	q := shardQuery{
		query: query,
		fmt:   tx.shard.fmt,
	}
	return tx.Tx.Exec(q)
}

// ExecOne is an alias for pg.Tx.ExecOne.
func (tx *Tx) ExecOne(query interface{}, params ...interface{}) (types.Result, error) {
	q := shardQuery{
		query: query,
		fmt:   tx.shard.fmt,
	}
	return tx.Tx.ExecOne(q)
}

// Query is an alias for pg.Tx.Query.
func (tx *Tx) Query(model, query interface{}, params ...interface{}) (types.Result, error) {
	q := shardQuery{
		query: query,
		fmt:   tx.shard.fmt,
	}
	return tx.Tx.Query(model, q)
}

// QueryOne is an alias for pg.Tx.QueryOne.
func (tx *Tx) QueryOne(model, query interface{}, params ...interface{}) (types.Result, error) {
	q := shardQuery{
		query: query,
		fmt:   tx.shard.fmt,
	}
	return tx.Tx.QueryOne(model, q)
}
