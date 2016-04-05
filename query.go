package sharding

import (
	"fmt"
	"sync"

	"gopkg.in/pg.v4/orm"
)

var buffers = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}

type shardQuery struct {
	query interface{}
	fmter orm.Formatter
}

var _ orm.QueryAppender = (*shardQuery)(nil)

func (q shardQuery) AppendQuery(dst []byte, params []interface{}) ([]byte, error) {
	b := buffers.Get().([]byte)
	defer buffers.Put(b)

	switch query := q.query.(type) {
	case string:
		b = append(b[:0], query...)
		return q.fmter.AppendBytes(dst, b, params, true), nil
	case orm.QueryAppender:
		var err error
		b, err = query.AppendQuery(b[:0], nil)
		if err != nil {
			return nil, err
		}
		return q.fmter.AppendBytes(dst, b, params, false), nil
	default:
		return nil, fmt.Errorf("unsupported query type: %T", query)
	}
}
