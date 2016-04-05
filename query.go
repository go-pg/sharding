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
	case orm.QueryAppender:
		var err error
		b, err = query.AppendQuery(b[:0], nil)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported query type: %T", query)
	}

	return q.fmter.AppendBytes(dst, b, params, false), nil
}
