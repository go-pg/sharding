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
	fmt   orm.Formatter
}

func (q shardQuery) AppendQuery(dst []byte, params ...interface{}) ([]byte, error) {
	b := buffers.Get().([]byte)
	defer buffers.Put(b)

	b = b[:0]
	switch query := q.query.(type) {
	case string:
		b = append(b, query...)
	case orm.QueryAppender:
		var err error
		b, err = query.AppendQuery(b)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported query type: %T", query)
	}

	return q.fmt.AppendBytes(dst, b, params...)
}
