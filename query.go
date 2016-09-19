package sharding

import (
	"fmt"

	"gopkg.in/pg.v5/orm"
)

type shardQuery struct {
	query interface{}
	fmter orm.Formatter
}

var _ orm.QueryAppender = (*shardQuery)(nil)

func (q shardQuery) AppendQuery(dst []byte, params ...interface{}) ([]byte, error) {
	switch query := q.query.(type) {
	case string:
		return q.fmter.Append(dst, query, params...), nil
	case orm.QueryAppender:
		return query.AppendQuery(dst, params...)
	default:
		return nil, fmt.Errorf("unsupported query type: %T", query)
	}
}
