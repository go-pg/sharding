package sharding // import "gopkg.in/go-pg/sharding.v4"

import (
	"fmt"
	"hash/fnv"
	"io"
	"sync"

	"gopkg.in/pg.v4"
)

// Cluster maps many (up to 8198) logical database shards implemented
// using PostgreSQL schemas to far fewer physical PostgreSQL servers.
type Cluster struct {
	dbs, servers []*pg.DB
	shards       []*Shard
}

// NewCluster returns new PostgreSQL cluster consisting of physical
// dbs and running nshards logical shards.
func NewCluster(dbs []*pg.DB, nshards int) *Cluster {
	if len(dbs) == 0 {
		panic("at least one db is required")
	}
	if nshards == 0 {
		panic("at least on shard is required")
	}
	if len(dbs) > int(shardMask+1) || nshards > int(shardMask+1) {
		panic(fmt.Sprintf("too many shards"))
	}
	if nshards < len(dbs) {
		panic("number of shards must be greater or equal number of dbs")
	}
	if nshards%len(dbs) != 0 {
		panic("number of shards must be divideable by number of dbs")
	}
	cl := &Cluster{
		dbs:    dbs,
		shards: make([]*Shard, nshards),
	}
	cl.init()
	return cl
}

func (cl *Cluster) init() {
	dbSet := make(map[*pg.DB]struct{})
	for _, db := range cl.dbs {
		if _, ok := dbSet[db]; ok {
			continue
		}
		dbSet[db] = struct{}{}
		cl.servers = append(cl.servers, db)
	}

	for i := 0; i < len(cl.shards); i++ {
		cl.shards[i] = NewShard(int64(i), cl.dbs[i%len(cl.dbs)])
	}
}

func (cl *Cluster) Close() error {
	var retErr error
	for _, db := range cl.servers {
		if err := db.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}
	return retErr
}

// DBIndex returns a db index for the shardId.
func (cl *Cluster) DBIndex(shardId int64) int {
	return int(shardId % int64(len(cl.dbs)))
}

// DBs returns list of unique databases in the cluster.
func (cl *Cluster) DBs() []*pg.DB {
	return cl.servers
}

// Shards returns list of shards running in the db. If db is nil all
// shards are returned.
func (cl *Cluster) Shards(db *pg.DB) []*Shard {
	if db == nil {
		return cl.shards
	}
	var shards []*Shard
	for _, shard := range cl.shards {
		if shard.DB == db {
			shards = append(shards, shard)
		}
	}
	return shards
}

// Shard maps the number to a shard in the cluster.
func (cl *Cluster) Shard(number int64) *Shard {
	number = number % int64(len(cl.shards))
	return cl.shards[number]
}

// ShardString maps the str to a shard in the cluster.
func (cl *Cluster) ShardString(str string) *Shard {
	h := fnv.New32a()
	io.WriteString(h, str)
	return cl.Shard(int64(h.Sum32()))
}

// SplitShard uses SplitId to extract shard id from the id and then
// returns corresponding cluster Shard.
func (cl *Cluster) SplitShard(id int64) *Shard {
	_, shardId, _ := SplitId(id)
	return cl.Shard(shardId)
}

// ForEachDB concurrently calls the fn on each database in the cluster.
func (cl *Cluster) ForEachDB(fn func(db *pg.DB) error) error {
	errCh := make(chan error, len(cl.dbs))
	var wg sync.WaitGroup
	wg.Add(len(cl.servers))
	for _, db := range cl.servers {
		go func(db *pg.DB) {
			defer wg.Done()
			if err := fn(db); err != nil {
				errCh <- err
			}
		}(db)
	}
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// ForEachShard concurrently calls the fn on each shard in the cluster.
func (cl *Cluster) ForEachShard(fn func(shard *Shard) error) error {
	return cl.ForEachDB(func(db *pg.DB) error {
		var retErr error
		for _, shard := range cl.Shards(db) {
			if err := fn(shard); err != nil && retErr == nil {
				retErr = err
			}
		}
		return retErr
	})
}
