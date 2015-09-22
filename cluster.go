package sharding

import (
	"fmt"
	"strconv"
	"sync"

	"gopkg.in/pg.v3"
)

// Cluster maps many (up to 2048) logical database shards to far fewer
// physical PostgreSQL server instances.
type Cluster struct {
	dbs    []*pg.DB
	shards []*Shard
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
	for i := 0; i < len(cl.shards); i++ {
		cl.shards[i] = newShard(int64(i), cl.dbs[i%len(cl.dbs)],
			"SHARD_ID", strconv.Itoa(i),
			"SHARD", "shard"+strconv.Itoa(i),
		)
	}
	return cl
}

// DBs returns list of databases in the cluster.
func (cl *Cluster) DBs() []*pg.DB {
	return cl.dbs
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

// Shard maps the id to a shard in the cluster.
func (cl *Cluster) Shard(id int64) *Shard {
	n := id % int64(len(cl.shards))
	return cl.shards[n]
}

// ForEachDB concurrently calls the fn on each database in the cluster.
func (cl *Cluster) ForEachDB(fn func(db *pg.DB) error) error {
	errCh := make(chan error, len(cl.dbs))
	var wg sync.WaitGroup
	dbSet := make(map[*pg.DB]struct{})
	for _, db := range cl.dbs {
		if _, ok := dbSet[db]; ok {
			continue
		}
		dbSet[db] = struct{}{}

		wg.Add(1)
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
		for _, shard := range cl.Shards(db) {
			if err := fn(shard); err != nil {
				return err
			}
		}
		return nil
	})
}
