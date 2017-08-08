package sharding

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/types"
)

// Cluster maps many (up to 8198) logical database shards implemented
// using PostgreSQL schemas to far fewer physical PostgreSQL servers.
type Cluster struct {
	dbs, servers []*pg.DB
	shards       []*pg.DB
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
		shards: make([]*pg.DB, nshards),
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
		cl.shards[i] = newShard(cl.dbs[i%len(cl.dbs)], int64(i))
	}
}

func newShard(db *pg.DB, id int64) *pg.DB {
	name := "shard" + strconv.FormatInt(id, 10)
	return db.WithParam("shard_id", id).
		WithParam("shard", types.F(name)).
		WithParam("epoch", epoch)
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

// DBs returns list of database servers in the cluster.
func (cl *Cluster) DBs() []*pg.DB {
	return cl.dbs
}

// DB maps the number to the corresponding database server.
func (cl *Cluster) DB(number int64) *pg.DB {
	number = number % int64(len(cl.shards))
	number = number % int64(len(cl.dbs))
	return cl.dbs[number]
}

// Shards returns list of shards running in the db. If db is nil all
// shards are returned.
func (cl *Cluster) Shards(db *pg.DB) []*pg.DB {
	if db == nil {
		return cl.shards
	}
	var shards []*pg.DB
	for i, shard := range cl.shards {
		if cl.dbs[i%len(cl.dbs)] == db {
			shards = append(shards, shard)
		}
	}
	return shards
}

// Shard maps the number to the corresponding shard.
func (cl *Cluster) Shard(number int64) *pg.DB {
	number = number % int64(len(cl.shards))
	return cl.shards[number]
}

// SplitShard uses SplitId to extract shard id from the id and then
// returns corresponding cluster Shard.
func (cl *Cluster) SplitShard(id int64) *pg.DB {
	_, shardId, _ := SplitId(id)
	return cl.Shard(shardId)
}

// ForEachDB concurrently calls the fn on each database in the cluster.
func (cl *Cluster) ForEachDB(fn func(db *pg.DB) error) error {
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(len(cl.servers))
	for _, db := range cl.servers {
		go func(db *pg.DB) {
			defer wg.Done()
			if err := fn(db); err != nil {
				select {
				case errCh <- err:
				default:
				}
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
// It is the same as ForEachNShards(1, fn).
func (cl *Cluster) ForEachShard(fn func(shardId int64, shard *pg.DB) error) error {
	return cl.ForEachDB(func(db *pg.DB) error {
		var firstErr error
		for i, shard := range cl.shards {
			if cl.dbs[i%len(cl.dbs)] != db {
				continue
			}

			if err := fn(int64(i), shard); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	})
}

// ForEachNShards concurrently calls the fn on each N shards in the cluster.
func (cl *Cluster) ForEachNShards(n int, fn func(shardId int64, shard *pg.DB) error) error {
	return cl.ForEachDB(func(db *pg.DB) error {
		var wg sync.WaitGroup
		errCh := make(chan error, 1)
		limit := make(chan struct{}, n)

		for i, shard := range cl.shards {
			if cl.dbs[i%len(cl.dbs)] != db {
				continue
			}

			limit <- struct{}{}
			wg.Add(1)
			go func(i int, shard *pg.DB) {
				defer func() {
					<-limit
					wg.Done()
				}()
				if err := fn(int64(i), shard); err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			}(i, shard)
		}

		wg.Wait()

		select {
		case err := <-errCh:
			return err
		default:
			return nil
		}
	})
}
