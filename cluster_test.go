package sharding_test

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/go-pg/sharding"

	"github.com/go-pg/pg"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGinkgo(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "sharding")
}

var _ = Describe("named params", func() {
	var cluster *sharding.Cluster

	BeforeEach(func() {
		db := pg.Connect(&pg.Options{
			User: "postgres",
		})
		cluster = sharding.NewCluster([]*pg.DB{db}, 4)
	})

	It("supports ?shard", func() {
		var shardName, hello string
		_, err := cluster.Shard(3).QueryOne(pg.Scan(&shardName, &hello), `SELECT '?shard', ?`, "hello")
		Expect(err).NotTo(HaveOccurred())
		Expect(shardName).To(Equal(`"shard3"`))
		Expect(hello).To(Equal("hello"))
	})

	It("supports ?shard_id", func() {
		var shardId int
		_, err := cluster.Shard(3).QueryOne(pg.Scan(&shardId), "SELECT ?shard_id")
		Expect(err).NotTo(HaveOccurred())
		Expect(shardId).To(Equal(3))
	})

	It("support ?epoch", func() {
		var epoch int
		_, err := cluster.Shard(0).QueryOne(pg.Scan(&epoch), "SELECT ?epoch")
		Expect(err).NotTo(HaveOccurred())
		Expect(epoch).To(Equal(1262304000000))
	})

	It("supports UUID", func() {
		src := sharding.NewUUID(1234, time.Unix(math.MaxInt64, 0))
		var dst sharding.UUID
		_, err := cluster.Shard(3).QueryOne(pg.Scan(&dst), `SELECT ?`, src)
		Expect(err).NotTo(HaveOccurred())
		Expect(dst).To(Equal(src))
	})
})

var _ = Describe("Cluster", func() {
	var db1, db2 *pg.DB
	var cluster *sharding.Cluster

	BeforeEach(func() {
		db1 = pg.Connect(&pg.Options{
			Addr: "db1",
		})
		db2 = pg.Connect(&pg.Options{
			Addr: "db2",
		})
		Expect(db1).NotTo(Equal(db2))
		cluster = sharding.NewCluster([]*pg.DB{db1, db2, db1, db2}, 4)
	})

	AfterEach(func() {
		Expect(cluster.Close()).NotTo(HaveOccurred())
	})

	It("distributes shards on servers", func() {
		var dbs []*pg.DB
		for i := 0; i < 16; i++ {
			db := pg.Connect(&pg.Options{
				Addr: fmt.Sprintf("db%d", i),
			})
			dbs = append(dbs, db)
		}

		tests := []struct {
			dbs    []int
			db     int
			shards []string
		}{
			{[]int{0}, 0, []string{"0", "1", "2", "3", "4", "5", "6", "7"}},

			{[]int{0, 1}, 0, []string{"0", "2", "4", "6"}},
			{[]int{0, 1}, 1, []string{"1", "3", "5", "7"}},

			{[]int{0, 1, 2, 3}, 0, []string{"0", "4"}},
			{[]int{0, 1, 2, 3}, 1, []string{"1", "5"}},
			{[]int{0, 1, 2, 3}, 2, []string{"2", "6"}},
			{[]int{0, 1, 2, 3}, 3, []string{"3", "7"}},

			{[]int{0, 1, 2, 1}, 0, []string{"0", "4"}},
			{[]int{0, 1, 2, 1}, 1, []string{"1", "3", "5", "7"}},
			{[]int{0, 1, 2, 1}, 2, []string{"2", "6"}},

			{[]int{0, 1, 2, 3}, 0, []string{"0", "4"}},
			{[]int{0, 1, 2, 3}, 1, []string{"1", "5"}},
			{[]int{0, 1, 2, 3}, 2, []string{"2", "6"}},
			{[]int{0, 1, 2, 3}, 3, []string{"3", "7"}},
		}
		for _, test := range tests {
			var cldbs []*pg.DB
			for _, ind := range test.dbs {
				cldbs = append(cldbs, dbs[ind])
			}
			cluster = sharding.NewCluster(cldbs, 8)

			var ss []string
			for _, shard := range cluster.Shards(dbs[test.db]) {
				s := string(shard.FormatQuery(nil, "?shard_id"))
				ss = append(ss, s)
			}
			Expect(ss).To(Equal(test.shards))
		}
	})

	It("distributes projects on different servers", func() {
		tests := []struct {
			projectId int64
			wanted    *pg.DB
		}{
			{0, db1},
			{1, db2},
			{2, db1},
			{3, db2},
			{4, db1},
		}

		for _, test := range tests {
			shard := cluster.Shard(test.projectId)
			Expect(shard.Options()).To(Equal(test.wanted.Options()))
		}
	})

	It("distributes users on different servers", func() {
		tests := []struct {
			username string
			wanted   *pg.DB
		}{
			{"a", db1},
			{"b", db2},
		}

		for _, test := range tests {
			shard := cluster.ShardString(test.username)
			Expect(shard.Options()).To(Equal(test.wanted.Options()))
		}
	})

	Describe("ForEachDB", func() {
		It("fn is called once for every database", func() {
			var dbs []*pg.DB
			var mu sync.Mutex
			err := cluster.ForEachDB(func(db *pg.DB) error {
				defer GinkgoRecover()
				mu.Lock()
				Expect(dbs).NotTo(ContainElement(db))
				dbs = append(dbs, db)
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(dbs).To(HaveLen(2))
		})

		It("returns an error if fn fails", func() {
			err := cluster.ForEachDB(func(db *pg.DB) error {
				if db == db2 {
					return errors.New("fake error")
				}
				return nil
			})
			Expect(err).To(MatchError("fake error"))
		})
	})

	Describe("ForEachShard", func() {
		It("fn is called once for every shard", func() {
			var shards []string
			var mu sync.Mutex
			err := cluster.ForEachShard(func(shard *pg.DB) error {
				defer GinkgoRecover()

				s := string(shard.FormatQuery(nil, "?shard_id"))

				mu.Lock()
				Expect(shards).NotTo(ContainElement(s))
				shards = append(shards, s)
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(shards).To(HaveLen(4))
			for i := 0; i < 4; i++ {
				Expect(shards).To(ContainElement(fmt.Sprintf("%d", i)))
			}
		})

		It("returns an error if fn fails", func() {
			err := cluster.ForEachShard(func(shard *pg.DB) error {
				s := string(shard.FormatQuery(nil, "?shard_id"))
				if s == "3" {
					return errors.New("fake error")
				}
				return nil
			})
			Expect(err).To(MatchError("fake error"))
		})
	})

	Describe("ForEachNShards", func() {
		It("fn is called once for every shard", func() {
			var shards []string
			var mu sync.Mutex
			err := cluster.ForEachNShards(2, func(shard *pg.DB) error {
				defer GinkgoRecover()

				s := string(shard.FormatQuery(nil, "?shard_id"))

				mu.Lock()
				Expect(shards).NotTo(ContainElement(s))
				shards = append(shards, s)
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(shards).To(HaveLen(4))
			for i := 0; i < 4; i++ {
				Expect(shards).To(ContainElement(fmt.Sprintf("%d", i)))
			}
		})

		It("returns an error if fn fails", func() {
			err := cluster.ForEachNShards(2, func(shard *pg.DB) error {
				s := string(shard.FormatQuery(nil, "?shard_id"))
				if s == "3" {
					return errors.New("fake error")
				}
				return nil
			})
			Expect(err).To(MatchError("fake error"))
		})
	})
})
