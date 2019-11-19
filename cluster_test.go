package sharding_test

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/go-pg/sharding/v7"

	"github.com/go-pg/pg/v9"
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

	It("supports ?SHARD", func() {
		var shardName, hello string
		_, err := cluster.Shard(3).QueryOne(
			pg.Scan(&shardName, &hello), `SELECT '?SHARD', ?`, "hello")
		Expect(err).NotTo(HaveOccurred())
		Expect(shardName).To(Equal("shard3"))
		Expect(hello).To(Equal("hello"))
	})

	It("supports ?SHARD_ID", func() {
		var shardID int
		_, err := cluster.Shard(3).QueryOne(pg.Scan(&shardID), "SELECT ?SHARD_ID")
		Expect(err).NotTo(HaveOccurred())
		Expect(shardID).To(Equal(3))
	})

	It("supports ?EPOCH", func() {
		var epoch int64
		_, err := cluster.Shard(0).QueryOne(pg.Scan(&epoch), "SELECT ?EPOCH")
		Expect(err).NotTo(HaveOccurred())
		Expect(epoch).To(Equal(int64(1262304000000)))
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
			var shards []int64
			var mu sync.Mutex
			err := cluster.ForEachShard(func(shard *pg.DB) error {
				defer GinkgoRecover()

				mu.Lock()
				Expect(shards).NotTo(ContainElement(shardID))
				shards = append(shards, shardID(shard))
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(shards).To(HaveLen(4))
			for shardID := int64(0); shardID < 4; shardID++ {
				Expect(shards).To(ContainElement(shardID))
			}
		})

		It("returns an error if fn fails", func() {
			err := cluster.ForEachShard(func(shard *pg.DB) error {
				if shardID(shard) == 3 {
					return errors.New("fake error")
				}
				return nil
			})
			Expect(err).To(MatchError("fake error"))
		})
	})

	Describe("ForEachNShards", func() {
		It("fn is called once for every shard", func() {
			var shards []int64
			var mu sync.Mutex
			err := cluster.ForEachNShards(2, func(shard *pg.DB) error {
				defer GinkgoRecover()

				shardID := shardID(shard)
				mu.Lock()
				Expect(shards).NotTo(ContainElement(shardID))
				shards = append(shards, shardID)
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(shards).To(HaveLen(4))
			for shardID := int64(0); shardID < 4; shardID++ {
				Expect(shards).To(ContainElement(shardID))
			}
		})

		It("returns an error if fn fails", func() {
			err := cluster.ForEachNShards(2, func(shard *pg.DB) error {
				if shardID(shard) == 3 {
					return errors.New("fake error")
				}
				return nil
			})
			Expect(err).To(MatchError("fake error"))
		})
	})

	Describe("SubCluster", func() {
		var alldbs []*pg.DB

		BeforeEach(func() {
			alldbs = alldbs[:0]
			for i := 0; i < 4; i++ {
				alldbs = append(alldbs, pg.Connect(&pg.Options{
					Addr: "db1",
				}))
			}
			cluster = sharding.NewCluster(alldbs, 8)
		})

		It("creates sub-cluster", func() {
			tests := []struct {
				subcl    *sharding.SubCluster
				shardIDs []int64
			}{
				{cluster.SubCluster(0, 2), []int64{0, 1}},
				{cluster.SubCluster(1, 2), []int64{2, 3}},
				{cluster.SubCluster(2, 2), []int64{4, 5}},
				{cluster.SubCluster(3, 2), []int64{6, 7}},
				{cluster.SubCluster(4, 2), []int64{0, 1}},

				{cluster.SubCluster(0, 4), []int64{0, 1, 2, 3}},
				{cluster.SubCluster(1, 4), []int64{4, 5, 6, 7}},
				{cluster.SubCluster(2, 4), []int64{0, 1, 2, 3}},

				{cluster.SubCluster(0, 8), []int64{0, 1, 2, 3, 4, 5, 6, 7}},
				{cluster.SubCluster(1, 8), []int64{0, 1, 2, 3, 4, 5, 6, 7}},

				{cluster.SubCluster(0, 16), []int64{0, 1, 2, 3, 4, 5, 6, 7}},
				{cluster.SubCluster(1, 16), []int64{0, 1, 2, 3, 4, 5, 6, 7}},
			}
			for _, test := range tests {
				var mu sync.Mutex
				var shardIDs []int64
				dbs := make(map[*pg.Options]struct{})
				err := test.subcl.ForEachShard(func(shard *pg.DB) error {
					mu.Lock()
					shardIDs = append(shardIDs, shardID(shard))
					dbs[shard.Options()] = struct{}{}
					mu.Unlock()
					return nil
				})
				Expect(err).NotTo(HaveOccurred())
				sort.Slice(shardIDs, func(i, j int) bool {
					return shardIDs[i] < shardIDs[j]
				})
				Expect(shardIDs).To(Equal(test.shardIDs))
				Expect(len(dbs)).To(Equal(min(len(alldbs), len(shardIDs))))

				shardIDs = shardIDs[:0]
				add := func(shardID int64) {
					for _, id := range shardIDs {
						if id == shardID {
							return
						}
					}
					shardIDs = append(shardIDs, shardID)
				}

				for i := 0; i < 16; i++ {
					shard := test.subcl.Shard(int64(i))
					shardID := shardID(shard)
					Expect(test.shardIDs).To(ContainElement(shardID), "number=%d", i)
					add(shardID)
				}
				Expect(shardIDs).To(Equal(test.shardIDs))
			}
		})
	})
})

var _ = Describe("Cluster shards", func() {
	It("are distributed across the servers", func() {
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
			shards []int64
		}{
			{[]int{0}, 0, []int64{0, 1, 2, 3, 4, 5, 6, 7}},

			{[]int{0, 1}, 0, []int64{0, 2, 4, 6}},
			{[]int{0, 1}, 1, []int64{1, 3, 5, 7}},

			{[]int{0, 1, 2, 1}, 0, []int64{0, 4}},
			{[]int{0, 1, 2, 1}, 1, []int64{1, 3, 5, 7}},
			{[]int{0, 1, 2, 1}, 2, []int64{2, 6}},

			{[]int{0, 1, 2, 3}, 0, []int64{0, 4}},
			{[]int{0, 1, 2, 3}, 1, []int64{1, 5}},
			{[]int{0, 1, 2, 3}, 2, []int64{2, 6}},
			{[]int{0, 1, 2, 3}, 3, []int64{3, 7}},
		}
		for _, test := range tests {
			var cldbs []*pg.DB
			for _, ind := range test.dbs {
				cldbs = append(cldbs, dbs[ind])
			}
			cluster := sharding.NewCluster(cldbs, 8)

			var shardIDs []int64
			for _, shard := range cluster.Shards(dbs[test.db]) {
				shardIDs = append(shardIDs, shardID(shard))
			}
			Expect(shardIDs).To(Equal(test.shards))
		}
	})
})

func shardID(shard *pg.DB) int64 {
	return shard.Param("SHARD_ID").(int64)
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
