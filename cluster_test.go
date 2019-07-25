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

	It("supports ?shard", func() {
		var shardName, hello string
		_, err := cluster.Shard(3).QueryOne(
			pg.Scan(&shardName, &hello), `SELECT '?shard', ?`, "hello")
		Expect(err).NotTo(HaveOccurred())
		Expect(shardName).To(Equal("shard3"))
		Expect(hello).To(Equal("hello"))
	})

	It("supports ?shard_id", func() {
		var shardId int
		_, err := cluster.Shard(3).QueryOne(pg.Scan(&shardId), "SELECT ?shard_id")
		Expect(err).NotTo(HaveOccurred())
		Expect(shardId).To(Equal(3))
	})

	It("supports ?epoch", func() {
		var epoch int64
		_, err := cluster.Shard(0).QueryOne(pg.Scan(&epoch), "SELECT ?epoch")
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
				Expect(shards).NotTo(ContainElement(shardId))
				shards = append(shards, shardId(shard))
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(shards).To(HaveLen(4))
			for shardId := int64(0); shardId < 4; shardId++ {
				Expect(shards).To(ContainElement(shardId))
			}
		})

		It("returns an error if fn fails", func() {
			err := cluster.ForEachShard(func(shard *pg.DB) error {
				if shardId(shard) == 3 {
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

				shardId := shardId(shard)
				mu.Lock()
				Expect(shards).NotTo(ContainElement(shardId))
				shards = append(shards, shardId)
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(shards).To(HaveLen(4))
			for shardId := int64(0); shardId < 4; shardId++ {
				Expect(shards).To(ContainElement(shardId))
			}
		})

		It("returns an error if fn fails", func() {
			err := cluster.ForEachNShards(2, func(shard *pg.DB) error {
				if shardId(shard) == 3 {
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
				shardIds []int64
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
				var shardIds []int64
				dbs := make(map[*pg.Options]struct{})
				err := test.subcl.ForEachShard(func(shard *pg.DB) error {
					mu.Lock()
					shardIds = append(shardIds, shardId(shard))
					dbs[shard.Options()] = struct{}{}
					mu.Unlock()
					return nil
				})
				Expect(err).NotTo(HaveOccurred())
				sort.Slice(shardIds, func(i, j int) bool {
					return shardIds[i] < shardIds[j]
				})
				Expect(shardIds).To(Equal(test.shardIds))
				Expect(len(dbs)).To(Equal(min(len(alldbs), len(shardIds))))

				shardIds = shardIds[:0]
				add := func(shardId int64) {
					for _, id := range shardIds {
						if id == shardId {
							return
						}
					}
					shardIds = append(shardIds, shardId)
				}

				for i := 0; i < 16; i++ {
					shard := test.subcl.Shard(int64(i))
					shardId := shardId(shard)
					Expect(test.shardIds).To(ContainElement(shardId), "number=%d", i)
					add(shardId)
				}
				Expect(shardIds).To(Equal(test.shardIds))
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

			var shardIds []int64
			for _, shard := range cluster.Shards(dbs[test.db]) {
				shardIds = append(shardIds, shardId(shard))
			}
			Expect(shardIds).To(Equal(test.shards))
		}
	})
})

func shardId(shard *pg.DB) int64 {
	return shard.Param("shard_id").(int64)
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
