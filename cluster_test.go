package sharding_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/pg.v5"

	"gopkg.in/go-pg/sharding.v5"
)

func TestSharding(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "sharding")
}

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
			dbs []int
			db  int
			s   string
		}{
			{[]int{0}, 0, "[shard0 shard1 shard2 shard3 shard4 shard5 shard6 shard7]"},

			{[]int{0, 1}, 0, "[shard0 shard2 shard4 shard6]"},
			{[]int{0, 1}, 1, "[shard1 shard3 shard5 shard7]"},

			{[]int{0, 1, 2, 3}, 0, "[shard0 shard4]"},
			{[]int{0, 1, 2, 3}, 1, "[shard1 shard5]"},
			{[]int{0, 1, 2, 3}, 2, "[shard2 shard6]"},
			{[]int{0, 1, 2, 3}, 3, "[shard3 shard7]"},

			{[]int{0, 1, 2, 1}, 0, "[shard0 shard4]"},
			{[]int{0, 1, 2, 1}, 1, "[shard1 shard3 shard5 shard7]"},
			{[]int{0, 1, 2, 1}, 2, "[shard2 shard6]"},

			{[]int{0, 1, 2, 3}, 0, "[shard0 shard4]"},
			{[]int{0, 1, 2, 3}, 1, "[shard1 shard5]"},
			{[]int{0, 1, 2, 3}, 2, "[shard2 shard6]"},
			{[]int{0, 1, 2, 3}, 3, "[shard3 shard7]"},
		}
		for _, test := range tests {
			var cldbs []*pg.DB
			for _, ind := range test.dbs {
				cldbs = append(cldbs, dbs[ind])
			}
			cluster = sharding.NewCluster(cldbs, 8)
			shards := cluster.Shards(dbs[test.db])
			Expect(fmt.Sprint(shards)).To(Equal(test.s))
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
			Expect(shard.DB).To(Equal(test.wanted))
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
			Expect(shard.DB).To(Equal(test.wanted))
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
			err := cluster.ForEachShard(func(shard *sharding.Shard) error {
				defer GinkgoRecover()
				mu.Lock()
				Expect(shards).NotTo(ContainElement(shard.String()))
				shards = append(shards, shard.String())
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(shards).To(HaveLen(4))
			for i := 0; i < 4; i++ {
				Expect(shards).To(ContainElement(fmt.Sprintf("shard%d", i)))
			}
		})

		It("returns an error if fn fails", func() {
			err := cluster.ForEachShard(func(shard *sharding.Shard) error {
				if shard.String() == "shard3" {
					return errors.New("fake error")
				}
				return nil
			})
			Expect(err).To(MatchError("fake error"))
		})
	})
})
