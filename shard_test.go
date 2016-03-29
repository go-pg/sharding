package sharding_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/go-pg/sharding.v4"
	"gopkg.in/pg.v4"
)

var _ = Describe("Shard", func() {
	var shard *sharding.Shard

	BeforeEach(func() {
		db := pg.Connect(&pg.Options{
			User: "postgres",
		})
		shard = sharding.NewShard(1234, db)
	})

	It("supports ?shard", func() {
		var shardName, hello string
		_, err := shard.QueryOne(pg.Scan(&shardName, &hello), `SELECT '?shard', ?`, "hello")
		Expect(err).NotTo(HaveOccurred())
		Expect(shardName).To(Equal(`"shard1234"`))
		Expect(hello).To(Equal("hello"))
	})

	It("supports ?shard_id", func() {
		var shardId int
		var hello string
		_, err := shard.QueryOne(pg.Scan(&shardId, &hello), `SELECT ?shard_id, ?`, "hello")
		Expect(err).NotTo(HaveOccurred())
		Expect(shardId).To(Equal(1234))
		Expect(hello).To(Equal("hello"))
	})
})
