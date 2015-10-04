package sharding_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/go-pg/sharding.v1"
	"gopkg.in/pg.v3"
)

var _ = Describe("Shard", func() {
	var shard *sharding.Shard

	BeforeEach(func() {
		db := pg.Connect(&pg.Options{
			User:     "postgres",
			Database: "test",
		})
		shard = sharding.NewShard(0, db, "SHARD_ID", "1234", "SHARD", "shard1234")
	})

	It("supports SHARD_ID", func() {
		var shardId int
		_, err := shard.QueryOne(pg.LoadInto(&shardId), `SELECT SHARD_ID`)
		Expect(err).NotTo(HaveOccurred())
		Expect(shardId).To(Equal(1234))
	})

	It("supports SHARD", func() {
		_, err := shard.Exec(`DROP SCHEMA IF EXISTS SHARD`)
		Expect(err).NotTo(HaveOccurred())

		_, err = shard.Exec(`CREATE SCHEMA SHARD`)
		Expect(err).NotTo(HaveOccurred())

		_, err = shard.Exec(`DROP TABLE IF EXISTS SHARD.my_table`)
		Expect(err).NotTo(HaveOccurred())

		_, err = shard.Exec(`CREATE TABLE SHARD.my_table ()`)
		Expect(err).NotTo(HaveOccurred())

		var count int
		_, err = shard.QueryOne(pg.LoadInto(&count), `SELECT count(*) FROM shard1234.my_table`)
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(0))
	})
})
