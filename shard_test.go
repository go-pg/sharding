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
		shard = sharding.NewShard(0, db, "SHARD_ID", "1234", "SHARD", "shard1234")
	})

	It("supports SHARD_ID", func() {
		var shardId int
		_, err := shard.QueryOne(pg.Scan(&shardId), `SELECT SHARD_ID`)
		Expect(err).NotTo(HaveOccurred())
		Expect(shardId).To(Equal(1234))
	})

	It("supports SHARD", func() {
		_, err := shard.Exec(`DROP SCHEMA IF EXISTS SHARD CASCADE`)
		Expect(err).NotTo(HaveOccurred())

		_, err = shard.Exec(`CREATE SCHEMA SHARD`)
		Expect(err).NotTo(HaveOccurred())

		_, err = shard.Exec(`DROP TABLE IF EXISTS SHARD.my_table`)
		Expect(err).NotTo(HaveOccurred())

		_, err = shard.Exec(`CREATE TABLE SHARD.my_table ()`)
		Expect(err).NotTo(HaveOccurred())

		var count int
		_, err = shard.QueryOne(pg.Scan(&count), `SELECT count(*) FROM shard1234.my_table`)
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(0))
	})
})
