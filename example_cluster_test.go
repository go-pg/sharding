package sharding_test

import (
	"fmt"

	"gopkg.in/go-pg/sharding.v5"
	"gopkg.in/pg.v5"
)

// Users are sharded by AccountId, i.e. users with same account id are
// placed on same shard.
type User struct {
	TableName string `sql:"?shard.users"`

	Id        int64
	AccountId int64
	Name      string
	Emails    []string
}

func (u User) String() string {
	return u.Name
}

// CreateUser picks shard by account id and creates user in the shard.
func CreateUser(cluster *sharding.Cluster, user *User) error {
	return cluster.Shard(user.AccountId).Insert(user)
}

// GetUser splits shard from user id and fetches user from the shard.
func GetUser(cluster *sharding.Cluster, id int64) (*User, error) {
	var user User
	err := cluster.SplitShard(id).Model(&user).Where("id = ?", id).Select()
	return &user, err
}

// GetUsers picks shard by account id and fetches users from the shard.
func GetUsers(cluster *sharding.Cluster, accountId int64) ([]User, error) {
	var users []User
	err := cluster.Shard(accountId).Model(&users).Where("account_id = ?", accountId).Select()
	return users, err
}

// createShard creates database schema for a given shard.
func createShard(shard *sharding.Shard) error {
	queries := []string{
		`DROP SCHEMA IF EXISTS ?shard CASCADE`,
		`CREATE SCHEMA ?shard`,
		sqlFuncs,
		`CREATE TABLE ?shard.users (id bigint DEFAULT ?shard.next_id(), account_id int, name text, emails jsonb)`,
	}

	for _, q := range queries {
		_, err := shard.Exec(q)
		if err != nil {
			return err
		}
	}

	return nil
}

func ExampleCluster() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})

	dbs := []*pg.DB{db} // list of physical PostgreSQL servers
	nshards := 2        // 2 logical shards
	// Create cluster with 1 physical server and 2 logical shards.
	cluster := sharding.NewCluster(dbs, nshards)

	// Create database schema for our logical shards.
	for i := 0; i < nshards; i++ {
		if err := createShard(cluster.Shard(int64(i))); err != nil {
			panic(err)
		}
	}

	// user1 will be created in shard1 because AccountId % nshards = shard1.
	user1 := &User{
		Name:      "user1",
		AccountId: 1,
		Emails:    []string{"user1@domain"},
	}
	err := CreateUser(cluster, user1)
	if err != nil {
		panic(err)
	}

	// user2 will be created in shard1 too AccountId is the same.
	user2 := &User{
		Name:      "user2",
		AccountId: 1,
		Emails:    []string{"user2@domain"},
	}
	err = CreateUser(cluster, user2)
	if err != nil {
		panic(err)
	}

	// user3 will be created in shard0 because AccountId % nshards = shard0.
	user3 := &User{
		Name:      "user3",
		AccountId: 2,
		Emails:    []string{"user3@domain"},
	}
	err = CreateUser(cluster, user3)
	if err != nil {
		panic(err)
	}

	user, err := GetUser(cluster, user1.Id)
	if err != nil {
		panic(err)
	}

	users, err := GetUsers(cluster, 1)
	if err != nil {
		panic(err)
	}

	fmt.Println(user)
	fmt.Println(users[0], users[1])
	// Output: user1
	// user1 user2
}

const sqlFuncs = `
CREATE SEQUENCE ?shard.id_seq;

-- _next_id returns unique sortable id.
CREATE FUNCTION ?shard._next_id(tm timestamptz, shard_id int, seq_id bigint)
RETURNS bigint AS $$
DECLARE
  our_epoch CONSTANT bigint := 1262304000000;
  max_shard_id CONSTANT bigint := 2048;
  max_seq_id CONSTANT bigint := 4096;
  id bigint;
BEGIN
  shard_id := shard_id % max_shard_id;
  seq_id := seq_id % max_seq_id;
  id := (floor(extract(epoch FROM tm) * 1000)::bigint - our_epoch) << 23;
  id := id | (shard_id << 12);
  id := id | seq_id;
  RETURN id;
END;
$$
LANGUAGE plpgsql
IMMUTABLE;

CREATE FUNCTION ?shard.next_id()
RETURNS bigint AS $$
BEGIN
   RETURN ?shard._next_id(clock_timestamp(), ?shard_id, nextval('?shard.id_seq'));
END;
$$
LANGUAGE plpgsql;
`
