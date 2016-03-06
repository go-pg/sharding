all:
	go test gopkg.in/go-pg/sharding.v4 -cpu=1,2,4
	go test gopkg.in/go-pg/sharding.v4 -short -race
