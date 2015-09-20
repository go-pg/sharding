all:
	go test gopkg.in/go-pg/sharding.v1 -cpu=1,2,4
	go test gopkg.in/go-pg/sharding.v1 -short -race
