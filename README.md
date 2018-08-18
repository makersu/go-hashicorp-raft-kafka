# go-raft-kafka
Using Hashicorp Raft that doesn’t depend on ZooKeeper for Kafka

# build
go build -o bin/go-raft-kafka cmd/*


# setup
./go-raft-kafka -id engine1 ~/engine1