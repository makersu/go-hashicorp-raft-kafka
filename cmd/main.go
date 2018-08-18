package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/makersu/go-raft-kafka/config"
	"github.com/makersu/go-raft-kafka/server"
)

const (
	DefaultHTTPAddr = ":7777"
	DefaultRaftAddr = ":8888"
	DefaultRaftDir  = "./"
)

var (
	// inmem    bool
	httpAddr string
	raftAddr string
	raftDir  string
	joinAddr string
	nodeID   string
)

// ./redis-failover -http_addr=127.0.0.1:8080 -masters=127.0.0.1:6379 -raft_addr=127.0.0.1:12000 -raft_data_dir=./var0 -raft_cluster=127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002 -broker=raft
// ./hraftd -id node1 -haddr :10001 -raddr :20001 -join :10000 ~/node1
// ./bin/leto -id id2 -raftdir ./id2 -listen ":6379" -raftbind ":16379" -join "127.0.0.1:5379"
// ./bin/go-raft-kafka -id node1 -raftDir ./node1

func init() {
	flag.StringVar(&nodeID, "id", "", "Node ID")
	flag.StringVar(&httpAddr, "httpAddr", DefaultHTTPAddr, "Set the HTTP bind address") //?
	flag.StringVar(&raftAddr, "raftAddr", DefaultRaftAddr, "Set Raft bind address")
	flag.StringVar(&raftDir, "raftDir", DefaultRaftDir, "Set Raft data directory")
	flag.StringVar(&joinAddr, "joinAddr", "", "Set join address, if any") //?
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

// go build -o bin/go-raft-kafka cmd/*
// bin/go-raft-kafka -id matching1 -raftDir ./matching1
// bin/go-raft-kafka -id matching1 -raftDir ./matching1 -raftAddr :8888  -httpAddr :7777
// bin/go-raft-kafka -id matching2 -raftDir ./matching2 -raftAddr :8889  -httpAddr :7778 -joinAddr :7777
// bin/go-raft-kafka -id matching3 -raftDir ./matching3 -raftAddr :8890  -httpAddr :7780 -joinAddr :7777

func main() {
	flag.Parse()

	nodeConfig := config.NewNodeConfig(nodeID, httpAddr, raftAddr, raftDir, joinAddr)
	// fmt.Println("nodeConfig", nodeConfig)

	//TODO: error handling
	node := server.NewConsumerNode(nodeConfig)
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "Error configuring node: %s", err)
	// 	os.Exit(1)
	// }

	// TODO: refactoring to extract
	// httpService := server.NewHttpService(node, nodeConfig)
	// httpService.Start()

	node.Logger.Println("Node Id \"", nodeID, "\" started successfully")

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-terminate
	node.Logger.Println("Node Id \"", nodeID, "\" exiting")
}
