package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/makersu/go-raft-kafka/config"
	"github.com/makersu/go-raft-kafka/httpd"

	"github.com/makersu/go-raft-kafka/store"
)

type ConsumerNode struct {
	// listener net.Listener
	httpService *httpd.NodeHttpHandler

	// wrapper and manager for db instance
	store *store.Store //?

	Logger *log.Logger
}

// TODO: error handling
// initialize an app
func NewConsumerNode(nodeConfig *config.NodeConfig) *ConsumerNode {

	consumerNode := &ConsumerNode{
		Logger: log.New(os.Stderr, "[server] ", log.LstdFlags),
	}

	var err error
	// consumerNode.store, err = store.NewStore(consumerNodeConfig.RaftDir, consumerNodeConfig.RaftBindAddr)
	consumerNode.store, err = store.NewStore(nodeConfig)
	if err != nil {
		consumerNode.Logger.Println(err.Error())
		panic(err.Error())
	}

	// consumerNode.httpService, err = httpd.NewHttpService(nodeConfig)
	httpHandler := httpd.NewNodeHttpHandler(consumerNode.store, nodeConfig.Listen)
	if err := httpHandler.Start(); err != nil {
		// log.Fatalf("failed to start HTTP service: %s", err.Error())
		consumerNode.Logger.Println(err.Error())
		panic(err.Error())
	}

	if nodeConfig.JoinAddr != "" {
		if err := join(nodeConfig.JoinAddr, nodeConfig.RaftAddr, nodeConfig.NodeID); err != nil {
			log.Fatalf("failed to join node at %s: %s", nodeConfig.JoinAddr, err.Error())
		}
	}

	return consumerNode
}

func join(joinAddr, raftAddr, nodeID string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr, "id": nodeID})
	if err != nil {
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
