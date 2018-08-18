package httpd

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/makersu/go-raft-kafka/store"
)

// type HttpServer struct {
// 	address net.Addr
// 	node    *ConsumerNode
// 	Logger  *log.Logger
// }

// // func NewHttpServer(nodeConfig *config.NodeConfig) *HttpServer {
// func NewHttpServer(node *ConsumerNode, nodeConfig *config.NodeConfig) *HttpServer {
// 	httpServer := &HttpServer{
// 		node:    node,
// 		address: nodeConfig.Listen,
// 		Logger:  log.New(os.Stderr, "[http] ", log.LstdFlags),
// 	}
// }

// Service provides HTTP service.
type NodeHttpHandler struct {
	addr string
	ln   net.Listener

	// node *ConsumerNode
	store  *store.Store
	logger *log.Logger
}

// New returns an uninitialized HTTP service.
func NewNodeHttpHandler(store *store.Store, addr string) *NodeHttpHandler {
	return &NodeHttpHandler{
		logger: log.New(os.Stderr, "[http] ", log.LstdFlags),
		addr:   addr,
		store:  store,
	}
}

// Start starts the service.
func (handler *NodeHttpHandler) Start() error {
	server := http.Server{
		Handler: handler,
	}

	ln, err := net.Listen("tcp", handler.addr)
	if err != nil {
		return err
	}
	handler.ln = ln

	http.Handle("/", handler)

	go func() {
		err := server.Serve(handler.ln)
		if err != nil {
			log.Fatalf("HTTP serve: %s", err)
		}
	}()

	return nil
}

// ServeHTTP allows Service to serve HTTP requests.
func (handler *NodeHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/key") {
		// s.handleKeyRequest(w, r)
	} else if r.URL.Path == "/join" {
		handler.handleJoin(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (handler *NodeHttpHandler) handleJoin(w http.ResponseWriter, r *http.Request) {
	m := map[string]string{}
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(m) != 2 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteAddr, ok := m["addr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	nodeID, ok := m["id"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := handler.store.Join(nodeID, remoteAddr); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
