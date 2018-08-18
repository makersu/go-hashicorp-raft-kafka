package config

type NodeConfig struct {
	NodeID   string
	Listen   string //?
	RaftAddr string
	RaftDir  string
	// Join     string //?
	JoinAddr string //?
	Desc     string //?
}

func NewNodeConfig(nodeId, listen, raftAddr, raftDir, joinAddr string) *NodeConfig {
	return &NodeConfig{
		NodeID:   nodeId,
		Listen:   listen, //?
		RaftAddr: raftAddr,
		RaftDir:  raftDir,
		JoinAddr: joinAddr, //?
		Desc:     "",       //?
	}
}
