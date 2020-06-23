package scylla_cdc

import (
	"net"
	"sort"
	"strconv"
	"sync"

	"github.com/gocql/gocql"
)

// The gocql API does not expose (directly) information about current token ring of the cluster.
// This is a hack that plugs in into HostSelectionStrategy and keeps track of cluster tokens.

type ClusterStateTracker struct {
	gocql.HostSelectionPolicy

	mut   *sync.Mutex
	infos []*gocql.HostInfo
}

func NewClusterStateTracker(policy gocql.HostSelectionPolicy) *ClusterStateTracker {
	return &ClusterStateTracker{
		HostSelectionPolicy: policy,
		mut:                 &sync.Mutex{},
	}
}

func (cst *ClusterStateTracker) AddHost(host *gocql.HostInfo) {
	cst.HostSelectionPolicy.AddHost(host)

	cst.mut.Lock()
	defer cst.mut.Unlock()

	_, present := cst.getHostIndex(host.ConnectAddress())
	if !present {
		cst.infos = append(cst.infos, host)
	}
}

func (cst *ClusterStateTracker) RemoveHost(host *gocql.HostInfo) {
	cst.HostSelectionPolicy.RemoveHost(host)

	cst.mut.Lock()
	defer cst.mut.Unlock()

	idx, present := cst.getHostIndex(host.ConnectAddress())
	if present {
		// Order is irrelevant, so we don't need to preserve it here
		cst.infos[idx] = cst.infos[len(cst.infos)-1]
		cst.infos[len(cst.infos)-1] = nil
		cst.infos = cst.infos[:len(cst.infos)-1]
	}
}

func (cst *ClusterStateTracker) GetClusterSize() int {
	return len(cst.infos)
}

func (cst *ClusterStateTracker) GetTokens() TokenRing {
	cst.mut.Lock()
	defer cst.mut.Unlock()

	tokens := make(TokenRing, 0)

	for _, host := range cst.infos {
		for _, strTok := range host.Tokens() {
			tok, err := strconv.ParseInt(strTok, 10, 64)
			if err != nil {
				panic(err)
			}
			tokens = append(tokens, tok)
		}
	}

	sort.Sort(tokens)
	return tokens
}

func (cst *ClusterStateTracker) getHostIndex(ip net.IP) (int, bool) {
	for i, host := range cst.infos {
		if host.ConnectAddress().Equal(ip) {
			return i, true
		}
	}
	return 0, false
}

type TokenRing []int64

func (tr TokenRing) Len() int {
	return len(tr)
}

func (tr TokenRing) Less(i, j int) bool {
	return tr[i] < tr[j]
}

func (tr TokenRing) Swap(i, j int) {
	tr[i], tr[j] = tr[j], tr[i]
}
