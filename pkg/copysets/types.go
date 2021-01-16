// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package copysets

import (
	"fmt"
	"sort"
	"strings"

	"github.com/tikv/pd/server/core"
)

type NodeRole int

const (
	UnSet NodeRole = iota
	Any
	Extra
	Elementary
	Supplementary
)

func RoleToString(role NodeRole) string {
	switch role {
	case Any:
		return "Any"
	case Extra:
		return "Extra"
	case Elementary:
		return "Elementary"
	case Supplementary:
		return "Supplementary"
	}
	return ""
}

type Node struct {
	id   uint64
	role NodeRole
}

func (n *Node) GetID() uint64 {
	return n.id
}

func (n *Node) GetRole() NodeRole {
	return n.role
}

func (n *Node) setRole(role NodeRole) {
	n.role = role
}

type Group struct {
	nodes    map[uint64]struct{}
	complete bool
}

func (g *Group) IsNodeExist(nodeID uint64) bool {
	_, ok := g.nodes[nodeID]
	return ok
}

func (g *Group) GetNodes() []uint64 {
	ns := make([]uint64, 0, len(g.nodes))
	for id := range g.nodes {
		ns = append(ns, id)
	}
	return ns
}

func (g *Group) IsComplete() bool {
	return g.complete
}

func (g *Group) sign() string {
	var nodesID []int
	for nodeID := range g.nodes {
		nodesID = append(nodesID, int(nodeID))
	}
	sort.Ints(nodesID)
	var s []string
	for _, id := range nodesID {
		s = append(s, fmt.Sprintf("%v", id))
	}
	return strings.Join(s, "-")
}

func (g *Group) addNode(nodeID uint64) bool {
	if g.nodes == nil {
		g.nodes = make(map[uint64]struct{})
	}
	_, ok := g.nodes[nodeID]
	if ok {
		return false
	}
	g.nodes[nodeID] = struct{}{}
	return true
}

func (g *Group) deleteNode(nodeID uint64) {
	if g.nodes == nil {
		g.nodes = make(map[uint64]struct{})
		return
	}
	delete(g.nodes, nodeID)
}

func (g *Group) selectN(n int) []uint64 {
	s := make([]uint64, 0, len(g.nodes))
	for id := range g.nodes {
		s = append(s, id)
	}
	return selectN(n, s)
}

type CopySet struct {
	nodes [3]uint64
}

func NewCopySet(n1, n2, n3 uint64) CopySet {
	return CopySet{
		nodes: [3]uint64{
			n1, n2, n3,
		},
	}
}

func (cs CopySet) IsRegionSatisfied(region *core.RegionInfo) bool {
	m := make(map[uint64]struct{})
	for _, peer := range region.GetPeers() {
		m[peer.StoreId] = struct{}{}
	}
	cnt := 0
	for _, node := range cs.nodes {
		if _, ok := m[node]; ok {
			cnt++
		}
	}
	if cnt >= 3 {
		return true
	}
	return false
}

func (cs CopySet) CalCopysetDistScore(region *core.RegionInfo) int {
	originScore := 100
	for _, peer := range region.GetPeers() {
		if !cs.IsStoreInCopySet(peer.StoreId) {
			originScore = originScore - 10
		}
	}
	if !cs.IsStoreInCopySet(region.GetLeader().StoreId) {
		originScore = originScore - 5
	}
	return originScore
}

func (cs CopySet) IsStoreInCopySet(storeID uint64) bool {
	for _, node := range cs.nodes {
		if node == storeID {
			return true
		}
	}
	return false
}

func (cs CopySet) StoresCandidate(region *core.RegionInfo) []uint64 {
	storeCandidates := make([]uint64, 0, 0)
	for _, node := range cs.nodes {
		if !isAlreadyStorePeer(region, node) {
			storeCandidates = append(storeCandidates, node)
		}
	}
	return storeCandidates
}

func (cs CopySet) GetNodesID() (uint64, uint64, uint64) {
	return cs.nodes[0], cs.nodes[1], cs.nodes[2]
}

func (cs CopySet) Sign() string {
	tmp := make([]int, len(cs.nodes))
	for i, node := range cs.nodes {
		tmp[i] = int(node)
	}
	sort.Ints(tmp)
	var x []string
	for _, t := range tmp {
		x = append(x, fmt.Sprintf("%v", t))
	}
	return strings.Join(x, "-")
}

func isAlreadyStorePeer(region *core.RegionInfo, storeID uint64) bool {
	for _, peer := range region.GetPeers() {
		if peer.StoreId == storeID {
			return true
		}
	}
	return false
}
