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

package copyset

type NodeRole int

const (
	UnSet NodeRole = iota
	Any
	Extra
	Elementary
	Supplementary
)

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
