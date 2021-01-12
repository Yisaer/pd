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

import (
	"fmt"
	"math/rand"
	"sync"
)

type NodeManager struct {
	mu struct {
		sync.RWMutex
		nodes  []*Node
		groups []*Group
	}
	// The replication we want
	R int
	// The scatter width we want
	S int
	// rows of shuffle matrix
	L int
	// columns of shuffle matrix
	C int
	// expect numbers in each group
	NG int
	// Initialized indicates whether NodeManager would work
	workAble bool
}

func NewNodeManager(R, S int, nodesID []uint64) *NodeManager {
	if S%(R-1) > 0 {
		return &NodeManager{
			workAble: false,
		}
	}
	nodes := make([]*Node, 0, len(nodesID))
	for _, id := range nodesID {
		nodes = append(nodes, &Node{id: id})
	}
	manager := &NodeManager{
		mu: struct {
			sync.RWMutex
			nodes  []*Node
			groups []*Group
		}{
			nodes:  nodes,
			groups: make([]*Group, 0, 0),
		},
		R: R,
		S: S,
		L: S / (R - 1),
		C: func() int {
			x := S / (R - 1)
			if x%2 == 0 {
				return x + 1
			}
			return x + 2
		}(),
	}
	manager.NG = manager.L * manager.C
	manager.initializeGroups()
	manager.applyRole()
	return manager
}

func (m *NodeManager) AddNode(newID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.nodes = append(m.mu.nodes, &Node{id: newID})
	gi := -1
	for idx, group := range m.mu.groups {
		if !group.IsComplete() {
			gi = idx
			break
		}
	}

	if gi < 0 {
		// add a new incomplete group
		group := &Group{complete: false}
		nsNodes := make([]uint64, 0)
		for _, group := range m.mu.groups {
			if group.IsComplete() {
				nsNodes = append(nsNodes, group.GetNodes()...)
			}
		}
		nodes := append(selectN(m.NG-1, nsNodes), newID)
		for _, nodeID := range nodes {
			group.addNode(nodeID)
		}
		m.mu.groups = append(m.mu.groups, group)
	} else {
		if len(m.mu.groups) > 1 {
			// S -> E
			changeNodeID := uint64(0)
			for nodeID := range m.mu.groups[gi].nodes {
				if m.getNodeByID(nodeID).GetRole() == Supplementary {
					changeNodeID = nodeID
					break
				}
			}
			if changeNodeID < 1 {
				panic("AddNode didn't found supplementary Node in InComplete Group")
			}
			m.mu.groups[gi].deleteNode(changeNodeID)
		}
		m.mu.groups[gi].addNode(newID)
		remainSupplementary := 0
		for _, nodeID := range m.mu.groups[gi].GetNodes() {
			if m.getNodeByID(nodeID).role == Supplementary {
				remainSupplementary++
			}
		}
		if remainSupplementary < 1 && len(m.mu.groups[gi].nodes) == m.NG {
			m.mu.groups[gi].complete = true
		}
		m.mu.groups[gi].addNode(newID)
	}
	m.applyRole()
}

func (m *NodeManager) DeleteNode(nodeID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var deleteNode *Node
	for _, node := range m.mu.nodes {
		if node.id == nodeID {
			deleteNode = node
			break
		}
	}
	if deleteNode == nil {
		return nil
	}

	deleteNodeFunc := func() {
		newNodes := make([]*Node, 0, len(m.mu.nodes)-1)
		for _, node := range m.mu.nodes {
			if node.id != nodeID {
				newNodes = append(newNodes, node)
			}
		}
		m.mu.nodes = newNodes
		for id, group := range m.mu.groups {
			if group.IsNodeExist(nodeID) {
				m.mu.groups[id].deleteNode(nodeID)
			}
		}
	}

	addNodeFunc := func(addNodeID uint64) {
		for id, group := range m.mu.groups {
			if group.IsNodeExist(nodeID) {
				group.addNode(addNodeID)
				m.mu.groups[id] = group
			}
		}
	}

	switch deleteNode.GetRole() {
	case Extra:
		addNodeID := uint64(0)
		for _, group := range m.mu.groups {
			if group.IsComplete() {
				var elementaryNodes []uint64
				for nodeID := range group.nodes {
					if m.getNodeByID(nodeID).GetRole() == Elementary {
						elementaryNodes = append(elementaryNodes, nodeID)
					}
				}
				addNodeID = selectN(1, elementaryNodes)[0]
			}
		}
		if addNodeID < 1 {
			return fmt.Errorf("fail to find swap node for delete node %v", nodeID)
		}
		// swap node and delete node
		addNodeFunc(addNodeID)
		deleteNodeFunc()
	case Elementary:
		deleteNodeGroupID := -1
		for id, group := range m.mu.groups {
			if group.IsNodeExist(nodeID) {
				deleteNodeGroupID = id
			}
		}
		if deleteNodeGroupID < 0 {
			panic("didn't find delete Node GroupID")
		}
		// TODO:
		extraNodes := m.getNodesByRole(Extra)
		if len(extraNodes) > 0 {
			extraNodes := m.getNodesByRole(Extra)
			addNodeID := selectN(1, extraNodes)[0]
			addNodeFunc(addNodeID)
			deleteNodeFunc()
		} else {
			var otherCompleteGroupsID []int
			for id, group := range m.mu.groups {
				if group.IsComplete() && !group.IsNodeExist(nodeID) {
					otherCompleteGroupsID = append(otherCompleteGroupsID, id)
				}
			}
			if len(otherCompleteGroupsID) > 0 {
				selectGroupID := otherCompleteGroupsID[rand.Intn(len(otherCompleteGroupsID))]
				var otherElementaryNodesID []uint64
				for nodeID := range m.mu.groups[selectGroupID].nodes {
					otherElementaryNodesID = append(otherElementaryNodesID, nodeID)
				}
				addNodeID := selectN(1, otherElementaryNodesID)[0]
				addNodeFunc(addNodeID)
			}
			m.mu.groups[deleteNodeGroupID].complete = false
			deleteNodeFunc()
		}
	case Supplementary:
		// TODO:
		completeGroupID := -1
		incompleteGroupID := -1
		for id, group := range m.mu.groups {
			if completeGroupID == -1 && group.IsNodeExist(nodeID) && group.IsComplete() {
				completeGroupID = id
			} else if incompleteGroupID == -1 && group.IsNodeExist(nodeID) && !group.IsComplete() {
				incompleteGroupID = id
			}
		}
		if completeGroupID < 0 || incompleteGroupID < 0 {
			panic("can't find complete group or incomplete group for Supplementary node")
		}
		selectedElementaryNodeID := uint64(0)
		if len(m.mu.groups) < 3 {
			selectedElementaryNodeID = selectN(1, m.getNodesByRole(Elementary))[0]
		} else {
			for ; selectedElementaryNodeID == 0; {
				selectedElementaryNodeID = selectN(1, m.getNodesByRole(Elementary))[0]
				sg := m.getElementaryNodeGroupID(selectedElementaryNodeID)
				if sg == completeGroupID {
					selectedElementaryNodeID = 0
				}
			}
		}
		selectedExtraNodeID := selectN(1, m.getNodesByRole(Extra))[0]
		m.mu.groups[incompleteGroupID].addNode(selectedElementaryNodeID)
		m.mu.groups[completeGroupID].addNode(selectedExtraNodeID)
		deleteNodeFunc()
		m.removeDuplicatedGroups()
	default:
		return fmt.Errorf("unknown role")
	}
	m.applyRole()
	if len(m.mu.nodes) < m.L*m.C {
		m.workAble = false
	}
	return nil
}

func (m *NodeManager) GetNodesByRole(role NodeRole) []uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getNodesByRole(role)
}

func (m *NodeManager) GetGroups() []*Group {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.mu.groups
}

func (m *NodeManager) getNodeByID(id uint64) *Node {
	for _, node := range m.mu.nodes {
		if node.id == id {
			return node
		}
	}
	return nil
}

func (m *NodeManager) getNodesByRole(role NodeRole) []uint64 {
	nodes := make([]uint64, 0, len(m.mu.nodes))
	for _, node := range m.mu.nodes {
		if node.GetRole() == role || node.GetRole() == Any {
			nodes = append(nodes, node.id)
		}
	}
	return nodes
}

func (m *NodeManager) initializeGroups() {
	if len(m.mu.nodes) < m.NG {
		group := &Group{complete: false}
		for _, node := range m.mu.nodes {
			group.addNode(node.id)
		}
		m.mu.groups = append(m.mu.groups, group)
		m.workAble = false
		return
	}
	defer func() {
		m.workAble = true
	}()
	nodes := m.mu.nodes
	ng := m.NG
	j := 0
	for i := 0; i < len(nodes)/ng; i++ {
		group := &Group{complete: true}
		for ; j < (i+1)*ng; j++ {
			group.addNode(nodes[j].id)
		}
		m.mu.groups = append(m.mu.groups, group)
	}
	remained := len(m.mu.nodes) % m.NG
	if remained > 0 {
		gi := rand.Intn(len(m.mu.groups))
		group := &Group{complete: false}
		for ; j < len(nodes); j++ {
			group.addNode(nodes[j].id)
		}
		for _, id := range m.mu.groups[gi].selectN(m.NG - remained) {
			group.addNode(id)
		}
		m.mu.groups = append(m.mu.groups, group)
	}
	m.applyRole()
}

func (m *NodeManager) applyRole() {
	groups := m.mu.groups
	nodes := m.mu.nodes
	for i := 0; i < len(m.mu.nodes); i++ {
		id := nodes[i].id
		complete := 0
		incomplete := 0
		for _, group := range groups {
			if !group.IsNodeExist(id) {
				continue
			}
			if group.IsComplete() {
				complete++
			} else {
				incomplete++
			}
		}
		if incomplete > 1 {
			panic("incomplete number is greater than 1")
		}
		if complete > 0 && incomplete == 0 {
			nodes[i].role = Elementary
		} else if complete > 0 && incomplete > 0 {
			nodes[i].role = Supplementary
		} else {
			nodes[i].role = Extra
		}
	}
}

func (m *NodeManager) removeDuplicatedGroups() {
	newGroups := make([]*Group, 0, len(m.mu.groups))
	gm := make(map[string]struct{})
	for _, group := range m.mu.groups {
		sign := group.sign()
		if _, ok := gm[sign]; ok {
			continue
		}
		gm[sign] = struct{}{}
		newGroups = append(newGroups, group)
	}
	m.mu.groups = newGroups
}

func (m *NodeManager) debug() {
	groups := m.GetGroups()
	complete := 0
	incomplete := 0
	for _, node := range m.mu.nodes {
		fmt.Println("node=", node.id, " role=", RoleToString(node.role))
	}
	for _, group := range groups {
		fmt.Println(group.sign(), group.IsComplete())
		if group.IsComplete() {
			complete++
		} else {
			incomplete++
		}
	}
	fmt.Println("complete=", complete)
	fmt.Println("inComplete=", incomplete)
	fmt.Println("expectElementaryNodes=", len(m.GetNodesByRole(Elementary)))
	fmt.Println("expectSupplementaryNodes=", len(m.GetNodesByRole(Supplementary)))
	fmt.Println("expectExtraNodes=", len(m.GetNodesByRole(Extra)))
}

func (m *NodeManager) getElementaryNodeGroupID(nodeID uint64) int {
	node := m.getNodeByID(nodeID)
	if node.GetRole() != Elementary {
		panic("node is not Elementary")
	}
	for id, group := range m.mu.groups {
		if group.IsNodeExist(nodeID) {
			return id
		}
	}
	return -1
}
