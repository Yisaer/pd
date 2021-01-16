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

import "sync"

type CopysetsManager struct {
	R int
	//nodesID map[uint64]struct{}
	cm *CopySetManager
	//nm      *NodeManager
	mu struct {
		sync.RWMutex
		needChange bool
		cache      []CopySet
		cacheGroup map[string][]CopySet
		nodesID    map[uint64]struct{}
		nm         *NodeManager
	}
}

func NewCopysetsManager(R, S int, nodesID []uint64) *CopysetsManager {
	if S%(R-1) > 0 {
		return nil
	}
	if R != 3 {
		return nil
	}
	cm := NewCopySetManager(R, S)
	manager := &CopysetsManager{
		cm: cm,
	}
	manager.R = R
	manager.mu.nodesID = sliceToMap(nodesID)
	if len(nodesID) >= 15 {
		manager.mu.nm = NewNodeManager(R*cm.C, nodesID)
	}
	manager.mu.needChange = true
	manager.mu.cacheGroup = nil
	manager.mu.cache = nil
	return manager
}

func (m *CopysetsManager) AddNode(nodeID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.nodesID[nodeID] = struct{}{}
	if m.mu.nm != nil {
		m.mu.nm.AddNode(nodeID)
	}
	if len(m.mu.nodesID) >= 15 {
		m.mu.nm = NewNodeManager(m.R*m.cm.C, mapToSlice(m.mu.nodesID))
	}
	m.mu.needChange = true
}

func (m *CopysetsManager) DelNode(nodeID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.mu.nodesID, nodeID)
	if m.mu.nm != nil {
		m.DelNode(nodeID)
	}
	m.mu.needChange = true
}

func (m *CopysetsManager) GenerateCopySets() []CopySet {
	m.mu.RLock()
	var groups []*Group
	csGauge.WithLabelValues("copyset").Set(float64(len(m.mu.nodesID)))
	if m.mu.nm == nil || len(m.mu.nodesID) < 15 {
		return nil
	}
	if m.mu.cache != nil {
		return m.mu.cache
	}
	groups = m.mu.nm.GetGroups()
	m.mu.RUnlock()
	groupCopysets := m.cm.GenerateCopySets(groups)
	m.mu.cacheGroup = groupCopysets
	m.mu.cache = merge(groupCopysets)
	return m.mu.cache
}

func (m *CopysetsManager) GetCopysetsByGroup() map[string][]CopySet {
	m.mu.RLock()
	var groups []*Group
	if m.mu.nm == nil || len(m.mu.nodesID) < 15 {
		return nil
	}
	if m.mu.cacheGroup != nil {
		return m.mu.cacheGroup
	}
	groups = m.mu.nm.GetGroups()
	m.mu.RUnlock()
	// TODO: we should provide copysets by incremental group instead of whole groups
	m.mu.Lock()
	defer m.mu.Unlock()
	groupCopysets := m.cm.GenerateCopySets(groups)
	m.mu.cacheGroup = groupCopysets
	m.mu.cache = merge(groupCopysets)
	return groupCopysets
}

func merge(groupCopyset map[string][]CopySet) []CopySet {
	c := make([]CopySet, 0, 0)
	for _, css := range groupCopyset {
		c = append(c, css...)
	}
	return c
}

func mapToSlice(m map[uint64]struct{}) []uint64 {
	s := make([]uint64, 0, len(m))
	for id := range m {
		s = append(s, id)
	}
	return s
}

func sliceToMap(nodesID []uint64) map[uint64]struct{} {
	m := make(map[uint64]struct{})
	for _, node := range nodesID {
		m[node] = struct{}{}
	}
	return m
}
