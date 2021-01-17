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
	"sync"
)

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
	csGauge.WithLabelValues("add_node").Set(float64(len(m.mu.nodesID)))
	m.mu.nodesID[nodeID] = struct{}{}
	if m.mu.nm != nil {
		m.mu.nm.AddNode(nodeID)
	}
	m.mu.needChange = true
}

func (m *CopysetsManager) DelNode(nodeID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	csGauge.WithLabelValues("del_node").Set(float64(len(m.mu.nodesID)))
	delete(m.mu.nodesID, nodeID)
	if m.mu.nm != nil {
		m.DelNode(nodeID)
	}
	m.mu.needChange = true
}

func (m *CopysetsManager) GenerateCopySets(nowID []uint64) []CopySet {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(nowID) < 15 {
		return nil
	}
	reset := false
	var groups []*Group
	if m.mu.nm == nil || len(m.mu.nodesID) < 15 {
		if len(nowID) >= 15 {
			reset = true
		} else {
			return nil
		}
	} else if m.mu.needChange {
		groups = m.mu.nm.GetGroups()
		if len(groups) < 1 {
			return nil
		}
		groupCopysets := m.cm.GenerateCopySets(groups)
		m.mu.cacheGroup = groupCopysets
		m.mu.cache = merge(groupCopysets)
		m.mu.needChange = false
		csGauge.WithLabelValues("").Set(float64(len(m.mu.cache)))
		return m.mu.cache
	} else if len(m.mu.cache) > 0 {
		x := m.mu.cache
		for _, cs := range m.mu.cache {
			copySetGauge.WithLabelValues(cs.Sign()).Set(1)
		}
		csGauge.WithLabelValues("").Set(float64(len(m.mu.cache)))
		return x
	}
	if reset || len(m.mu.cache) < 1 {
		m.mu.nodesID = sliceToMap(nowID)
		m.mu.nm = NewNodeManager(m.R*m.cm.C, nowID)
		groups = m.mu.nm.GetGroups()
		if len(groups) < 1 {
			return nil
		}
		groupCopysets := m.cm.GenerateCopySets(groups)
		m.mu.cacheGroup = groupCopysets
		m.mu.cache = merge(groupCopysets)
		return m.mu.cache
	}
	return nil
}

func (m *CopysetsManager) GetCopysetsByGroup(nowID []uint64) map[string][]CopySet {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(nowID) < 15 {
		return nil
	}
	reset := false
	var groups []*Group
	csGauge.WithLabelValues("by_group").Set(float64(len(m.mu.nodesID)))
	if m.mu.nm == nil || len(m.mu.nodesID) < 15 {
		if len(nowID) >= 15 {
			reset = true
		} else {
			return nil
		}
	} else if m.mu.needChange {
		groups = m.mu.nm.GetGroups()
		if len(groups) < 1 {
			return nil
		}
		groupCopysets := m.cm.GenerateCopySets(groups)
		m.mu.cacheGroup = groupCopysets
		m.mu.cache = merge(groupCopysets)
		m.mu.needChange = false
		return m.mu.cacheGroup
	} else if len(m.mu.cache) > 0 {
		return m.mu.cacheGroup
	}
	if reset || len(m.mu.cache) < 1 {
		m.mu.nodesID = sliceToMap(nowID)
		m.mu.nm = NewNodeManager(m.R*m.cm.C, nowID)
		groups = m.mu.nm.GetGroups()
		if len(groups) < 1 {
			return nil
		}
		groupCopysets := m.cm.GenerateCopySets(groups)
		m.mu.cacheGroup = groupCopysets
		m.mu.cache = merge(groupCopysets)
		return m.mu.cacheGroup
	}
	return nil
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
