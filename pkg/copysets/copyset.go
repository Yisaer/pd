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

type CopySetManager struct {
	mu struct {
		sync.RWMutex
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
}

func NewCopySetManager(R, S int) *CopySetManager {
	if S%(R-1) > 0 {
		return nil
	}
	if R != 3 {
		return nil
	}
	cm := &CopySetManager{
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
	return cm
}

func (cm *CopySetManager) SetGroups(groups []*Group) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.mu.groups = groups
}

func (cm *CopySetManager) GenerateCopySets() []CopySet {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	cs := make([]CopySet, 0, 0)
	for _, group := range cm.mu.groups {
		cs = append(cs, cm.generateCopySetForGroup(group)...)
	}
	return cs
}

func (cm *CopySetManager) generateCopySetForGroup(group *Group) []CopySet {
	cs := make([]CopySet, 0, 0)
	nodes := group.GetNodes()
	arrayA := nodes[0 : len(nodes)/3]
	arrayB := nodes[len(nodes)/3 : 2*len(nodes)/3]
	arrayC := nodes[2*len(nodes)/3:]
	if len(arrayA) != len(arrayB) || len(arrayB) != len(arrayC) {
		panic("couldn't generate initial array due to length")
	}
	smA := cm.generateShuffleMatrixOrder1(arrayA)
	smB := cm.generateShuffleMatrixOrder2(arrayB)
	smC := cm.generateShuffleMatrixOrder3(arrayC)
	for i := 0; i < cm.L; i++ {
		for j := 0; j < cm.C; j++ {
			cs = append(cs, NewCopySet(smA[i][j], smB[i][j], smC[i][j]))
		}
	}
	return cs
}

func (cm *CopySetManager) generateShuffleMatrixOrder1(rows []uint64) [][]uint64 {
	var sm [][]uint64
	for i := 0; i < cm.L; i++ {
		rows := make([]uint64, len(rows), len(rows))
		sm = append(sm, rows)
	}

	for i := 0; i < len(rows); i++ {
		for k := 0; k < cm.L; k++ {
			sm[k][i] = rows[i]
		}
	}
	return sm
}

func (cm *CopySetManager) generateShuffleMatrixOrder2(rows []uint64) [][]uint64 {
	var sm [][]uint64
	for i := 0; i < cm.L; i++ {
		rows := make([]uint64, len(rows), len(rows))
		sm = append(sm, rows)
	}
	for k := 0; k < cm.L; k++ {
		for i := 0; i < len(rows); i++ {
			x := (i + k*cm.C) % cm.L
			y := (i + k*cm.C) / cm.L
			sm[x][y] = rows[i]
		}
	}
	return sm
}

func (cm *CopySetManager) generateShuffleMatrixOrder3(rows []uint64) [][]uint64 {
	var sm [][]uint64
	for i := 0; i < cm.L; i++ {
		rows := make([]uint64, len(rows), len(rows))
		sm = append(sm, rows)
	}
	for k := 0; k < cm.L; k++ {
		for i := 0; i < len(rows); i++ {
			x := (i + k*cm.C) % cm.L
			y := cm.C - 1 - (i+k*cm.C)/cm.L
			sm[x][y] = rows[i]
		}
	}
	return sm
}
