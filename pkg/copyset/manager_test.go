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
	"testing"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testManagerSuite{})

type testManagerSuite struct{}

func (s *testManagerSuite) TestNewManager(c *C) {
	testcases := []struct {
		name                     string
		n                        int
		r                        int
		s                        int
		expectGroups             int
		expectCompleteGroups     int
		expectInCompleteGroups   int
		expectElementaryNodes    int
		expectSupplementaryNodes int
		expectExtraNodes         int
	}{
		{
			name:                     "15 nodes",
			n:                        15,
			r:                        3,
			s:                        6,
			expectGroups:             1,
			expectCompleteGroups:     1,
			expectInCompleteGroups:   0,
			expectElementaryNodes:    15,
			expectSupplementaryNodes: 0,
			expectExtraNodes:         0,
		},
		{
			name:                     "16 nodes",
			n:                        16,
			r:                        3,
			s:                        6,
			expectGroups:             2,
			expectCompleteGroups:     1,
			expectInCompleteGroups:   1,
			expectElementaryNodes:    1,
			expectSupplementaryNodes: 14,
			expectExtraNodes:         1,
		},
		{
			name:                     "45 nodes",
			n:                        45,
			r:                        3,
			s:                        6,
			expectGroups:             3,
			expectCompleteGroups:     3,
			expectInCompleteGroups:   0,
			expectElementaryNodes:    45,
			expectSupplementaryNodes: 0,
			expectExtraNodes:         0,
		},
		{
			name:                     "20 nodes",
			n:                        20,
			r:                        3,
			s:                        6,
			expectGroups:             2,
			expectCompleteGroups:     1,
			expectInCompleteGroups:   1,
			expectElementaryNodes:    5,
			expectSupplementaryNodes: 10,
			expectExtraNodes:         5,
		},
	}

	for _, testcase := range testcases {
		name := testcase.name
		c.Log(name)
		var nodes []uint64
		for i := 1; i <= testcase.n; i++ {
			nodes = append(nodes, uint64(i))
		}
		m := NewNodeManager(testcase.r, testcase.s, nodes)
		groups := m.GetGroups()
		c.Assert(testcase.expectGroups, Equals, len(groups))
		complete := 0
		incomplete := 0
		for _, group := range groups {
			if group.IsComplete() {
				complete++
			} else {
				incomplete++
			}
		}
		c.Assert(testcase.expectCompleteGroups, Equals, complete)
		c.Assert(testcase.expectInCompleteGroups, Equals, incomplete)
		c.Assert(testcase.expectElementaryNodes, Equals, len(m.GetNodesByRole(Elementary)))
		c.Assert(testcase.expectSupplementaryNodes, Equals, len(m.GetNodesByRole(Supplementary)))
		c.Assert(testcase.expectExtraNodes, Equals, len(m.GetNodesByRole(Extra)))
	}
}

func (s *testManagerSuite) TestAddNode(c *C) {
	testcases := []struct {
		name                     string
		n                        int
		r                        int
		s                        int
		expectGroups             int
		expectCompleteGroups     int
		expectInCompleteGroups   int
		expectElementaryNodes    int
		expectSupplementaryNodes int
		expectExtraNodes         int
	}{
		{
			name:                     "15 -> 16 nodes",
			n:                        15,
			r:                        3,
			s:                        6,
			expectGroups:             2,
			expectCompleteGroups:     1,
			expectInCompleteGroups:   1,
			expectElementaryNodes:    1,
			expectSupplementaryNodes: 14,
			expectExtraNodes:         1,
		},
		{
			name:                     "29 -> 30 nodes",
			n:                        29,
			r:                        3,
			s:                        6,
			expectGroups:             2,
			expectCompleteGroups:     2,
			expectInCompleteGroups:   0,
			expectElementaryNodes:    30,
			expectSupplementaryNodes: 0,
			expectExtraNodes:         0,
		},
		{
			name:                     "16 -> 17 nodes",
			n:                        16,
			r:                        3,
			s:                        6,
			expectGroups:             2,
			expectCompleteGroups:     1,
			expectInCompleteGroups:   1,
			expectElementaryNodes:    2,
			expectSupplementaryNodes: 13,
			expectExtraNodes:         2,
		},
		{
			name:                     "14 -> 15 nodes",
			n:                        14,
			r:                        3,
			s:                        6,
			expectGroups:             1,
			expectCompleteGroups:     1,
			expectInCompleteGroups:   0,
			expectElementaryNodes:    15,
			expectSupplementaryNodes: 0,
			expectExtraNodes:         0,
		},
		{
			name:                     "13 -> 14 nodes",
			n:                        13,
			r:                        3,
			s:                        6,
			expectGroups:             1,
			expectCompleteGroups:     0,
			expectInCompleteGroups:   1,
			expectElementaryNodes:    0,
			expectSupplementaryNodes: 0,
			expectExtraNodes:         14,
		},
	}

	for _, testcase := range testcases {
		c.Log(testcase.name)
		var nodes []uint64
		for i := 1; i <= testcase.n; i++ {
			nodes = append(nodes, uint64(i))
		}
		m := NewNodeManager(testcase.r, testcase.s, nodes)
		m.AddNode(uint64(testcase.n + 1))
		c.Assert(testcase.expectGroups, Equals, len(m.GetGroups()))
		groups := m.GetGroups()
		complete := 0
		incomplete := 0
		for _, group := range groups {
			if group.IsComplete() {
				complete++
			} else {
				incomplete++
			}
		}
		c.Assert(testcase.expectCompleteGroups, Equals, complete)
		c.Assert(testcase.expectInCompleteGroups, Equals, incomplete)
		c.Assert(testcase.expectElementaryNodes, Equals, len(m.GetNodesByRole(Elementary)))
		c.Assert(testcase.expectSupplementaryNodes, Equals, len(m.GetNodesByRole(Supplementary)))
		c.Assert(testcase.expectExtraNodes, Equals, len(m.GetNodesByRole(Extra)))
	}
}

func (s *testManagerSuite) TestDelNode(c *C) {
	testcases := []struct {
		name                     string
		n                        int
		r                        int
		s                        int
		expectGroups             int
		expectCompleteGroups     int
		expectInCompleteGroups   int
		expectElementaryNodes    int
		expectSupplementaryNodes int
		expectExtraNodes         int
		delNodeRole              NodeRole
	}{
		{
			name:                     "16 -> 15, delete extra node",
			n:                        16,
			r:                        3,
			s:                        6,
			delNodeRole:              Extra,
			expectGroups:             2,
			expectCompleteGroups:     1,
			expectInCompleteGroups:   1,
			expectElementaryNodes:    0,
			expectSupplementaryNodes: 15,
			expectExtraNodes:         0,
		},
		{
			name:                     "16 -> 15, delete elementary node",
			n:                        16,
			r:                        3,
			s:                        6,
			delNodeRole:              Elementary,
			expectGroups:             2,
			expectCompleteGroups:     1,
			expectInCompleteGroups:   1,
			expectElementaryNodes:    0,
			expectSupplementaryNodes: 15,
			expectExtraNodes:         0,
		},
		{
			name:                     "15 -> 14, delete elementary node",
			n:                        15,
			r:                        3,
			s:                        6,
			delNodeRole:              Elementary,
			expectGroups:             1,
			expectCompleteGroups:     0,
			expectInCompleteGroups:   1,
			expectElementaryNodes:    0,
			expectSupplementaryNodes: 0,
			expectExtraNodes:         14,
		},
		{
			name:                     "30 -> 29, delete elementary node",
			n:                        30,
			r:                        3,
			s:                        6,
			delNodeRole:              Elementary,
			expectGroups:             2,
			expectCompleteGroups:     1,
			expectInCompleteGroups:   1,
			expectElementaryNodes:    14,
			expectSupplementaryNodes: 1,
			expectExtraNodes:         14,
		},
		{
			name:                     "16 -> 15, delete supplementary node",
			n:                        16,
			r:                        3,
			s:                        6,
			delNodeRole:              Supplementary,
			expectGroups:             1,
			expectCompleteGroups:     1,
			expectInCompleteGroups:   0,
			expectElementaryNodes:    15,
			expectSupplementaryNodes: 0,
			expectExtraNodes:         0,
		},
		{
			name:                     "17 -> 16, delete supplementary node",
			n:                        17,
			r:                        3,
			s:                        6,
			delNodeRole:              Supplementary,
			expectGroups:             2,
			expectCompleteGroups:     1,
			expectInCompleteGroups:   1,
			expectElementaryNodes:    1,
			expectSupplementaryNodes: 14,
			expectExtraNodes:         1,
		},
		{
			name:                     "31 -> 30, delete supplementary node",
			n:                        31,
			r:                        3,
			s:                        6,
			delNodeRole:              Supplementary,
			expectGroups:             3,
			expectCompleteGroups:     2,
			expectInCompleteGroups:   1,
			expectElementaryNodes:    15,
			expectSupplementaryNodes: 15,
			expectExtraNodes:         0,
		},
	}

	for _, testcase := range testcases {
		c.Log(testcase.name)
		var nodes []uint64
		for i := 1; i <= testcase.n; i++ {
			nodes = append(nodes, uint64(i))
		}
		m := NewNodeManager(testcase.r, testcase.s, nodes)
		deleteNodeID := m.getNodesByRole(testcase.delNodeRole)[0]
		m.debug()
		err := m.DeleteNode(deleteNodeID)
		m.debug()
		c.Assert(err, IsNil)
		c.Assert(testcase.expectGroups, Equals, len(m.GetGroups()))
		groups := m.GetGroups()
		complete := 0
		incomplete := 0
		for _, group := range groups {
			if group.IsComplete() {
				complete++
			} else {
				incomplete++
			}
		}
		c.Assert(testcase.expectCompleteGroups, Equals, complete)
		c.Assert(testcase.expectInCompleteGroups, Equals, incomplete)
		c.Assert(testcase.expectElementaryNodes, Equals, len(m.GetNodesByRole(Elementary)))
		c.Assert(testcase.expectSupplementaryNodes, Equals, len(m.GetNodesByRole(Supplementary)))
		c.Assert(testcase.expectExtraNodes, Equals, len(m.GetNodesByRole(Extra)))
	}
}
