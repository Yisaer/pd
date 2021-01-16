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

	. "github.com/pingcap/check"
)

func (s *testManagerSuite) TestOrder(c *C) {
	row := []uint64{
		1, 2, 3, 4, 5,
	}
	cm := NewCopySetManager(3, 6)
	x := cm.generateShuffleMatrixOrder1(row)
	c.Assert(x, DeepEquals, [][]uint64{
		{1, 2, 3, 4, 5},
		{1, 2, 3, 4, 5},
		{1, 2, 3, 4, 5},
	})
	x = cm.generateShuffleMatrixOrder2(row)
	c.Assert(x, DeepEquals, [][]uint64{
		{1, 4, 2, 5, 3},
		{2, 5, 3, 1, 4},
		{3, 1, 4, 2, 5},
	})
	x = cm.generateShuffleMatrixOrder3(row)
	c.Assert(x, DeepEquals, [][]uint64{
		{3, 5, 2, 4, 1},
		{4, 1, 3, 5, 2},
		{5, 2, 4, 1, 3},
	})
}

func (s *testManagerSuite) TestQQQQCopysetsManagerXXX(c *C) {
	nodesID := make([]uint64, 0, 0)
	for i := 1; i <= 15; i++ {
		nodesID = append(nodesID, uint64(i))
	}
	cm := NewCopysetsManager(3, 6, nodesID)
	css := cm.GenerateCopySets()
	x := make(map[uint64]struct{})
	for _, cs := range css {
		if cs.IsStoreInCopySet(uint64(1)) {
			n1, n2, n3 := cs.GetNodesID()
			x[n1] = struct{}{}
			x[n2] = struct{}{}
			x[n3] = struct{}{}
			fmt.Println(cs.Sign())
		}
	}
	for k := range x {
		if k != 1 {
			fmt.Println(k)
		}
	}
}
