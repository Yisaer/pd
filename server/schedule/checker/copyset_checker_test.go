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

package checker

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/copysets"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
)

var _ = Suite(&testCopySetCheckerSuite{})

type testCopySetCheckerSuite struct {
	cluster    *mockcluster.Cluster
	csc        *CopySetChecker
	storeCount int
}

func (s *testCopySetCheckerSuite) SetUpTest(c *C) {
	s.cluster = mockcluster.NewCluster(config.NewTestOptions())
	s.csc = NewCopySetChecker(s.cluster)
	s.storeCount = 30
	for id := uint64(1); id <= uint64(s.storeCount); id++ {
		s.cluster.PutStoreWithLabelsInCopySets(id)
	}
}

func (s *testCopySetCheckerSuite) TestCopySetCheck(c *C) {
	newRegion := func(peers []*metapb.Peer) *core.RegionInfo {
		return core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[0])
	}
	css := s.cluster.GetCopySets()
	for i := 0; i <= 100; i++ {
		wholeStores := make([]uint64, 0, 0)
		for i := 1; i <= s.storeCount; i++ {
			wholeStores = append(wholeStores, uint64(i))
		}
		stores := copysets.SelectN(3, wholeStores)
		peers := make([]*metapb.Peer, 0, 0)
		i := 0
		for _, store := range stores {
			i++
			peers = append(peers, &metapb.Peer{
				Id:      uint64(i),
				StoreId: store,
				Role:    metapb.PeerRole_Voter,
			})
		}
		region := newRegion(peers)
		valid := false
		for _, cs := range css {
			if cs.IsRegionSatisfied(region) {
				valid = true
				break
			}
		}
		if valid {
			op := s.csc.Check(region)
			c.Assert(op, IsNil)
		} else {
			op := s.csc.Check(region)
			c.Assert(op, NotNil)
		}
	}
}
