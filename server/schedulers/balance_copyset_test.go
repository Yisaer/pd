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

package schedulers

import (
	"context"
	"fmt"
	"math/rand"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/copysets"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/kv"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/versioninfo"
)

var _ = Suite(&testBalanceCopysetSuite{})

type testBalanceCopysetSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	oc     *schedule.OperatorController
}

func (s *testBalanceCopysetSuite) SetUpTest(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	opt := config.NewTestOptions()
	s.tc = mockcluster.NewCluster(opt)
	s.oc = schedule.NewOperatorController(s.ctx, nil, nil)
}

func (s *testBalanceCopysetSuite) TestBalanceCopySet(c *C) {
	opt := config.NewTestOptions()
	// TODO: enable palcementrules
	opt.SetPlacementRuleEnabled(false)
	tc := s.tc
	tc.DisableFeature(versioninfo.JointConsensus)
	tc.SetTolerantSizeRatio(2.5)
	// Add stores 1,2,3,4,5.
	storeCount := 30
	for i := 1; i <= storeCount; i++ {
		tc.AddRegionStore(uint64(i), 0)
	}
	cs := tc.GetCopySets()
	c.Assert(len(cs), Equals, 30)
	var (
		id      uint64
		regions []*metapb.Region
	)
	regionCount := 200
	for i := 0; i < regionCount; i++ {
		storeID := rand.Intn(storeCount) + 1
		cs, err := selectRandCopySet(uint64(storeID), cs)
		if err != nil {
			continue
		}
		n1, n2, n3 := cs.GetNodesID()
		peers := []*metapb.Peer{
			{Id: id + 1, StoreId: n1},
			{Id: id + 2, StoreId: n2},
			{Id: id + 3, StoreId: n3},
		}
		region := &metapb.Region{
			Id:       id + 4,
			Peers:    peers,
			StartKey: []byte(fmt.Sprintf("s_%02d", i)),
			EndKey:   []byte(fmt.Sprintf("s_%02d", i+1)),
		}
		regions = append(regions, region)
		fmt.Println(fmt.Sprintf("%v: sign:%v", region.GetId(), cs.Sign()))
		id += 4
	}
	// empty case
	regions[regionCount-1].EndKey = []byte("")
	for _, meta := range regions {
		leader := rand.Intn(4) % 3
		regionInfo := core.NewRegionInfo(
			meta,
			meta.Peers[leader],
			core.SetApproximateKeys(96),
			core.SetApproximateSize(96),
		)

		tc.Regions.SetRegion(regionInfo)
	}
	for i := 0; i < 100; i++ {
		_, err := tc.AllocPeer(1)
		c.Assert(err, IsNil)
	}
	for i := 1; i <= storeCount; i++ {
		tc.UpdateStoreStatus(uint64(i))
	}
	//for i := 1; i <= storeCount; i++ {
	//	storeID := uint64(i)
	//	x := s.tc.GetStoreRegions(storeID)
	//	debug(storeID,x)
	//}
	oc := schedule.NewOperatorController(s.ctx, nil, nil)
	hb, err := schedule.CreateScheduler("balance-copyset-scheduler", oc, core.NewStorage(kv.NewMemoryKV()), schedule.ConfigSliceDecoder("balance-copyset-scheduler", []string{"s_00", "s_09", "t"}))
	c.Assert(err, IsNil)
	limit := 0
	for {
		if limit > 100 {
			break
		}
		ops := hb.Schedule(tc)
		if ops == nil {
			limit++
			continue
		}
		schedule.ApplyOperator(tc, ops[0])
	}
	for i := 1; i <= storeCount; i++ {
		leaderCount := tc.Regions.GetStoreLeaderCount(uint64(i))
		c.Check(leaderCount, LessEqual, 12)
		regionCount := tc.Regions.GetStoreRegionCount(uint64(i))
		c.Check(regionCount, LessEqual, 32)
	}
}

func selectNInM(n, m int) []uint64 {
	array := make([]uint64, n, n)
	r := rand.Perm(m)
	for i := 0; i < n; i++ {
		array[i] = uint64(r[i] + 1)
	}
	return array
}

func selectRandCopySet(storeID uint64, css []copysets.CopySet) (copysets.CopySet, error) {
	for _, copyset := range css {
		if copyset.IsStoreInCopySet(storeID) {
			return copyset, nil
		}
	}
	x := copysets.CopySet{}
	return x, fmt.Errorf("no copyset")
}
