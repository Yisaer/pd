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

package schedule

import (
	"bytes"
	"context"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
)

type mockSplitRegionsHandler struct {
	// regionID -> startKey, endKey
	regions map[uint64][2][]byte
}

func newMockSplitRegionsHandler() *mockSplitRegionsHandler {
	return &mockSplitRegionsHandler{
		regions: map[uint64][2][]byte{},
	}
}

// SplitRegionByKeys mock SplitRegionsHandler
func (m *mockSplitRegionsHandler) SplitRegionByKeys(region *core.RegionInfo, splitKeys [][]byte) error {
	m.regions[region.GetID()] = [2][]byte{
		region.GetStartKey(),
		region.GetEndKey(),
	}
	return nil
}

// WatchRegionsByKeyRange mock SplitRegionsHandler
func (m *mockSplitRegionsHandler) WatchRegionsByKeyRange(ctx context.Context, startKey, endKey []byte, splitKeys [][]byte,
	timeout, watchInterval time.Duration, response *splitKeyResponse, wg *sync.WaitGroup) {
	defer wg.Done()
	for regionID, keyRange := range m.regions {
		if bytes.Equal(startKey, keyRange[0]) && bytes.Equal(endKey, keyRange[1]) {
			regions := make([]*core.RegionInfo, 0)
			for i := 0; i < len(splitKeys); i++ {
				regions = append(regions, core.NewRegionInfo(&metapb.Region{Id: regionID + uint64(i) + 1000}, nil))
			}
			response.addRegionsID(regions...)
		}
	}
}

var _ = Suite(&testRegionSplitterSuite{})

type testRegionSplitterSuite struct{}

func (s *testRegionSplitterSuite) TestRegionSplitter(c *C) {
	ctx := context.Background()
	opt := config.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(opt)
	handler := newMockSplitRegionsHandler()
	tc.AddLeaderRegionWithRange(1, "eee", "hhh", 2, 3, 4)
	splitter := NewRegionSplitter(tc, handler)
	newRegions := map[uint64]struct{}{}
	// assert success
	failureKeys := splitter.splitRegionsByKeys(ctx, [][]byte{[]byte("fff"), []byte("ggg")}, newRegions)
	c.Assert(len(failureKeys), Equals, 0)
	c.Assert(len(newRegions), Equals, 2)

	percentage, newRegionsID := splitter.SplitRegions(ctx, [][]byte{[]byte("fff"), []byte("ggg")}, 1)
	c.Assert(percentage, Equals, 100)
	c.Assert(len(newRegionsID), Equals, 2)
	// assert out of range
	newRegions = map[uint64]struct{}{}
	failureKeys = splitter.splitRegionsByKeys(ctx, [][]byte{[]byte("aaa"), []byte("bbb")}, newRegions)
	c.Assert(len(failureKeys), Equals, 2)
	c.Assert(len(newRegions), Equals, 0)

	percentage, newRegionsID = splitter.SplitRegions(ctx, [][]byte{[]byte("aaa"), []byte("bbb")}, 1)
	c.Assert(percentage, Equals, 0)
	c.Assert(len(newRegionsID), Equals, 0)
}

func (s *testRegionSplitterSuite) TestGroupKeysByRegion(c *C) {
	opt := config.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(opt)
	handler := newMockSplitRegionsHandler()
	tc.AddLeaderRegionWithRange(1, "aaa", "ccc", 2, 3, 4)
	tc.AddLeaderRegionWithRange(2, "ccc", "eee", 2, 3, 4)
	tc.AddLeaderRegionWithRange(3, "fff", "ggg", 2, 3, 4)
	splitter := NewRegionSplitter(tc, handler)
	groupKeys, unprocessKeys := splitter.groupKeysByRegion([][]byte{
		[]byte("bbb"),
		[]byte("ddd"),
		[]byte("fff"),
		[]byte("zzz"),
	})
	c.Assert(len(groupKeys), Equals, 2)
	c.Assert(len(unprocessKeys), Equals, 1)
	for k, v := range groupKeys {
		switch k {
		case uint64(1):
			c.Assert(len(v.keys), Equals, 1)
			c.Assert(v.keys[0], DeepEquals, []byte("bbb"))
		case uint64(2):
			c.Assert(len(v.keys), Equals, 1)
			c.Assert(v.keys[0], DeepEquals, []byte("ddd"))
		}
	}
	c.Assert(len(unprocessKeys), Equals, 1)
	c.Assert(unprocessKeys[0], DeepEquals, []byte("zzz"))
}
