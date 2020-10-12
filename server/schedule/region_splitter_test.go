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
	"time"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
)

type mockSplitRegionsHandler struct {
	// regionID -> splitKeys
	groupKeysByRegion map[uint64][][]byte
	// regionID -> startKey, endKey
	regions map[uint64][2][]byte
}

func newMockSplitRegionsHandler() *mockSplitRegionsHandler {
	return &mockSplitRegionsHandler{
		groupKeysByRegion: map[uint64][][]byte{},
		regions:           map[uint64][2][]byte{},
	}

}

// SplitRegionByKeys mock SplitRegionsHandler
func (m *mockSplitRegionsHandler) SplitRegionByKeys(region *core.RegionInfo, splitKeys [][]byte) error {
	m.regions[region.GetID()] = [2][]byte{
		region.GetStartKey(),
		region.GetEndKey(),
	}
	m.groupKeysByRegion[region.GetID()] = splitKeys
	return nil
}

// WatchRegionsByKeyRange mock SplitRegionsHandler
func (m *mockSplitRegionsHandler) WatchRegionsByKeyRange(startKey, endKey []byte, timeout, watchInterval time.Duration) []uint64 {
	for regionID, keyRange := range m.regions {
		if bytes.Equal(startKey, keyRange[0]) && bytes.Equal(endKey, keyRange[1]) {
			returned := []uint64{regionID}
			for i := 0; i < len(m.groupKeysByRegion[regionID]); i++ {
				returned = append(returned, returned[0]+uint64(i)+1000)
			}
			return returned
		}
	}
	return nil
}

var _ = Suite(&testRegionSplitterSuite{})

type testRegionSplitterSuite struct{}

func (s *testRegionSplitterSuite) TestRegionSplitter(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	handler := newMockSplitRegionsHandler()
	tc.AddLeaderRegionWithRange(1, "aaa", "zzz", 2, 3)
	splitter := NewRegionSplitter(tc, handler)
	newRegions := map[uint64]struct{}{}
	failureKeys := splitter.splitRegions([][]byte{[]byte("bbb"), []byte("ccc")}, newRegions)
	c.Assert(len(failureKeys), Equals, 0)
	c.Assert(len(newRegions), Equals, 3)
}
