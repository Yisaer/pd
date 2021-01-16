package cases

import (
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
	"math"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
)

func newBalanceRegion() *Case {
	var simCase Case

	storeNum, regionNum := getStoreNum(), getRegionNum()
	for i := 1; i <= storeNum; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:        IDAllocator.nextID(),
			Status:    metapb.StoreState_Up,
			Capacity:  1 * TB,
			Available: 900 * GB,
			Version:   "2.1.0",
		})
	}

	//x := selectNinM(3, 100)
	for i := 0; i < storeNum*regionNum/3; i++ {
		//s1 := x[0]
		//s2 := x[1]
		//s3 := x[2]
		s1 := uint64(storeNum)
		s2 := uint64((i+1)%(storeNum-1)) + 1
		s3 := uint64((i+2)%(storeNum-1)) + 1
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: s1},
			{Id: IDAllocator.nextID(), StoreId: s2},
			{Id: IDAllocator.nextID(), StoreId: s3},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * MB,
			Keys:   960000,
		})
	}
	//threshold := 0.05
	max := 0
	min := math.MaxInt64
	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		res := true
		regionCounts := make([]int, 0, storeNum)
		for i := 1; i <= storeNum; i++ {
			regionCount := len(regions.GetStoreRegions(uint64(i)))
			regionCounts = append(regionCounts, regionCount)
			//leaderCount := regions.GetStoreLeaderCount(uint64(i))
			//leaderCounts = append(leaderCounts, leaderCount)
			max, min, res = minAndMax(min, max, 5, regionCounts)
		}
		simutil.Logger.Info("current counts", zap.Ints("regions", regionCounts))
		return res
	}
	return &simCase
}

func minAndMax(min, max, tole int, regionCounts []int) (int, int, bool) {
	tmax := 0
	tmin := math.MaxInt64
	for _, regionCount := range regionCounts {
		if regionCount > tmax {
			tmax = regionCount
		} else if regionCount < tmin {
			tmin = regionCount
		}
	}
	if tmax > max {
		max = tmax
	}
	if tmin < min {
		min = tmin
	}
	exit := false
	if max-min < tole {
		exit = true
	}
	return min, max, exit
}
