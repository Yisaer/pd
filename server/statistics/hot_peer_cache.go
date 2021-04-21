// Copyright 2019 TiKV Project Authors.
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

package statistics

import (
	"math"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
)

const (
	// TopNN is the threshold which means we can get hot threshold from store.
	TopNN = 60
	// HotThresholdRatio is used to calculate hot thresholds
	HotThresholdRatio = 0.8
	topNTTL           = 3 * RegionHeartBeatReportInterval * time.Second

	rollingWindowsSize = 5

	// HotRegionReportMinInterval is used for the simulator and test
	HotRegionReportMinInterval = 3

	hotRegionAntiCount = 2
)

var (
	minHotThresholds = [2][dimLen]float64{
		WriteFlow: {
			byteDim: 1 * 1024,
			keyDim:  32,
		},
		ReadFlow: {
			byteDim: 8 * 1024,
			keyDim:  128,
		},
	}
)

type peerPair struct {
	PeerID  uint64
	StoreID uint64
}

// hotPeerCache saves the hot peer's statistics.
type hotPeerCache struct {
	kind         FlowKind
	peersOfStore map[uint64]*TopN      // storeID -> hot peers
	regionsPairs map[uint64][]peerPair // regionID -> peer pair
	//peers map[uint64]struct{} // recordPeer
	//storesOfRegion map[uint64]map[uint64]struct{} // regionID -> storeIDs
}

// NewHotStoresStats creates a HotStoresStats
func NewHotStoresStats(kind FlowKind) *hotPeerCache {
	return &hotPeerCache{
		kind:         kind,
		peersOfStore: make(map[uint64]*TopN),
		//regionsPeers:   make(map[uint64]map[uint64]struct{}),
		regionsPairs: make(map[uint64][]peerPair),
		//peers:        make(map[uint64]struct{}),
	}
}

// PeerStats returns hot items, return map storeID -> []* HotPeerStat
func (f *hotPeerCache) PeerStats(minHotDegree int) map[uint64][]*HotPeerStat {
	res := make(map[uint64][]*HotPeerStat)
	for storeID, peers := range f.peersOfStore {
		values := peers.GetAll()
		stat := make([]*HotPeerStat, 0, len(values))
		for _, v := range values {
			if peer := v.(*HotPeerStat); peer.HotDegree >= minHotDegree {
				stat = append(stat, peer)
			}
		}
		res[storeID] = stat
	}
	return res
}

func (f *hotPeerCache) CheckRegion(region *core.RegionInfo) {
	regionID := region.GetID()
	oldPeerPair, ok := f.regionsPairs[regionID]
	if !ok {
		return
	}
	for _, peerPair := range oldPeerPair {
		peerID := peerPair.PeerID
		storeID := peerPair.StoreID
		if region.GetPeer(peerID) == nil {
			item := f.getOldHotPeerStat(peerID, storeID)
			if item != nil {
				f.RemoveItem(item)
			}
		}
	}
}

func (f *hotPeerCache) AddPeerFlow(peer *core.PeerInfo, region *core.RegionInfo, interval uint64) {
	bytes := float64(f.getPeerBytes(peer))
	keys := float64(f.getPeerKeys(peer))
	byteRate := bytes / float64(interval)
	keyRate := keys / float64(interval)
	peerID := peer.PeerID
	storeID := peer.StoreID
	thresholds := f.calcHotThresholds(storeID)
	newItem := &HotPeerStat{
		StoreID:        storeID,
		RegionID:       region.GetID(),
		PeerID:         peerID,
		Kind:           f.kind,
		ByteRate:       byteRate,
		KeyRate:        keyRate,
		LastUpdateTime: time.Now(),
		isLeader:       region.GetLeader().GetStoreId() == storeID,
		interval:       interval,
		thresholds:     thresholds,
	}
	oldItem := f.getOldHotPeerStat(peerID, storeID)
	newItem, remove := f.updateHotPeerStat(newItem, oldItem, bytes, keys, time.Duration(interval)*time.Second)
	if remove {
		f.RemoveItem(newItem)
		return
	}
	if newItem != nil {
		f.AddItem(newItem)
	}
}

func (f *hotPeerCache) AddItem(item *HotPeerStat) {
	peers, ok := f.peersOfStore[item.StoreID]
	if !ok {
		peers = NewTopN(dimLen, TopNN, topNTTL)
		f.peersOfStore[item.StoreID] = peers
	}
	peers.Put(item)

	peerPairs, ok := f.regionsPairs[item.RegionID]
	if !ok {
		peerPairs = make([]peerPair, 0)
	}
	isNew := true
	for _, pp := range peerPairs {
		if pp.PeerID == item.PeerID {
			isNew = false
			break
		}
	}
	if isNew {
		peerPairs = append(peerPairs, peerPair{PeerID: item.PeerID, StoreID: item.StoreID})
	}
	f.regionsPairs[item.RegionID] = peerPairs
}

func (f *hotPeerCache) RemoveItem(item *HotPeerStat) {
	peers, ok := f.peersOfStore[item.StoreID]
	if !ok {
		return
	}
	peers.Remove(item.PeerID)
	newPairs := make([]peerPair, 0)
	for _, pp := range f.regionsPairs[item.RegionID] {
		if pp.PeerID != item.PeerID {
			newPairs = append(newPairs, pp)
		}
	}
	f.regionsPairs[item.RegionID] = newPairs
}

// RegionStats returns hot items
func (f *hotPeerCache) RegionStats(minHotDegree int) map[uint64][]*HotPeerStat {
	res := make(map[uint64][]*HotPeerStat)
	for storeID, peers := range f.peersOfStore {
		values := peers.GetAll()
		stat := make([]*HotPeerStat, 0, len(values))
		for _, v := range values {
			if peer := v.(*HotPeerStat); peer.HotDegree >= minHotDegree {
				stat = append(stat, peer)
			}
		}
		res[storeID] = stat
	}
	return res
}

//func (f *hotPeerCache) update(item *HotPeerStat) {
//	peers, ok := f.peersOfStore[item.StoreID]
//	if !ok {
//		peers = NewTopN(dimLen, TopNN, topNTTL)
//		f.peersOfStore[item.StoreID] = peers
//	}
//	peers.Put(item)
//
//	stores, ok := f.regionsPairs[item.RegionID]
//	if !ok {
//		stores = make([]peerPair, 0)
//		f.regionsPairs[item.RegionID] = stores
//	}
//	find := false
//	for _, peerPair := range f.regionsPairs[item.RegionID] {
//		if peerPair.PeerID == item.PeerID {
//			find = true
//			break
//		}
//	}
//	if !find {
//
//		f.regionsPairs[item.RegionID] = append(f.regionsPairs[item.RegionID], peerPair{PeerID: item.PeerID, StoreID: item.StoreID})
//	}
//}

//func (f *hotPeerCache) collectRegionMetrics(byteRate, keyRate float64, interval uint64) {
//	regionHeartbeatIntervalHist.Observe(float64(interval))
//	if interval == 0 {
//		return
//	}
//	if f.kind == ReadFlow {
//		readByteHist.Observe(byteRate)
//		readKeyHist.Observe(keyRate)
//	}
//	if f.kind == WriteFlow {
//		writeByteHist.Observe(byteRate)
//		writeKeyHist.Observe(keyRate)
//	}
//}

//
func (f *hotPeerCache) IsRegionHot(region *core.RegionInfo, hotDegree int) bool {
	switch f.kind {
	case WriteFlow:
		return f.isRegionHotWithAnyPeers(region, hotDegree)
	case ReadFlow:
		return f.isRegionHotWithAnyPeers(region, hotDegree)
	}
	return false
}

func (f *hotPeerCache) CollectMetrics(typ string) {
	for storeID, peers := range f.peersOfStore {
		store := storeTag(storeID)
		thresholds := f.calcHotThresholds(storeID)
		hotCacheStatusGauge.WithLabelValues("total_length", store, typ).Set(float64(peers.Len()))
		hotCacheStatusGauge.WithLabelValues("byte-rate-threshold", store, typ).Set(thresholds[byteDim])
		hotCacheStatusGauge.WithLabelValues("key-rate-threshold", store, typ).Set(thresholds[keyDim])
		// for compatibility
		hotCacheStatusGauge.WithLabelValues("hotThreshold", store, typ).Set(thresholds[byteDim])
	}
}

func (f *hotPeerCache) getPeerBytes(peer *core.PeerInfo) uint64 {
	switch f.kind {
	case WriteFlow:
		return peer.WriteBytes
	case ReadFlow:
		return peer.ReadBytes
	}
	return 0
}

func (f *hotPeerCache) getPeerKeys(peer *core.PeerInfo) uint64 {
	switch f.kind {
	case WriteFlow:
		return peer.WriteKeys
	case ReadFlow:
		return peer.ReadKeys
	}
	return 0
}

func (f *hotPeerCache) getOldHotPeerStat(peerID, storeID uint64) *HotPeerStat {
	if hotPeers, ok := f.peersOfStore[storeID]; ok {
		if v := hotPeers.Get(peerID); v != nil {
			return v.(*HotPeerStat)
		}
	}
	return nil
}

//
//func (f *hotPeerCache) isRegionExpired(region *core.RegionInfo, storeID uint64) bool {
//	switch f.kind {
//	case WriteFlow:
//		return region.GetStorePeer(storeID) == nil
//	case ReadFlow:
//		return region.GetLeader().GetStoreId() != storeID
//	}
//	return false
//}
//
func (f *hotPeerCache) calcHotThresholds(storeID uint64) [dimLen]float64 {
	minThresholds := minHotThresholds[f.kind]
	tn, ok := f.peersOfStore[storeID]
	if !ok || tn.Len() < TopNN {
		return minThresholds
	}
	ret := [dimLen]float64{
		byteDim: tn.GetTopNMin(byteDim).(*HotPeerStat).GetByteRate(),
		keyDim:  tn.GetTopNMin(keyDim).(*HotPeerStat).GetKeyRate(),
	}
	for k := 0; k < dimLen; k++ {
		ret[k] = math.Max(ret[k]*HotThresholdRatio, minThresholds[k])
	}
	return ret
}

//
//// gets the storeIDs, including old region and new region
//func (f *hotPeerCache) getAllStoreIDs(region *core.RegionInfo) []uint64 {
//	storeIDs := make(map[uint64]struct{})
//	ret := make([]uint64, 0, len(region.GetPeers()))
//	// old stores
//	ids, ok := f.storesOfRegion[region.GetID()]
//	if ok {
//		for storeID := range ids {
//			storeIDs[storeID] = struct{}{}
//			ret = append(ret, storeID)
//		}
//	}
//
//	// new stores
//	for _, peer := range region.GetPeers() {
//		// ReadFlow no need consider the followers.
//		if f.kind == ReadFlow && peer.GetStoreId() != region.GetLeader().GetStoreId() {
//			continue
//		}
//		if _, ok := storeIDs[peer.GetStoreId()]; !ok {
//			storeIDs[peer.GetStoreId()] = struct{}{}
//			ret = append(ret, peer.GetStoreId())
//		}
//	}
//
//	return ret
//}
//func (f *hotPeerCache) isOldColdPeer(oldItem *HotPeerStat, storeID uint64) bool {
//	isOldPeer := func() bool {
//		for _, id := range oldItem.peers {
//			if id == storeID {
//				return true
//			}
//		}
//		return false
//	}
//	noInCache := func() bool {
//		ids, ok := f.storesOfRegion[oldItem.RegionID]
//		if ok {
//			for id := range ids {
//				if id == storeID {
//					return false
//				}
//			}
//		}
//		return true
//	}
//	return isOldPeer() && noInCache()
//}

//
//func (f *hotPeerCache) justTransferLeader(region *core.RegionInfo) bool {
//	ids, ok := f.storesOfRegion[region.GetID()]
//	if ok {
//		for storeID := range ids {
//			oldItem := f.getOldHotPeerStat(region.GetID(), storeID)
//			if oldItem == nil {
//				continue
//			}
//			if oldItem.isLeader {
//				return oldItem.StoreID != region.GetLeader().GetStoreId()
//			}
//		}
//	}
//	return false
//}
//
func (f *hotPeerCache) isRegionHotWithAnyPeers(region *core.RegionInfo, hotDegree int) bool {
	for _, peer := range region.GetPeers() {
		if f.isRegionHotWithPeer(peer, hotDegree) {
			return true
		}
	}
	return false
}

func (f *hotPeerCache) isRegionHotWithPeer(peer *metapb.Peer, hotDegree int) bool {
	if peer == nil {
		return false
	}
	storeID := peer.GetStoreId()
	if peers, ok := f.peersOfStore[storeID]; ok {
		if stat := peers.Get(peer.GetId()); stat != nil {
			return stat.(*HotPeerStat).HotDegree >= hotDegree
		}
	}
	return false
}

//
//func (f *hotPeerCache) getDefaultTimeMedian() *movingaverage.TimeMedian {
//	return movingaverage.NewTimeMedian(DefaultAotSize, rollingWindowsSize, RegionHeartBeatReportInterval*time.Second)
//}
//

func (f *hotPeerCache) updateHotPeerStat(newItem, oldItem *HotPeerStat, bytes, keys float64, interval time.Duration) (*HotPeerStat, bool) {
	if oldItem == nil {
		if interval == 0 {
			return nil, false
		}
		isHot := bytes/interval.Seconds() >= newItem.thresholds[byteDim] || keys/interval.Seconds() >= newItem.thresholds[keyDim]
		if !isHot {
			return nil, false
		}
		if interval.Seconds() >= StoreHeartBeatReportInterval {
			newItem.HotDegree = 1
			newItem.AntiCount = hotRegionAntiCount
		}
		newItem.isNew = true
		newItem.rollingByteRate = newDimStat(byteDim)
		newItem.rollingKeyRate = newDimStat(keyDim)
		newItem.rollingByteRate.Add(bytes, interval)
		newItem.rollingKeyRate.Add(keys, interval)
		if newItem.rollingKeyRate.isFull() {
			newItem.clearLastAverage()
		}
		return newItem, false
	}

	remove := false
	newItem.rollingByteRate = oldItem.rollingByteRate
	newItem.rollingKeyRate = oldItem.rollingKeyRate
	newItem.rollingByteRate.Add(bytes, interval)
	newItem.rollingKeyRate.Add(keys, interval)
	if !newItem.rollingKeyRate.isFull() {
		// not update hot degree and anti count
		newItem.HotDegree = oldItem.HotDegree
		newItem.AntiCount = oldItem.AntiCount
	} else {
		if newItem.isFullAndHot() {
			newItem.HotDegree = oldItem.HotDegree + 1
			newItem.AntiCount = hotRegionAntiCount
		} else {
			newItem.HotDegree = oldItem.HotDegree - 1
			newItem.AntiCount = oldItem.AntiCount - 1
			if newItem.AntiCount <= 0 {
				remove = true
			}
		}
		newItem.clearLastAverage()
	}
	return newItem, remove
}
