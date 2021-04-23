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
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/movingaverage"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

const (
	// TopNN is the threshold which means we can get hot threshold from store.
	TopNN = 60
	// HotThresholdRatio is used to calculate hot thresholds
	HotThresholdRatio = 0.8
	// ReportInterval indicates the interval between each data reporting
	// TODO: change into StoreHeartBeatReportInterval when we use store heartbeat to report data
	ReportInterval = RegionHeartBeatReportInterval
	topNTTL        = 3 * ReportInterval * time.Second

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

// hotPeerCache saves the hot peer's statistics.
type hotPeerCache struct {
	kind           FlowKind
	peersOfStore   map[uint64]*TopN               // storeID -> hot peers
	storesOfRegion map[uint64]map[uint64]struct{} // regionID -> storeIDs
}

// NewHotStoresStats creates a HotStoresStats
func NewHotStoresStats(kind FlowKind) *hotPeerCache {
	return &hotPeerCache{
		kind:           kind,
		peersOfStore:   make(map[uint64]*TopN),
		storesOfRegion: make(map[uint64]map[uint64]struct{}),
	}
}

// TODO: rename RegionStats as PeerStats
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

// Update updates the items in statistics.
func (f *hotPeerCache) Update(item *HotPeerStat) {
	if item.IsNeedDelete() {
		if peers, ok := f.peersOfStore[item.StoreID]; ok {
			peers.Remove(item.RegionID)
		}

		if stores, ok := f.storesOfRegion[item.RegionID]; ok {
			delete(stores, item.StoreID)
		}
		item.Log("region heartbeat delete from cache", log.Debug)
	} else {
		peers, ok := f.peersOfStore[item.StoreID]
		if !ok {
			peers = NewTopN(dimLen, TopNN, topNTTL)
			f.peersOfStore[item.StoreID] = peers
		}
		peers.Put(item)

		stores, ok := f.storesOfRegion[item.RegionID]
		if !ok {
			stores = make(map[uint64]struct{})
			f.storesOfRegion[item.RegionID] = stores
		}
		stores[item.StoreID] = struct{}{}
		item.Log("region heartbeat update", log.Debug)
	}
}

func (f *hotPeerCache) collectPeerMetrics(byteRate, keyRate float64, interval uint64) {
	// TODO
}

// CheckRegionFlow checks the flow information of region.
func (f *hotPeerCache) CheckRegionFlow(region *core.RegionInfo) (ret []*HotPeerStat) {
	regionID := region.GetID()
	storeIDs := f.getAllStoreIDs(region)
	var peers []uint64
	for _, peer := range region.GetPeers() {
		peers = append(peers, peer.StoreId)
	}

	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()

	{
		bytes := float64(f.getRegionBytes(region))
		keys := float64(f.getRegionKeys(region))
		byteRate := bytes / float64(interval)
		keyRate := keys / float64(interval)
		f.collectRegionMetrics(byteRate, keyRate, interval)
	}

	for _, storeID := range storeIDs {
		peer := region.GetStorePeer(storeID)
		var item *HotPeerStat
		if peer != nil {
			peerInfo := core.FromMetaPeer(peer).
				SetReadKeys(region.GetKeysRead()).
				SetReadBytes(region.GetBytesRead()).
				SetWriteKeys(region.GetKeysWritten()).
				SetWriteBytes(region.GetBytesWritten())
			item = f.CheckPeerFlow(peerInfo, region, interval)
		} else {
			// Not in region anymore
			item = f.getOldHotPeerStat(regionID, storeID)
			item.needDelete = true
		}
		if item != nil {
			ret = append(ret, item)
		}
	}

	log.Debug("region heartbeat info",
		zap.String("type", f.kind.String()),
		zap.Uint64("region", region.GetID()),
		zap.Uint64("leader", region.GetLeader().GetStoreId()),
		zap.Uint64s("peers", peers),
	)
	return ret
}

// CheckPeerFlow checks the flow information of a peer.
func (f *hotPeerCache) CheckPeerFlow(peer *core.PeerInfo, region *core.RegionInfo, interval uint64) *HotPeerStat {
	storeID := peer.GetStoreID()
	bytes := float64(f.getPeerBytes(peer))
	keys := float64(f.getPeerKeys(peer))
	byteRate := bytes / float64(interval)
	keyRate := keys / float64(interval)
	f.collectPeerMetrics(byteRate, keyRate, interval)
	justTransferLeader := f.justTransferLeaderPeer(region)
	isExpired := f.isPeerExpired(peer, region) // transfer read leader or remove write peer
	oldItem := f.getOldHotPeerStat(region.GetID(), storeID)
	if !isExpired && Denoising && interval < HotRegionReportMinInterval {
		return nil
	}
	thresholds := f.calcHotThresholds(storeID)
	var peers []uint64
	for _, peer := range region.GetPeers() {
		peers = append(peers, peer.StoreId)
	}
	newItem := &HotPeerStat{
		StoreID:            storeID,
		RegionID:           region.GetID(),
		Kind:               f.kind,
		ByteRate:           byteRate,
		KeyRate:            keyRate,
		LastUpdateTime:     time.Now(),
		needDelete:         isExpired,
		isLeader:           region.GetLeader().GetStoreId() == storeID,
		justTransferLeader: justTransferLeader,
		interval:           interval,
		peers:              peers,
		thresholds:         thresholds,
	}
	if oldItem == nil {
		if f.kind == ReadFlow {
			if justTransferLeader {
				for _, storeID := range f.getAllStoreIDs(region) {
					oldItem = f.getOldHotPeerStat(region.GetID(), storeID)
					if oldItem != nil {
						break
					}
				}
			}
		} else {
			for _, storeID := range f.getAllStoreIDs(region) {
				oldItem = f.getOldHotPeerStat(region.GetID(), storeID)
				if oldItem != nil {
					break
				}
			}
		}
	}
	return f.updateHotPeerStat(newItem, oldItem, bytes, keys, time.Duration(interval)*time.Second)
}

func (f *hotPeerCache) IsRegionHot(region *core.RegionInfo, hotDegree int) bool {
	switch f.kind {
	case WriteFlow:
		return f.isRegionHotWithAnyPeers(region, hotDegree)
	case ReadFlow:
		return f.isRegionHotWithPeer(region, region.GetLeader(), hotDegree)
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
		return peer.GetWrittenBytes()
	case ReadFlow:
		return peer.GetReadBytes()
	}
	return 0
}

func (f *hotPeerCache) getPeerKeys(peer *core.PeerInfo) uint64 {
	switch f.kind {
	case WriteFlow:
		return peer.GetWrittenKeys()
	case ReadFlow:
		return peer.GetReadKeys()
	}
	return 0
}

func (f *hotPeerCache) getOldHotPeerStat(regionID, storeID uint64) *HotPeerStat {
	if hotPeers, ok := f.peersOfStore[storeID]; ok {
		if v := hotPeers.Get(regionID); v != nil {
			return v.(*HotPeerStat)
		}
	}
	return nil
}

func (f *hotPeerCache) isPeerExpired(peer *core.PeerInfo, region *core.RegionInfo) bool {
	storeID := peer.GetStoreID()
	switch f.kind {
	case WriteFlow:
		return region.GetStorePeer(storeID) == nil
	case ReadFlow:
		return region.GetLeader().GetStoreId() != storeID
	}
	return false
}

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

func (f *hotPeerCache) isOldColdPeer(oldItem *HotPeerStat, storeID uint64) bool {
	isOldPeer := func() bool {
		for _, id := range oldItem.peers {
			if id == storeID {
				return true
			}
		}
		return false
	}
	noInCache := func() bool {
		ids, ok := f.storesOfRegion[oldItem.RegionID]
		if ok {
			for id := range ids {
				if id == storeID {
					return false
				}
			}
		}
		return true
	}
	return isOldPeer() && noInCache()
}

func (f *hotPeerCache) justTransferLeaderPeer(region *core.RegionInfo) bool {
	ids, ok := f.storesOfRegion[region.GetID()]
	if ok {
		for storeID := range ids {
			oldItem := f.getOldHotPeerStat(region.GetID(), storeID)
			if oldItem == nil {
				continue
			}
			if oldItem.isLeader {
				return oldItem.StoreID != region.GetLeader().GetStoreId()
			}
		}
	}
	return false
}

func (f *hotPeerCache) isRegionHotWithAnyPeers(region *core.RegionInfo, hotDegree int) bool {
	for _, peer := range region.GetPeers() {
		if f.isRegionHotWithPeer(region, peer, hotDegree) {
			return true
		}
	}
	return false
}

func (f *hotPeerCache) isRegionHotWithPeer(region *core.RegionInfo, peer *metapb.Peer, hotDegree int) bool {
	if peer == nil {
		return false
	}
	storeID := peer.GetStoreId()
	if peers, ok := f.peersOfStore[storeID]; ok {
		if stat := peers.Get(region.GetID()); stat != nil {
			return stat.(*HotPeerStat).HotDegree >= hotDegree
		}
	}
	return false
}

func (f *hotPeerCache) getDefaultTimeMedian() *movingaverage.TimeMedian {
	return movingaverage.NewTimeMedian(DefaultAotSize, rollingWindowsSize, RegionHeartBeatReportInterval*time.Second)
}

func (f *hotPeerCache) updateHotPeerStat(newItem, oldItem *HotPeerStat, bytes, keys float64, interval time.Duration) *HotPeerStat {
	if newItem.needDelete {
		return newItem
	}

	if oldItem == nil {
		if interval == 0 {
			return nil
		}
		isHot := bytes/interval.Seconds() >= newItem.thresholds[byteDim] || keys/interval.Seconds() >= newItem.thresholds[keyDim]
		if !isHot {
			return nil
		}
		if interval.Seconds() >= RegionHeartBeatReportInterval {
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
		return newItem
	}

	newItem.rollingByteRate = oldItem.rollingByteRate
	newItem.rollingKeyRate = oldItem.rollingKeyRate

	if newItem.justTransferLeader {
		// skip the first heartbeat flow statistic after transfer leader, because its statistics are calculated by the last leader in this store and are inaccurate
		// maintain anticount and hotdegree to avoid store threshold and hot peer are unstable.
		newItem.HotDegree = oldItem.HotDegree
		newItem.AntiCount = oldItem.AntiCount
		newItem.lastTransferLeaderTime = time.Now()
		return newItem
	}

	newItem.lastTransferLeaderTime = oldItem.lastTransferLeaderTime
	newItem.rollingByteRate.Add(bytes, interval)
	newItem.rollingKeyRate.Add(keys, interval)

	if !newItem.rollingKeyRate.isFull() {
		// not update hot degree and anti count
		newItem.HotDegree = oldItem.HotDegree
		newItem.AntiCount = oldItem.AntiCount
	} else {
		if f.isOldColdPeer(oldItem, newItem.StoreID) {
			if newItem.isFullAndHot() {
				newItem.HotDegree = 1
				newItem.AntiCount = hotRegionAntiCount
			} else {
				newItem.needDelete = true
			}
		} else {
			if newItem.isFullAndHot() {
				newItem.HotDegree = oldItem.HotDegree + 1
				newItem.AntiCount = hotRegionAntiCount
			} else {
				newItem.HotDegree = oldItem.HotDegree - 1
				newItem.AntiCount = oldItem.AntiCount - 1
				if newItem.AntiCount <= 0 {
					newItem.needDelete = true
				}
			}
		}
		newItem.clearLastAverage()
	}
	return newItem
}

// TODO: remove it in future
func (f *hotPeerCache) getRegionBytes(region *core.RegionInfo) uint64 {
	switch f.kind {
	case WriteFlow:
		return region.GetBytesWritten()
	case ReadFlow:
		return region.GetBytesRead()
	}
	return 0
}

// TODO: remove it in future
func (f *hotPeerCache) getRegionKeys(region *core.RegionInfo) uint64 {
	switch f.kind {
	case WriteFlow:
		return region.GetKeysWritten()
	case ReadFlow:
		return region.GetKeysRead()
	}
	return 0
}

// TODO: remove it in future
// gets the storeIDs, including old region and new region
func (f *hotPeerCache) getAllStoreIDs(region *core.RegionInfo) []uint64 {
	storeIDs := make(map[uint64]struct{})
	ret := make([]uint64, 0, len(region.GetPeers()))
	// old stores
	ids, ok := f.storesOfRegion[region.GetID()]
	if ok {
		for storeID := range ids {
			storeIDs[storeID] = struct{}{}
			ret = append(ret, storeID)
		}
	}

	// new stores
	for _, peer := range region.GetPeers() {
		// ReadFlow no need consider the followers.
		if f.kind == ReadFlow && peer.GetStoreId() != region.GetLeader().GetStoreId() {
			continue
		}
		if _, ok := storeIDs[peer.GetStoreId()]; !ok {
			storeIDs[peer.GetStoreId()] = struct{}{}
			ret = append(ret, peer.GetStoreId())
		}
	}

	return ret
}

// TODO: remove it in future
func (f *hotPeerCache) collectRegionMetrics(byteRate, keyRate float64, interval uint64) {
	regionHeartbeatIntervalHist.Observe(float64(interval))
	if interval == 0 {
		return
	}
	if f.kind == ReadFlow {
		readByteHist.Observe(byteRate)
		readKeyHist.Observe(keyRate)
	}
	if f.kind == WriteFlow {
		writeByteHist.Observe(byteRate)
		writeKeyHist.Observe(keyRate)
	}
}
