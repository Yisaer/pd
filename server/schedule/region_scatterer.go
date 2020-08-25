// Copyright 2017 PingCAP, Inc.
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
	"math"
	"math/rand"
	"sync"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/pkg/codec"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/filter"
	"github.com/pingcap/pd/v4/server/schedule/operator"
	"github.com/pingcap/pd/v4/server/schedule/opt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const regionScatterName = "region-scatter"

type selectedLeaderStores struct {
	mu                 sync.RWMutex
	leaderDistribution map[int64]map[uint64]uint64
	storeLeaders       map[uint64]uint64
}

func (s *selectedLeaderStores) put(tableID int64, storeID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.leaderDistribution[tableID]; !ok {
		s.leaderDistribution[tableID] = map[uint64]uint64{}
	}
	s.storeLeaders[storeID] = s.storeLeaders[storeID] + 1
	s.leaderDistribution[tableID][storeID] = s.leaderDistribution[tableID][storeID] + 1
}

func (s *selectedLeaderStores) getTableStoreCount(tableID int64, storeID uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.leaderDistribution[tableID]; !ok {
		s.leaderDistribution[tableID] = map[uint64]uint64{}
	}
	return s.leaderDistribution[tableID][storeID]
}

func (s *selectedLeaderStores) getStoreLeaderCount(storeID uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.storeLeaders[storeID]
}

func newSelectedLeaderStores() *selectedLeaderStores {
	return &selectedLeaderStores{
		leaderDistribution: map[int64]map[uint64]uint64{},
		storeLeaders:       map[uint64]uint64{},
	}
}

type selectedStores struct {
	mu               sync.Mutex
	stores           map[uint64]struct{}
	peerDistribution map[int64]map[uint64]uint64
}

func newSelectedStores() *selectedStores {
	return &selectedStores{
		stores: make(map[uint64]struct{}),
	}
}

func (s *selectedStores) put(tableID int64, storeID uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.stores[storeID]; ok {
		return false
	}
	s.stores[storeID] = struct{}{}
	distribution, ok := s.peerDistribution[tableID]
	if !ok {
		distribution = map[uint64]uint64{}
	}
	distribution[storeID] = distribution[storeID] + 1
	s.peerDistribution[tableID] = distribution
	return true
}

func (s *selectedStores) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stores = make(map[uint64]struct{})
}

func (s *selectedStores) newFilter(scope string) filter.Filter {
	s.mu.Lock()
	defer s.mu.Unlock()
	cloned := make(map[uint64]struct{})
	for id := range s.stores {
		cloned[id] = struct{}{}
	}
	return filter.NewExcludedFilter(scope, nil, cloned)
}

func (s *selectedStores) getTableDistribution(tableID int64, storeID uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	distribution, ok := s.peerDistribution[tableID]
	if !ok || distribution == nil {
		return 0
	}
	v, ok := distribution[storeID]
	if !ok {
		return 0
	}
	return v
}

// RegionScatterer scatters regions.
type RegionScatterer struct {
	name           string
	cluster        opt.Cluster
	ordinaryEngine engineContext
	specialEngines map[string]engineContext
}

// NewRegionScatterer creates a region scatterer.
// RegionScatter is used for the `Lightning`, it will scatter the specified regions before import data.
func NewRegionScatterer(cluster opt.Cluster) *RegionScatterer {
	return &RegionScatterer{
		name:           regionScatterName,
		cluster:        cluster,
		ordinaryEngine: newEngineContext(filter.NewOrdinaryEngineFilter(regionScatterName)),
		specialEngines: make(map[string]engineContext),
	}
}

type engineContext struct {
	filters        []filter.Filter
	selected       *selectedStores
	selectedLeader *selectedLeaderStores
}

func newEngineContext(filters ...filter.Filter) engineContext {
	filters = append(filters, filter.StoreStateFilter{ActionScope: regionScatterName})
	return engineContext{
		filters:        filters,
		selected:       newSelectedStores(),
		selectedLeader: newSelectedLeaderStores(),
	}
}

// Scatter relocates the region.
func (r *RegionScatterer) Scatter(region *core.RegionInfo) (*operator.Operator, error) {
	if !opt.IsRegionReplicatedLoose(r.cluster, region) {
		r.cluster.AddSuspectRegions(region.GetID())
		return nil, errors.Errorf("region %d is not fully replicated", region.GetID())
	}

	if region.GetLeader() == nil {
		return nil, errors.Errorf("region %d has no leader", region.GetID())
	}

	return r.scatterRegion(region), nil
}

func (r *RegionScatterer) scatterRegion(region *core.RegionInfo) *operator.Operator {
	ordinaryFilter := filter.NewOrdinaryEngineFilter(r.name)
	var ordinaryPeers []*metapb.Peer
	specialPeers := make(map[string][]*metapb.Peer)
	// Group peers by the engine of their stores
	for _, peer := range region.GetPeers() {
		store := r.cluster.GetStore(peer.GetStoreId())
		if ordinaryFilter.Target(r.cluster, store) {
			ordinaryPeers = append(ordinaryPeers, peer)
		} else {
			engine := store.GetLabelValue(filter.EngineKey)
			specialPeers[engine] = append(specialPeers[engine], peer)
		}
	}
	tableID := codec.Key(region.GetStartKey()).TableID()
	targetPeers := make(map[uint64]*metapb.Peer)
	scatterWithSameEngine := func(peers []*metapb.Peer, context engineContext) {
		stores := r.collectAvailableStores(region, context)
		for _, peer := range peers {
			if len(stores) == 0 {
				context.selected.reset()
				stores = r.collectAvailableStores(region, context)
			}
			if context.selected.put(tableID, peer.GetStoreId()) {
				delete(stores, peer.GetStoreId())
				targetPeers[peer.GetStoreId()] = peer
				continue
			}
			newPeer := r.selectPeerToReplace(stores, region, peer, context)
			if newPeer == nil {
				targetPeers[peer.GetStoreId()] = peer
				continue
			}
			// Remove it from stores and mark it as selected.
			delete(stores, newPeer.GetStoreId())
			context.selected.put(tableID, newPeer.GetStoreId())
			targetPeers[newPeer.GetStoreId()] = newPeer
		}
	}

	scatterWithSameEngine(ordinaryPeers, r.ordinaryEngine)
	for engine, peers := range specialPeers {
		context, ok := r.specialEngines[engine]
		if !ok {
			context = newEngineContext(filter.NewEngineFilter(r.name, engine))
			r.specialEngines[engine] = context
		}
		scatterWithSameEngine(peers, context)
	}

	targetLeader := r.collectAvailableLeaderStores(region, targetPeers, r.ordinaryEngine)
	op, err := operator.CreateScatterRegionOperator("scatter-region", r.cluster, region, targetPeers, targetLeader)
	if err != nil {
		log.Debug("fail to create scatter region operator", zap.Error(err))
		return nil
	}
	op.SetPriorityLevel(core.HighPriority)
	return op
}

func (r *RegionScatterer) selectPeerToReplace(stores map[uint64]*core.StoreInfo, region *core.RegionInfo, oldPeer *metapb.Peer, context engineContext) *metapb.Peer {
	// scoreGuard guarantees that the distinct score will not decrease.
	regionStores := r.cluster.GetRegionStores(region)
	storeID := oldPeer.GetStoreId()
	sourceStore := r.cluster.GetStore(storeID)
	if sourceStore == nil {
		log.Error("failed to get the store", zap.Uint64("store-id", storeID))
	}
	var scoreGuard filter.Filter
	if r.cluster.IsPlacementRulesEnabled() {
		scoreGuard = filter.NewRuleFitFilter(r.name, r.cluster, region, oldPeer.GetStoreId())
	} else {
		scoreGuard = filter.NewDistinctScoreFilter(r.name, r.cluster.GetLocationLabels(), regionStores, sourceStore)
	}

	candidates := make([]*core.StoreInfo, 0, len(stores))
	for _, store := range stores {
		if !scoreGuard.Target(r.cluster, store) {
			continue
		}
		candidates = append(candidates, store)
	}

	if len(candidates) == 0 {
		return nil
	}

	minPeer := uint64(math.MaxUint64)
	tableID := codec.Key(region.GetStartKey()).TableID()
	var selectedCandidateID uint64
	selectedCandidateID = 0
	for _, candidate := range candidates {
		count := context.selected.getTableDistribution(tableID, candidate.GetID())
		if count < minPeer {
			minPeer = count
			selectedCandidateID = candidate.GetID()
		}
	}
	if selectedCandidateID < 1 {
		target := candidates[rand.Intn(len(candidates))]
		return &metapb.Peer{
			StoreId:   target.GetID(),
			IsLearner: oldPeer.GetIsLearner(),
		}
	}

	return &metapb.Peer{
		StoreId:   selectedCandidateID,
		IsLearner: oldPeer.GetIsLearner(),
	}
}

func (r *RegionScatterer) collectAvailableStores(region *core.RegionInfo, context engineContext) map[uint64]*core.StoreInfo {
	filters := []filter.Filter{
		context.selected.newFilter(r.name),
		filter.NewExcludedFilter(r.name, nil, region.GetStoreIds()),
	}
	filters = append(filters, context.filters...)

	stores := r.cluster.GetStores()
	targets := make(map[uint64]*core.StoreInfo, len(stores))
	for _, store := range stores {
		if filter.Target(r.cluster, store, filters) && !store.IsBusy() {
			targets[store.GetID()] = store
		}
	}
	return targets
}

func (r *RegionScatterer) collectAvailableLeaderStores(region *core.RegionInfo, peers map[uint64]*metapb.Peer, context engineContext) uint64 {
	m := uint64(math.MaxUint64)
	tableID := codec.Key(region.GetStartKey()).TableID()
	minTableLeaderStores := map[uint64]struct{}{}
	for storeID := range peers {
		count := context.selectedLeader.getTableStoreCount(tableID, storeID)
		if m > count {
			m = count
			minTableLeaderStores = map[uint64]struct{}{}
			minTableLeaderStores[storeID] = struct{}{}
		} else if m == count {
			minTableLeaderStores[storeID] = struct{}{}
		}
	}
	m = uint64(math.MaxUint64)
	id := uint64(0)
	for storeID := range minTableLeaderStores {
		count := context.selectedLeader.getStoreLeaderCount(storeID)
		if m > count {
			m = count
			id = storeID
		}
	}
	if id != 0 {
		context.selectedLeader.put(tableID, id)
	}
	return id
}
