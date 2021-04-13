// Copyright 2017 TiKV Project Authors.
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
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
)

const regionScatterName = "region-scatter"

var gcInterval = time.Minute
var gcTTL = time.Minute * 3

type selectedStores struct {
	mu                sync.RWMutex
	groupDistribution *cache.TTLString // value type: map[uint64]uint64, group -> StoreID -> count
}

func newSelectedStores(ctx context.Context) *selectedStores {
	return &selectedStores{
		groupDistribution: cache.NewStringTTL(ctx, gcInterval, gcTTL),
	}
}

// Put plus count by storeID and group
func (s *selectedStores) Put(id uint64, group string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	distribution, ok := s.getDistributionByGroupLocked(group)
	if !ok {
		distribution = map[uint64]uint64{}
		distribution[id] = 0
	}
	distribution[id] = distribution[id] + 1
	s.groupDistribution.Put(group, distribution)
}

// Get the count by storeID and group
func (s *selectedStores) Get(id uint64, group string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	distribution, ok := s.getDistributionByGroupLocked(group)
	if !ok {
		return 0
	}
	count, ok := distribution[id]
	if !ok {
		return 0
	}
	return count
}

// GetGroupDistribution get distribution group by `group`
func (s *selectedStores) GetGroupDistribution(group string) (map[uint64]uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getDistributionByGroupLocked(group)
}

// getDistributionByGroupLocked should be called with lock
func (s *selectedStores) getDistributionByGroupLocked(group string) (map[uint64]uint64, bool) {
	if result, ok := s.groupDistribution.Get(group); ok {
		return result.(map[uint64]uint64), true
	}
	return nil, false
}

// RegionScatterer scatters regions.
type RegionScatterer struct {
	ctx            context.Context
	name           string
	cluster        opt.Cluster
	ordinaryEngine engineContext
	specialEngines map[string]engineContext
}

// NewRegionScatterer creates a region scatterer.
// RegionScatter is used for the `Lightning`, it will scatter the specified regions before import data.
func NewRegionScatterer(ctx context.Context, cluster opt.Cluster) *RegionScatterer {
	return &RegionScatterer{
		ctx:            ctx,
		name:           regionScatterName,
		cluster:        cluster,
		ordinaryEngine: newEngineContext(ctx, filter.NewOrdinaryEngineFilter(regionScatterName)),
		specialEngines: make(map[string]engineContext),
	}
}

type engineContext struct {
	filters        []filter.Filter
	selectedPeer   *selectedStores
	selectedLeader *selectedStores
}

func newEngineContext(ctx context.Context, filters ...filter.Filter) engineContext {
	filters = append(filters, &filter.StoreStateFilter{ActionScope: regionScatterName, MoveRegion: true, ScatterRegion: true})
	return engineContext{
		filters:        filters,
		selectedPeer:   newSelectedStores(ctx),
		selectedLeader: newSelectedStores(ctx),
	}
}

const maxSleepDuration = 1 * time.Minute
const initialSleepDuration = 100 * time.Millisecond
const maxRetryLimit = 30

// ScatterRegionsByRange directly scatter regions by ScatterRegions
func (r *RegionScatterer) ScatterRegionsByRange(startKey, endKey []byte, group string, retryLimit int) ([]*operator.Operator, map[uint64]error, error) {
	regions := r.cluster.ScanRegions(startKey, endKey, -1)
	if len(regions) < 1 {
		return nil, nil, errors.New("empty region")
	}
	failures := make(map[uint64]error, len(regions))
	regionMap := make(map[uint64]*core.RegionInfo, len(regions))
	for _, region := range regions {
		regionMap[region.GetID()] = region
	}
	// If there existed any region failed to relocated after retry, add it into unProcessedRegions
	ops, err := r.ScatterRegions(regionMap, failures, group, retryLimit)
	if err != nil {
		return nil, nil, err
	}
	return ops, failures, nil
}

// ScatterRegionsByID directly scatter regions by ScatterRegions
func (r *RegionScatterer) ScatterRegionsByID(regionsID []uint64, group string, retryLimit int) ([]*operator.Operator, map[uint64]error, error) {
	if len(regionsID) < 1 {
		return nil, nil, errors.New("empty region")
	}
	failures := make(map[uint64]error, len(regionsID))
	var regions []*core.RegionInfo
	for _, id := range regionsID {
		region := r.cluster.GetRegion(id)
		if region == nil {
			failures[id] = errors.New(fmt.Sprintf("failed to find region %v", id))
			continue
		}
		regions = append(regions, region)
	}
	regionMap := make(map[uint64]*core.RegionInfo, len(regions))
	for _, region := range regions {
		regionMap[region.GetID()] = region
	}
	// If there existed any region failed to relocated after retry, add it into unProcessedRegions
	ops, err := r.ScatterRegions(regionMap, failures, group, retryLimit)
	if err != nil {
		return nil, nil, err
	}
	return ops, failures, nil
}

// ScatterRegions relocates the regions. If the group is defined, the regions' leader with the same group would be scattered
// in a group level instead of cluster level.
// RetryTimes indicates the retry times if any of the regions failed to relocate during scattering. There will be
// time.Sleep between each retry.
// Failures indicates the regions which are failed to be relocated, the key of the failures indicates the regionID
// and the value of the failures indicates the failure error.
func (r *RegionScatterer) ScatterRegions(regions map[uint64]*core.RegionInfo, failures map[uint64]error, group string, retryLimit int) ([]*operator.Operator, error) {
	if len(regions) < 1 {
		return nil, errors.New("empty region")
	}
	if retryLimit > maxRetryLimit {
		retryLimit = maxRetryLimit
	}
	ops := make([]*operator.Operator, 0, len(regions))
	for currentRetry := 0; currentRetry <= retryLimit; currentRetry++ {
		for _, region := range regions {
			op, err := r.Scatter(region, group)
			failpoint.Inject("scatterFail", func() {
				if region.GetID() == 1 {
					err = errors.New("mock error")
				}
			})
			if err != nil {
				failures[region.GetID()] = err
				continue
			}
			if op != nil {
				ops = append(ops, op)
			}
			delete(regions, region.GetID())
			delete(failures, region.GetID())
		}
		// all regions have been relocated, break the loop.
		if len(regions) < 1 {
			break
		}
		// Wait for a while if there are some regions failed to be relocated
		time.Sleep(typeutil.MinDuration(maxSleepDuration, time.Duration(math.Pow(2, float64(currentRetry)))*initialSleepDuration))
	}
	return ops, nil
}

// Scatter relocates the region. If the group is defined, the regions' leader with the same group would be scattered
// in a group level instead of cluster level.
func (r *RegionScatterer) Scatter(region *core.RegionInfo, group string) (*operator.Operator, error) {
	if !opt.IsRegionReplicated(r.cluster, region) {
		return nil, errors.Errorf("region %d is not fully replicated", region.GetID())
	}

	if region.GetLeader() == nil {
		return nil, errors.Errorf("region %d has no leader", region.GetID())
	}

	if r.cluster.IsRegionHot(region) {
		return nil, errors.Errorf("region %d is hot", region.GetID())
	}

	return r.scatterRegion(region, group), nil
}

func (r *RegionScatterer) scatterRegion(region *core.RegionInfo, group string) *operator.Operator {
	ordinaryFilter := filter.NewOrdinaryEngineFilter(r.name)
	ordinaryPeers := make(map[uint64]*metapb.Peer)
	specialPeers := make(map[string]map[uint64]*metapb.Peer)
	// Group peers by the engine of their stores
	for _, peer := range region.GetPeers() {
		store := r.cluster.GetStore(peer.GetStoreId())
		if ordinaryFilter.Target(r.cluster, store) {
			ordinaryPeers[peer.GetId()] = peer
		} else {
			engine := store.GetLabelValue(filter.EngineKey)
			if _, ok := specialPeers[engine]; !ok {
				specialPeers[engine] = make(map[uint64]*metapb.Peer)
			}
			specialPeers[engine][peer.GetId()] = peer
		}
	}

	targetPeers := make(map[uint64]*metapb.Peer)
	selectedStores := make(map[uint64]struct{})
	scatterWithSameEngine := func(peers map[uint64]*metapb.Peer, context engineContext) {
		for _, peer := range peers {
			candidates := r.selectCandidates(region, peer.GetStoreId(), selectedStores, context)
			newPeer := r.selectStore(group, peer, peer.GetStoreId(), candidates, context)
			targetPeers[newPeer.GetStoreId()] = newPeer
			selectedStores[newPeer.GetStoreId()] = struct{}{}
		}
	}

	scatterWithSameEngine(ordinaryPeers, r.ordinaryEngine)
	// FIXME: target leader only considers the ordinary stores，maybe we need to consider the
	// special engine stores if the engine supports to become a leader. But now there is only
	// one engine, tiflash, which does not support the leader, so don't consider it for now.
	targetLeader := r.selectAvailableLeaderStores(group, targetPeers, r.ordinaryEngine)

	for engine, peers := range specialPeers {
		ctx, ok := r.specialEngines[engine]
		if !ok {
			ctx = newEngineContext(r.ctx, filter.NewEngineFilter(r.name, engine))
			r.specialEngines[engine] = ctx
		}
		scatterWithSameEngine(peers, ctx)
	}

	op, err := operator.CreateScatterRegionOperator("scatter-region", r.cluster, region, targetPeers, targetLeader)
	if err != nil {
		for _, peer := range region.GetPeers() {
			targetPeers[peer.GetStoreId()] = peer
		}
		r.Put(targetPeers, region.GetLeader().GetStoreId(), group)
		log.Debug("fail to create scatter region operator", errs.ZapError(err))
		return nil
	}
	r.Put(targetPeers, targetLeader, group)
	op.SetPriorityLevel(core.HighPriority)
	return op
}

func (r *RegionScatterer) selectCandidates(region *core.RegionInfo, sourceStoreID uint64, selectedStores map[uint64]struct{}, context engineContext) []uint64 {
	sourceStore := r.cluster.GetStore(sourceStoreID)
	if sourceStore == nil {
		log.Error("failed to get the store", zap.Uint64("store-id", sourceStoreID), errs.ZapError(errs.ErrGetSourceStore))
		return nil
	}
	filters := []filter.Filter{
		filter.NewExcludedFilter("scatter-region", nil, selectedStores),
	}
	var scoreGuard filter.Filter
	if r.cluster.IsPlacementRulesEnabled() {
		scoreGuard = filter.NewRuleFitFilter(r.name, r.cluster, region, sourceStoreID)
	} else {
		regionStores := make([]*core.StoreInfo, 0)
		for _, peer := range region.GetPeers() {
			store := r.cluster.GetStore(peer.GetStoreId())
			regionStores = append(regionStores, store)
		}
		scoreGuard = filter.NewDistinctScoreFilter(r.name, r.cluster.GetLocationLabels(), regionStores, sourceStore)
	}
	filters = append(filters, context.filters...)
	filters = append(filters, scoreGuard)
	stores := r.cluster.GetStores()
	candidates := make([]uint64, 0)
	for _, store := range stores {
		if filter.Target(r.cluster, store, filters) && !store.IsBusy() {
			candidates = append(candidates, store.GetID())
		}
	}
	return candidates
}

func (r *RegionScatterer) selectStore(group string, peer *metapb.Peer, sourceStoreID uint64, candidates []uint64, context engineContext) *metapb.Peer {
	if len(candidates) < 1 {
		return peer
	}
	var newPeer *metapb.Peer
	minCount := uint64(math.MaxUint64)
	for _, storeID := range candidates {
		count := context.selectedPeer.Get(storeID, group)
		if count < minCount {
			minCount = count
			newPeer = &metapb.Peer{
				StoreId:   storeID,
				IsLearner: peer.GetIsLearner(),
			}
		}
	}
	// if the source store have the least count, we don't need to scatter this peer
	for _, storeID := range candidates {
		if storeID == sourceStoreID && context.selectedPeer.Get(sourceStoreID, group) <= minCount {
			return peer
		}
	}
	if newPeer == nil {
		return peer
	}
	return newPeer
}

// selectAvailableLeaderStores select the target leader store from the candidates. The candidates would be collected by
// the existed peers store depended on the leader counts in the group level.
func (r *RegionScatterer) selectAvailableLeaderStores(group string, peers map[uint64]*metapb.Peer, context engineContext) uint64 {
	minStoreGroupLeader := uint64(math.MaxUint64)
	id := uint64(0)
	for storeID := range peers {
		if id == 0 {
			id = storeID
		}
		storeGroupLeaderCount := context.selectedLeader.Get(storeID, group)
		if minStoreGroupLeader > storeGroupLeaderCount {
			minStoreGroupLeader = storeGroupLeaderCount
			id = storeID
		}
	}
	return id
}

// Put put the final distribution in the context no matter the operator was created
func (r *RegionScatterer) Put(peers map[uint64]*metapb.Peer, leaderStoreID uint64, group string) {
	ordinaryFilter := filter.NewOrdinaryEngineFilter(r.name)
	// Group peers by the engine of their stores
	for _, peer := range peers {
		storeID := peer.GetStoreId()
		store := r.cluster.GetStore(storeID)
		if ordinaryFilter.Target(r.cluster, store) {
			r.ordinaryEngine.selectedPeer.Put(storeID, group)
		} else {
			engine := store.GetLabelValue(filter.EngineKey)
			r.specialEngines[engine].selectedPeer.Put(storeID, group)
		}
	}
	r.ordinaryEngine.selectedLeader.Put(leaderStoreID, group)
}
