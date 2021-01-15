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
	"math/rand"

	"github.com/pingcap/kvproto/pkg/metapb"
	log "github.com/sirupsen/logrus"
	"github.com/tikv/pd/pkg/copysets"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
)

// CopySetChecker fix/improve region by copySets
type CopySetChecker struct {
	cluster opt.Cluster
	name    string
}

func NewCopySetChecker(cluster opt.Cluster) *CopySetChecker {
	return &CopySetChecker{
		cluster: cluster,
		name:    "copyset-checker",
	}
}

func (c *CopySetChecker) Check(region *core.RegionInfo) *operator.Operator {
	css := c.cluster.GetCopySets()
	if css == nil {
		return nil
	}
	cs := c.selectCopyset(region, css)
	op, err := c.generateOperator(region, cs)
	if err != nil {
		log.Error(err)
		return nil
	}
	return op
}

// TODO: implement selectCopyset
func (c *CopySetChecker) selectCopyset(region *core.RegionInfo, css []copysets.CopySet) copysets.CopySet {
	tarCS := copysets.CopySet{}
	highestScore := 0
	for _, cs := range css {
		if cs.IsRegionSatisfied(region) {
			tarCS = cs
			break
		}
		score := cs.CalCopysetDistScore(region)
		if score > highestScore {
			highestScore = score
			tarCS = cs
		}
	}
	return tarCS
}

// TODO: implement generateOperator
func (c *CopySetChecker) generateOperator(region *core.RegionInfo, cs copysets.CopySet) (*operator.Operator, error) {
	if cs.IsRegionSatisfied(region) {
		return nil, nil
	}
	storeCandidates := cs.StoresCandidate(region)
	if len(storeCandidates) < 1 {
		return nil, nil
	}

	for _, peer := range region.GetFollowers() {
		if cs.IsStoreInCopySet(peer.StoreId) {
			continue
		}
		selectStore := storeCandidates[rand.Intn(len(storeCandidates))]
		newPeer := &metapb.Peer{StoreId: selectStore, Role: peer.Role}
		return operator.CreateMovePeerOperator("copyset-checker", c.cluster, region, operator.OpRegion, peer.StoreId, newPeer)
	}

	if !cs.IsStoreInCopySet(region.GetLeader().StoreId) {
		selectStore := storeCandidates[rand.Intn(len(storeCandidates))]
		destPeer := &metapb.Peer{StoreId: selectStore}
		return operator.CreateMoveLeaderOperator("copyset-checker", c.cluster, region, operator.OpLeader, region.GetLeader().StoreId, destPeer)
	}
	return nil, nil
}
