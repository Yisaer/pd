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
	"context"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rpcconfig "github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rpc"
	"github.com/tikv/pd/pkg/client"
	"github.com/tikv/pd/server/schedule/opt"
)

type RegionSplitter struct {
	cluster opt.Cluster
}

func (r *RegionSplitter) SplitRegions(ctx context.Context, splitKeys [][]byte, retryLimit int) error {
	unprocessedKeys := splitKeys
	for i := 0; i < retryLimit; i++ {
		newRegions, unprocessedKeys := r.splitRegions(ctx, unprocessedKeys)
		if len(unprocessedKeys) < 1 {
			break
		}
	}

	return nil
}

func (r *RegionSplitter) splitRegions(ctx context.Context, splitKeys [][]byte) (map[uint64]struct{}, [][]byte) {
	conf := rpcconfig.DefaultRPC()
	sender := client.NewRegionRequestSender(&conf)
	newRegions := make(map[uint64]struct{}, len(splitKeys))

	//TODO: support batch limit
	groupKeys, unProcessedKeys := r.groupKeysByRegion(splitKeys)
	for regionID, keys := range groupKeys {
		region := r.cluster.GetRegion(regionID)
		// TODO: assert region is not nil
		// TODO: assert leader exists
		// TODO: assert store exists
		// TODO: is region replicated
		leaderStore := r.cluster.GetStore(region.GetLeader().StoreId)
		req := &rpc.Request{
			Type: rpc.CmdSplitRegion,
			SplitRegion: &kvrpcpb.SplitRegionRequest{
				SplitKeys: keys,
			},
		}
		resp, err := sender.SendReq(ctx, client.NewRegionRequest(req, region, leaderStore, conf.ReadTimeoutShort))
		if err != nil {
			for _, key := range keys {
				unProcessedKeys = append(unProcessedKeys, key)
			}
		}
		if resp == nil || resp.SplitRegion == nil {
			for _, key := range keys {
				unProcessedKeys = append(unProcessedKeys, key)
			}
		}
		for _, newRegion := range resp.SplitRegion.Regions {
			newRegions[newRegion.Id] = struct{}{}
		}
	}
	return newRegions, unProcessedKeys
}

// GroupKeysByRegion separates keys into groups by their belonging Regions.
// Specially it also returns the first key's region which may be used as the
// 'PrimaryLockKey' and should be committed ahead of others.
// filter is used to filter some unwanted keys.
func (r *RegionSplitter) groupKeysByRegion(keys [][]byte) (map[uint64][][]byte, [][]byte) {
	unProcessedKeys := make([][]byte, 0, len(keys))
	groupKeys := make(map[uint64][][]byte, len(keys))
	for _, key := range keys {
		region := r.cluster.GetRegionByKey(key)
		if region == nil {
			unProcessedKeys = append(unProcessedKeys, key)
			continue
		}
		group, ok := groupKeys[region.GetID()]
		if !ok {
			groupKeys[region.GetID()] = [][]byte{}
		}
		groupKeys[region.GetID()] = append(group, key)
	}
	return groupKeys, unProcessedKeys
}
