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
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/tikv/pd/pkg/copysets"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"math/rand"
	"sort"
)

func init() {
	schedule.RegisterScheduler("balance-copyset-scheduler", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &balanceCopySetSchedulerConfig{Name: "balance-copyset-scheduler"}
		return NewBalanceCopySetScheduler(opController, conf), nil
	})
}

type balanceCopySetScheduler struct {
	*BaseScheduler
	opController *schedule.OperatorController
	conf         *balanceCopySetSchedulerConfig
	filters      []filter.Filter
}

func NewBalanceCopySetScheduler(opController *schedule.OperatorController, conf *balanceCopySetSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	scheduler := &balanceCopySetScheduler{
		BaseScheduler: base,
		conf:          conf,
		opController:  opController,
	}
	scheduler.filters = []filter.Filter{
		&filter.StoreStateFilter{ActionScope: scheduler.GetName(), MoveRegion: true},
	}
	return scheduler
}

type balanceCopySetSchedulerConfig struct {
	Name string `json:"name"`
}

func (s *balanceCopySetScheduler) GetName() string {
	return s.conf.Name
}

func (s *balanceCopySetScheduler) GetType() string {
	return BalanceRegionType
}

func (s *balanceCopySetScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *balanceCopySetScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion)-s.opController.OperatorCount(operator.OpMerge) < cluster.GetOpts().GetRegionScheduleLimit()
}

// TODO: implement balanceCopySetScheduler Schedule
func (s *balanceCopySetScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	css := cluster.GetCopySets()
	stores := cluster.GetStores()
	opts := cluster.GetOpts()
	stores = filter.SelectSourceStores(stores, s.filters, opts)
	opInfluence := s.opController.GetOpInfluence(cluster)
	kind := core.NewScheduleKind(core.RegionKind, core.BySize)
	storesScore := make(map[uint64]float64, 0)
	for _, store := range stores {
		op := opInfluence.GetStoreInfluence(store.GetID()).ResourceProperty(kind)
		score := store.RegionScore(opts.GetRegionScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), op, -1)
		storesScore[store.GetID()] = score
	}
	cssScore := make([]copysetScore, 0, 0)
	for _, cs := range css {
		n1, n2, n3 := cs.GetNodesID()
		score := float64(0)
		if storesScore[n1] > score {
			score = storesScore[n1]
		}
		if storesScore[n2] > score {
			score = storesScore[n2]
		}
		if storesScore[n3] > score {
			score = storesScore[n3]
		}
		cssScore = append(cssScore, copysetScore{
			cs:    cs,
			sign:  cs.Sign(),
			score: score,
		})
	}
	sort.Slice(cssScore, func(i, j int) bool {
		return cssScore[i].score > cssScore[j].score
	})
	for _, source := range cssScore {
		sourceCS := source.cs
		s1, s2, s3 := sourceCS.GetNodesID()
		regionsList := make([][]*core.RegionInfo, 0)
		regionsList = append(regionsList, cluster.GetStoreRegions(s1))
		//debug(s1, cluster.GetStoreRegions(s1))
		regionsList = append(regionsList, cluster.GetStoreRegions(s2))
		//debug(s2, cluster.GetStoreRegions(s2))
		regionsList = append(regionsList, cluster.GetStoreRegions(s3))
		//debug(s3, cluster.GetStoreRegions(s3))
		commonRegions := findCommonRegions(regionsList)
		if len(commonRegions) < 1 {
			continue
		}
		for i := 0; i < balanceRegionRetryLimit; i++ {
			selectRegion := commonRegions[rand.Intn(len(commonRegions))]
			if op := s.transferCopySet(cluster, selectRegion, source, cssScore); op != nil {
				return []*operator.Operator{op}
			}
		}
	}
	return nil
}

func (s *balanceCopySetScheduler) transferCopySet(cluster opt.Cluster, region *core.RegionInfo, sourceCSSore copysetScore, csScore []copysetScore) *operator.Operator {
	sort.Slice(csScore, func(i, j int) bool {
		return csScore[i].score < csScore[j].score
	})
	sourceCS := sourceCSSore.cs
	for _, targetCS := range csScore {
		if targetCS.sign == sourceCS.Sign() {
			log.Warn(fmt.Sprintf("targetCS equal to sourceCS %v", sourceCS.Sign()))
			continue
		}
		op, err := operator.CreateMoveCopySetOperator("balance-copyset-scheduler", cluster, region, operator.OpRegion, targetCS.cs)
		if err != nil {
			log.Errorf(err.Error())
			continue
		}
		return op
	}
	return nil
}

type copysetScore struct {
	cs    copysets.CopySet
	score float64
	sign  string
}

func findCommonRegions(regionsList [][]*core.RegionInfo) []*core.RegionInfo {
	regionCnt := make(map[uint64]int, 0)
	commonRegions := make([]*core.RegionInfo, 0)
	for _, regions := range regionsList {
		for _, region := range regions {
			if _, ok := regionCnt[region.GetID()]; !ok {
				regionCnt[region.GetID()] = 0
			}
			regionCnt[region.GetID()] = regionCnt[region.GetID()] + 1
			cnt := regionCnt[region.GetID()]
			if cnt >= len(regionsList) {
				commonRegions = append(commonRegions, region)
			}
		}
	}
	return commonRegions
}

func debug(storeID uint64, regions []*core.RegionInfo) {
	var ids []uint64
	ids = make([]uint64, 0, 0)
	for _, region := range regions {
		ids = append(ids, region.GetID())
	}
	fmt.Println(storeID, ids)
}
