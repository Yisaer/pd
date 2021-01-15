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
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
)

type balanceCopySetScheduler struct {
	*BaseScheduler
	opController *schedule.OperatorController
	conf         *balanceCopySetSchedulerConfig
	filters      []filter.Filter
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

	return nil
}
