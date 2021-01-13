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
	cs := c.selectCopyset(region, css)
	return c.generateOperator(region, cs)
}

// TODO: implement selectCopyset
func (c *CopySetChecker) selectCopyset(region *core.RegionInfo, css []copysets.CopySet) copysets.CopySet {
	return copysets.CopySet{}
}

// TODO: implement generateOperator
func (c *CopySetChecker) generateOperator(region *core.RegionInfo, cs copysets.CopySet) *operator.Operator {
	return &operator.Operator{}
}
