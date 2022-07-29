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

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores := cluster.GetStores()
	var suitableStores []*core.StoreInfo
	for _, store := range stores {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, store)
		}
	}
	if len(suitableStores) == 0 {
		return nil
	}
	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
	})
	var region *core.RegionInfo
	var fromStore *core.StoreInfo
	for _, store := range suitableStores {
		cb := func(rc core.RegionsContainer) {
			region = rc.RandomRegion(nil, nil)
		}
		cluster.GetPendingRegionsWithLock(store.GetID(), cb)
		if region != nil {
			fromStore = store
			break
		}
		cluster.GetFollowersWithLock(store.GetID(), cb)
		if region != nil {
			fromStore = store
			break
		}
		cluster.GetLeadersWithLock(store.GetID(), cb)
		if region != nil {
			fromStore = store
			break
		}
	}
	if region == nil || fromStore == nil {
		return nil
	}
	if len(region.GetStoreIds()) < cluster.GetMaxReplicas() {
		return nil
	}
	var toStore *core.StoreInfo
	for i := len(suitableStores) - 1; i >= 0; i-- {
		if _, ok := region.GetStoreIds()[suitableStores[i].GetID()]; !ok {
			toStore = suitableStores[i]
			break
		}
	}
	if toStore == nil {
		return nil
	}
	if fromStore.GetRegionSize()-toStore.GetRegionSize() < 2*region.GetApproximateSize() {
		return nil
	}
	peer, _ := cluster.AllocPeer(toStore.GetID())
	operate, _ := operator.CreateMovePeerOperator("", cluster, region, operator.OpBalance, fromStore.GetID(), toStore.GetID(), peer.Id)
	return operate
}
