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
	"fmt"
	"sort"
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
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
	scheduleOnce := func() (*operator.Operator, error) {
		totalStores := cluster.GetStores()
		stores := make([]*core.StoreInfo, 0)
		for _, store := range totalStores {
			if store.GetMeta().State == metapb.StoreState_Up && time.Since(store.GetLastHeartbeatTS()) < cluster.GetMaxStoreDownTime() {
				stores = append(stores, store)
			}
		}
		if len(stores) <= cluster.GetMaxReplicas() {
			return nil, nil
		}
		sort.Slice(stores, func(i, j int) bool {
			return stores[i].GetRegionSize() > stores[j].GetRegionSize()
		})
		sourceStore := stores[0]
		var sourceRegion *core.RegionInfo
		callback := func(rc core.RegionsContainer) {
			sourceRegion = rc.RandomRegion(nil, nil)
		}
		cluster.GetPendingRegionsWithLock(sourceStore.GetID(), callback)
		if sourceRegion == nil {
			cluster.GetFollowersWithLock(sourceStore.GetID(), callback)
		}
		if sourceRegion == nil {
			cluster.GetLeadersWithLock(sourceStore.GetID(), callback)
		}
		if sourceRegion == nil || sourceRegion.GetMeta() == nil {
			return nil, fmt.Errorf("no region found in store %v", sourceStore.GetID())
		}
		le := len(stores)
		var targetStore *core.StoreInfo
		for i := le - 1; i > 0; i-- {
			storeId := stores[i].GetID()
			found := false
			for _, peer := range sourceRegion.GetMeta().Peers {
				if peer.GetStoreId() == storeId {
					found = true
					break
				}
			}
			if !found {
				targetStore = stores[i]
				break
			}
		}
		if targetStore == nil {
			return nil, fmt.Errorf("no available store found for region %v", sourceRegion.GetMeta().GetId())
		}
		if sourceStore.GetRegionSize()-targetStore.GetRegionSize() < 2*sourceRegion.GetApproximateSize() {
			return nil, fmt.Errorf("difference between store %v and %v is too small", sourceStore.GetID(), targetStore.GetID())
		}
		peerId, err := cluster.AllocPeer(targetStore.GetID())
		if err != nil {
			return nil, fmt.Errorf("failed to allocate a peer ID of store %v", targetStore.GetID())
		}
		ops, err := operator.CreateMovePeerOperator("balance-lw",
			cluster, sourceRegion, operator.OpBalance, sourceStore.GetID(), targetStore.GetID(), peerId.Id)
		if err != nil {
			return nil, fmt.Errorf("fail to create operator: %v", err)
		}
		return ops, nil
	}

	for i := 0; i < balanceRegionRetryLimit; i++ {
		op, err := scheduleOnce()
		if err != nil {
			continue
		}
		return op
	}
	return nil
}
