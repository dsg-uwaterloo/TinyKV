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
	"github.com/pingcap-incubator/tinykv/log"
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

//movePeer: addPeer/transferLeader/removePeer;
//schedule目的：store上有n多peer，可能会导致store上多peer不平衡;通过schedule，movePeer，可以平衡store上多peer.
//			store上多peer为啥会不平衡？因为region的split导致的.
//TODO: check 是否同时发生多个 schedule.
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	//1-Then sort them according to their region size
	//	Then the Scheduler tries to find regions to move from the store with the biggest region size
	stores := (SortStores)(cluster.GetStores())
	if stores.Len() <= 1 {
		return nil
	}
	sort.Sort(&stores)

	toIdx := 0
	var toStore *core.StoreInfo
	var fromStore *core.StoreInfo
	for i := 0; i < stores.Len(); i++ {
		if i == stores.Len()-1 {
			//如果到最后一个了，那么就没有必要比较了，没有满足条件到.
			return nil
		}
		toStore = stores[i]
		if toStore.DownTime() < cluster.GetMaxStoreDownTime() {
			toIdx = i
			break
		}
	}
	var region *core.RegionInfo
	for lastIdx := stores.Len() - 1; lastIdx > toIdx; lastIdx-- {
		fromStore = stores[lastIdx]
		if fromStore.GetRegionSize()-toStore.GetRegionSize() < int64(cluster.GetRegionScheduleLimit()) {
			break
		}
		region = s.pickupRegion(fromStore.GetID(), toStore.GetID(), cluster)
		if region != nil {
			break
		}
	}
	if region == nil {
		//not found;
		return nil
	}
	peer, err := cluster.AllocPeer(toStore.GetID())
	if err != nil {
		log.Errorf("from(%d)AllocPeer(%d) err:%s", fromStore.GetID(), toStore.GetID(), err.Error())
		return nil
	}
	//move to;
	op, err := operator.CreateMovePeerOperator("", cluster, region, operator.OpBalance, fromStore.GetID(), toStore.GetID(), peer.GetId())
	if err != nil {
		log.Errorf("from(%d)CreateMovePeerOperator(%d) err:%s", fromStore.GetID(), toStore.GetID(), err.Error())
		return nil
	}
	return op
}

func (s *balanceRegionScheduler) pickupRegion(from, to uint64, cluster opt.Cluster) *core.RegionInfo {
	var result *core.RegionInfo
	start, end := []byte(""), []byte("")
	//First it will try to select a pending
	cluster.GetPendingRegionsWithLock(from, func(container core.RegionsContainer) {
		result = container.RandomRegion(start, end)
	})
	//If there isn’t a pending region, it will try to find a follower region
	if result != nil {
		if nil == result.GetStorePeer(to) {
			return result
		} else {
			result = nil
		}
	}
	cluster.GetFollowersWithLock(from, func(container core.RegionsContainer) {
		result = container.RandomRegion(start, end)
	})
	//If it still cannot pick out one region, it will try to pick leader regions
	if result != nil {
		if nil == result.GetStorePeer(to) {
			return result
		} else {
			result = nil
		}
	}
	cluster.GetLeadersWithLock(from, func(container core.RegionsContainer) {
		result = container.RandomRegion(start, end)
	})
	//Finally it will select out the region to move, or the Scheduler will try the next store which has smaller region size until all stores will have been tried
	if result != nil {
		if nil == result.GetStorePeer(to) {
			return result
		} else {
			result = nil
		}
	}
	return result
}

type SortStores []*core.StoreInfo

func (s SortStores) Less(i, j int) bool {
	istore, jstore := s[i], s[j]
	return istore.GetRegionSize() < jstore.GetRegionSize()
}

func (s SortStores) Len() int {
	return len(s)
}

func (s SortStores) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
