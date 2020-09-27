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
	"time"
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
	//sort by GetRegionSize();
	sort.Sort(&stores)
	for _, s := range stores {
		log.Infof("stores %d,cnt=%d,sz=%d", s.GetID(), s.GetRegionCount(), s.GetRegionSize())
	}
	log.Infof("%s.Schedule<%d;Replica=%d> ", s.GetName(),
		cluster.GetReplicaScheduleLimit(), cluster.GetRegionScheduleLimit())
	var fromStore *core.StoreInfo
	//find the max region size store.
	for i := stores.Len() - 1; i > 0; i-- {
		fromStore = stores[i]
		if false == s.checkStore(fromStore) {
			continue
		}
		if fromStore.DownTime() < cluster.GetMaxStoreDownTime() {
			op := s.schedule(cluster, stores, i)
			if op != nil {
				return op
			}
		}
	}
	return nil
}

func (s *balanceRegionScheduler) schedule(cluster opt.Cluster, stores []*core.StoreInfo, fromIdx int) *operator.Operator {
	fromStore := stores[fromIdx]
	log.Infof("%s.Schedule find the fromStore<%d,cnt=%d;sz=%d>(%+v)", s.GetName(),
		fromStore.GetID(), fromStore.GetRegionCount(), fromStore.GetRegionSize(), fromStore.GetMeta())
	var region *core.RegionInfo
	var toStore *core.StoreInfo
	for idx := 0; idx < fromIdx; idx++ {
		toStore = stores[idx]
		if false == s.checkStore(toStore) {
			continue
		}
		//if fromStore.GetRegionCount()-toStore.GetRegionCount() < int(cluster.GetRegionScheduleLimit()) {
		//	//后面的store肯定比当前这个大，所以，当前不满足，后续肯定也满足不了.
		//	break
		//}
		region = s.pickupRegion(fromStore.GetID(), toStore.GetID(), cluster)
		if region != nil {
			if len(region.GetPeers()) < cluster.GetMaxReplicas() {
				log.Warnf("not enough replicas region %d,peers:%d", region.GetID(), len(region.GetPeers()))
				region = nil
				continue
			}
			if fromStore.GetRegionSize()-toStore.GetRegionSize() > 2*region.GetApproximateSize() {
				log.Infof("%s.Schedule from store(%d,sz=%d) find region<%d,sz=%d>(%+v) to store<%d,sz=%d>",
					s.GetName(), fromStore.GetID(), fromStore.GetRegionSize(),
					region.GetID(), region.GetApproximateSize(), region.GetMeta(),
					toStore.GetID(), toStore.GetRegionSize())
				break
			} else {
				//log.Warnf("failed:%d->%d (litter than region %d-%d)", fromStore.GetID(), toStore.GetID(), region.GetID(), region.GetApproximateSize())
				continue
			}
		}
	}
	if region == nil {
		//not found;
		log.Infof("%s.Schedule find region failed.do not schedule", s.GetName())
		return nil
	}
	peer, err := cluster.AllocPeer(toStore.GetID())
	if err != nil {
		log.Errorf("%s.Schedule from(%d)AllocPeer(%d) err:%s", s.GetName(), fromStore.GetID(), toStore.GetID(), err.Error())
		return nil
	}
	log.Infof("%s.Schedule region<%d> schedule (%d)->(%d:%d)", s.GetName(), region.GetID(), fromStore.GetID(), peer.GetStoreId(), peer.GetId())
	//move to;
	op, err := operator.CreateMovePeerOperator("", cluster, region, operator.OpBalance, fromStore.GetID(), toStore.GetID(), peer.GetId())
	if err != nil {
		log.Errorf("%s.Schedule from(%d)CreateMovePeerOperator(%d) err:%s", s.GetName(), fromStore.GetID(), toStore.GetID(), err.Error())
		return nil
	}
	return op
}

func (s *balanceRegionScheduler) pickupRegion(storeId, filterId uint64, cluster opt.Cluster) *core.RegionInfo {
	log.Debugf("pickupRegion storeId(%d) filterId(%d)", storeId, filterId)
	var result *core.RegionInfo
	start, end := []byte(""), []byte("")
	//First it will try to select a pending
	cluster.GetPendingRegionsWithLock(storeId, func(container core.RegionsContainer) {
		result = container.RandomRegion(start, end)
	})
	//If there isn’t a pending region, it will try to find a follower region
	if result != nil {
		log.Debugf("find region<%d,sz=%d,peers:%d> in GetPendingRegionsWithLock", result.GetID(), result.GetApproximateSize(), len(result.GetPeers()))
		if nil == result.GetStorePeer(filterId) {
			return result
		} else {
			result = nil
		}
	}
	cluster.GetFollowersWithLock(storeId, func(container core.RegionsContainer) {
		result = container.RandomRegion(start, end)
	})
	//If it still cannot pick out one region, it will try to pick leader regions
	if result != nil {
		log.Debugf("find region<%d,sz=%d,peers:%d> in GetFollowersWithLock", result.GetID(), result.GetApproximateSize(), len(result.GetPeers()))
		if nil == result.GetStorePeer(filterId) {
			return result
		} else {
			result = nil
		}
	}
	cluster.GetLeadersWithLock(storeId, func(container core.RegionsContainer) {
		result = container.RandomRegion(start, end)
	})
	//Finally it will select out the region to move, or the Scheduler will try the next store which has smaller region size until all stores will have been tried
	if result != nil {
		log.Debugf("find region<%d,sz=%d,peers:%d> in GetLeadersWithLock", result.GetID(), result.GetApproximateSize(), len(result.GetPeers()))
		if nil == result.GetStorePeer(filterId) {
			return result
		} else {
			result = nil
		}
	}
	return result
}

func (s *balanceRegionScheduler) checkStore(store *core.StoreInfo) bool {
	if !store.IsUp() {
		return false
	}
	diff := time.Now().Sub(store.GetLastHeartbeatTS())
	if diff.Nanoseconds() > s.GetMinInterval().Nanoseconds() {
		return false
	}
	return true
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
