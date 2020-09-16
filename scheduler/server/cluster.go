// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"path"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/logutil"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/typeutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/id"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap/errcode"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var (
	backgroundJobInterval      = time.Minute
	defaultChangedRegionsLimit = 10000
)

// RaftCluster is used for cluster config management.
// Raft cluster key format:
// cluster 1 -> /1/raft, value is metapb.Cluster
// cluster 2 -> /2/raft
// For cluster 1
// store 1 -> /1/raft/s/1, value is metapb.Store
// region 1 -> /1/raft/r/1, value is metapb.Region
type RaftCluster struct {
	sync.RWMutex
	ctx context.Context

	s *Server

	running bool

	clusterID   uint64
	clusterRoot string

	// cached cluster info
	core    *core.BasicCluster
	meta    *metapb.Cluster
	opt     *config.ScheduleOption
	storage *core.Storage
	id      id.Allocator

	prepareChecker *prepareChecker

	coordinator *coordinator

	wg   sync.WaitGroup
	quit chan struct{}
}

// ClusterStatus saves some state information
type ClusterStatus struct {
	RaftBootstrapTime time.Time `json:"raft_bootstrap_time,omitempty"`
	IsInitialized     bool      `json:"is_initialized"`
}

func newRaftCluster(ctx context.Context, s *Server, clusterID uint64) *RaftCluster {
	return &RaftCluster{
		ctx:         ctx,
		s:           s,
		running:     false,
		clusterID:   clusterID,
		clusterRoot: s.getClusterRootPath(),
	}
}

func (c *RaftCluster) loadClusterStatus() (*ClusterStatus, error) {
	bootstrapTime, err := c.loadBootstrapTime()
	if err != nil {
		return nil, err
	}
	var isInitialized bool
	if bootstrapTime != typeutil.ZeroTime {
		isInitialized = c.isInitialized()
	}
	return &ClusterStatus{
		RaftBootstrapTime: bootstrapTime,
		IsInitialized:     isInitialized,
	}, nil
}

func (c *RaftCluster) isInitialized() bool {
	if c.core.GetRegionCount() > 1 {
		return true
	}
	region := c.core.SearchRegion(nil)
	return region != nil &&
		len(region.GetVoters()) >= int(c.s.GetReplicationConfig().MaxReplicas) &&
		len(region.GetPendingPeers()) == 0
}

// loadBootstrapTime loads the saved bootstrap time from etcd. It returns zero
// value of time.Time when there is error or the cluster is not bootstrapped
// yet.
func (c *RaftCluster) loadBootstrapTime() (time.Time, error) {
	var t time.Time
	data, err := c.s.storage.Load(c.s.storage.ClusterStatePath("raft_bootstrap_time"))
	if err != nil {
		return t, err
	}
	if data == "" {
		return t, nil
	}
	return typeutil.ParseTimestamp([]byte(data))
}

func (c *RaftCluster) initCluster(id id.Allocator, opt *config.ScheduleOption, storage *core.Storage) {
	c.core = core.NewBasicCluster()
	c.opt = opt
	c.storage = storage
	c.id = id
	c.prepareChecker = newPrepareChecker()
}

func (c *RaftCluster) start() error {
	c.Lock()
	defer c.Unlock()

	if c.running {
		log.Warn("raft cluster has already been started")
		return nil
	}

	c.initCluster(c.s.idAllocator, c.s.scheduleOpt, c.s.storage)
	cluster, err := c.loadClusterInfo()
	if err != nil {
		return err
	}
	if cluster == nil {
		return nil
	}

	c.coordinator = newCoordinator(c.ctx, cluster, c.s.hbStreams)
	c.quit = make(chan struct{})

	c.wg.Add(2)
	//NOTICE-3C:create scheduler-->region process(add/remove node and so on).
	//			one scheduler one goroutine.
	go c.runCoordinator()
	//NOTICE-3C:check stores status,for send/receive msg;
	go c.runBackgroundJobs(backgroundJobInterval)
	c.running = true

	return nil
}

// Return nil if cluster is not bootstrapped.
func (c *RaftCluster) loadClusterInfo() (*RaftCluster, error) {
	c.meta = &metapb.Cluster{}
	ok, err := c.storage.LoadMeta(c.meta)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	start := time.Now()
	if err := c.storage.LoadStores(c.core.PutStore); err != nil {
		return nil, err
	}
	log.Info("load stores",
		zap.Int("count", c.getStoreCount()),
		zap.Duration("cost", time.Since(start)),
	)
	return c, nil
}

func (c *RaftCluster) runBackgroundJobs(interval time.Duration) {
	defer logutil.LogPanic()
	defer c.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.quit:
			log.Info("background jobs has been stopped")
			return
		case <-ticker.C:
			c.checkStores()
		}
	}
}

func (c *RaftCluster) runCoordinator() {
	defer logutil.LogPanic()
	defer c.wg.Done()
	defer func() {
		c.coordinator.wg.Wait()
		log.Info("coordinator has been stopped")
	}()
	c.coordinator.run()
	<-c.coordinator.ctx.Done()
	log.Info("coordinator is stopping")
}

func (c *RaftCluster) stop() {
	c.Lock()

	if !c.running {
		c.Unlock()
		return
	}

	c.running = false

	close(c.quit)
	c.coordinator.stop()
	c.Unlock()
	c.wg.Wait()
}

func (c *RaftCluster) isRunning() bool {
	c.RLock()
	defer c.RUnlock()
	return c.running
}

// GetOperatorController returns the operator controller.
func (c *RaftCluster) GetOperatorController() *schedule.OperatorController {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.opController
}

// GetHeartbeatStreams returns the heartbeat streams.
func (c *RaftCluster) GetHeartbeatStreams() *heartbeatStreams {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.hbStreams
}

// GetCoordinator returns the coordinator.
func (c *RaftCluster) GetCoordinator() *coordinator {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator
}

// handleStoreHeartbeat updates the store status.
func (c *RaftCluster) handleStoreHeartbeat(stats *schedulerpb.StoreStats) error {
	c.Lock()
	defer c.Unlock()

	storeID := stats.GetStoreId()
	store := c.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}
	newStore := store.Clone(core.SetStoreStats(stats), core.SetLastHeartbeatTS(time.Now()))
	c.core.PutStore(newStore)
	return nil
}

// processRegionHeartbeat updates the region information.
func (c *RaftCluster) processRegionHeartbeat(region *core.RegionInfo) error {
	// Your Code Here (3C).
	//copy from kv/test_raftstore/scheduler.go
	log.Debug("processRegionHeartbeat", zap.String("region", RegionBase2Str(region.GetMeta())))
	if err := c.handleHeartbeatVersion(region); err != nil {
		log.Error("processRegionHeartbeat", zap.String("handleHeartbeatVersion", err.Error()))
		return err
	}
	if err := c.handleHeartbeatConfVersion(region); err != nil {
		log.Error("processRegionHeartbeat", zap.String("handleHeartbeatConfVersion", err.Error()))
		return err
	}
	c.handleRegion(region)
	return nil
}

func (c *RaftCluster) handleRegion(regionInfo *core.RegionInfo) error {
	searchRegion := c.core.SearchRegion(regionInfo.GetStartKey())
	if searchRegion == nil {
		err := c.putRegion(regionInfo)
		if err != nil {
			return err
		}
		leader := regionInfo.GetLeader()
		c.core.Stores.SetLeaderCount(leader.GetStoreId(), c.core.GetStoreLeaderCount(leader.GetStoreId()))
		log.Info("handleRegion",
			zap.Uint64("region", regionInfo.GetID()),
			zap.Uint64("leader", leader.GetId()),
			zap.Int("leaderCount", c.core.GetStoreLeaderCount(leader.GetStoreId())))
		return err
	}
	fromPeers := slice2map(regionInfo.GetPeers())
	fromPending := slice2map(regionInfo.GetPendingPeers())
	//
	var opts []core.RegionCreateOption
	//1、pending处理：
	//	1.1、old的pending已经这from的peer中了，那么说明已经添加成功，那么就可以remove了.
	//	1.2、将from的pending加入进去.
	var pending []*metapb.Peer
	for _, p := range searchRegion.GetPendingPeers() {
		_, ok := fromPeers[p.GetId()]
		if !ok {
			pending = append(pending, p)
			//如果已经在pending中了，那么就不需要重复添加了，直接过滤掉。
			_, ok = fromPending[p.GetId()]
			if ok {
				delete(fromPending, p.GetId())
			}
		}
	}
	for _, p := range fromPending {
		pending = append(pending, p)
	}
	if len(pending) > 0 {
		opts = append(opts, core.WithPendingPeers(pending))
		log.Debug("handleRegion", zap.String("PendingPeers", fmt.Sprintf("%+v", pending)))
	}
	//2、peer处理:直接以新的peer为准.
	if len(regionInfo.GetPeers()) == 0 {
		log.Error("handleRegion", zap.String("peers", fmt.Sprintf("no peers.")))
		return nil
	}
	opts = append(opts, core.SetPeers(regionInfo.GetPeers()))
	//3、set leader;
	if regionInfo.GetLeader().GetId() != searchRegion.GetLeader().GetId() {
		leader := regionInfo.GetLeader()
		opts = append(opts, core.WithLeader(leader))
		log.Info("handleRegion", zap.Uint64("oldLeader", searchRegion.GetLeader().GetId()),
			zap.Uint64("Leader", leader.GetId()))
		c.core.Stores.SetLeaderCount(leader.GetStoreId(), c.core.GetStoreLeaderCount(leader.GetStoreId()))
	}
	//4、set Approximate
	if regionInfo.GetApproximateSize() != searchRegion.GetApproximateSize() {
		if regionInfo.GetApproximateSize() < searchRegion.GetApproximateSize() {
			log.Fatal(fmt.Sprintf("handleRegion region-%d ApproximateSize(%d);searchRegion-%d ApproximateSize(%d)",
				regionInfo.GetID(), regionInfo.GetApproximateSize(), searchRegion.GetID(), searchRegion.GetApproximateSize()))
		}
		opts = append(opts, core.SetApproximateSize(regionInfo.GetApproximateSize()))
		log.Debug("handleRegion", zap.Int64("ApproximateSize", regionInfo.GetApproximateSize()))
	}
	c.core.PutRegion(searchRegion.Clone(opts...))
	leader := regionInfo.GetLeader()

	log.Info("handleRegion", zap.Uint64("region", regionInfo.GetID()),
		zap.Uint64("leader", leader.GetId()), zap.Uint64("oldLeader", searchRegion.GetLeader().GetId()),
		zap.Int("leaderCount", c.core.GetStoreLeaderCount(leader.GetStoreId())),
		zap.String("Options", fmt.Sprintf("%+v", opts)))
	log.Info("handleRegion", zap.String("region", Region2Str(regionInfo.GetMeta())),
		zap.String("region", Region2Str(regionInfo.GetMeta())))
	return nil
}

//region split;
func (c *RaftCluster) handleHeartbeatVersion(regionInfo *core.RegionInfo) error {
	region := regionInfo.GetMeta()
	if engine_util.ExceedEndKey(region.GetStartKey(), region.GetEndKey()) {
		panic(fmt.Sprintf("%d start key(%s) > end key(%s)", region.GetId(),
			string(region.GetStartKey()), string(region.GetEndKey())))
	}

	for {
		searchRegion := c.core.SearchRegion(region.GetStartKey())
		if searchRegion == nil {
			c.putRegion(regionInfo)
			log.Debug("handleHeartbeatVersion ", zap.Uint64("putRegion", region.GetId()))
			//新增region，需要初始化设置下 .
			leader := regionInfo.GetLeader()
			c.core.Stores.SetLeaderCount(leader.GetStoreId(), c.core.GetStoreLeaderCount(leader.GetStoreId()))
			c.setRegionCount(regionInfo.GetPeers())
			return nil
		} else {
			if bytes.Equal(searchRegion.GetStartKey(), region.GetStartKey()) &&
				bytes.Equal(searchRegion.GetEndKey(), region.GetEndKey()) {
				// the two regions' range are same, must check epoch
				if util.IsEpochStale(region.RegionEpoch, searchRegion.GetMeta().GetRegionEpoch()) {
					return errors.New(fmt.Sprintf("epoch is stale1;in=%s;search=%s.", region.GetRegionEpoch(), searchRegion.GetRegionEpoch()))
				}
				if searchRegion.GetMeta().GetRegionEpoch().GetVersion() < region.RegionEpoch.Version {
					c.core.RemoveRegion(searchRegion)
					c.putRegion(regionInfo)
					log.Debug("handleHeartbeatVersion ", zap.Uint64("RemoveRegion", searchRegion.GetID()), zap.Uint64("putRegion", region.GetId()))
				} else {
					log.Debug("handleHeartbeatVersion", zap.Uint64("run here 4", region.GetId()))
				}
				return nil
			}
			//log.TestLog("handleHeartbeatVersion-1 : %s,%+v;%s,%+v",
			//	rkeys2str("search", searchRegion), searchRegion.GetRegionEpoch(),
			//	rkeys2str("region", region), region.GetRegionEpoch())
			if engine_util.ExceedEndKey(searchRegion.GetStartKey(), region.GetEndKey()) {
				// No range covers [start, end) now, insert directly.
				c.putRegion(regionInfo)
				log.Debug("handleHeartbeatVersion ", zap.Uint64("putRegion", region.GetId()))
				return nil
			} else {
				// overlap, remove old, insert new.
				// E.g, 1 [a, c) -> 1 [a, b) + 2 [b, c), either new 1 or 2 reports, the region
				// is overlapped with origin [a, c).
				if region.GetRegionEpoch().GetVersion() <= searchRegion.GetRegionEpoch().GetVersion() {
					return errors.New(fmt.Sprintf("epoch is stale2;in=%s;search=%s.", region.GetRegionEpoch(), searchRegion.GetRegionEpoch()))
				}
				c.core.RemoveRegion(searchRegion)
				log.Debug("handleHeartbeatVersion ", zap.Uint64("RemoveRegion", searchRegion.GetID()))
			}
		}
	}
	return nil
}

//confChange:add/remove.
func (c *RaftCluster) handleHeartbeatConfVersion(regionInfo *core.RegionInfo) error {
	region := regionInfo.GetMeta()
	searchRegion, _ := c.GetRegionByKey(region.GetStartKey())
	if searchRegion == nil {
		return nil
	}
	if util.IsEpochStale(region.GetRegionEpoch(), searchRegion.GetRegionEpoch()) {
		return errors.New("epoch is stale")
	}

	regionPeerLen := len(region.GetPeers())
	searchRegionPeerLen := len(searchRegion.GetPeers())

	if region.RegionEpoch.ConfVer > searchRegion.RegionEpoch.ConfVer {
		// If ConfVer changed, TinyKV has added/removed one peer already.
		// So scheduler and TinyKV can't have same peer count and can only have
		// only one different peer.
		log.Debug(fmt.Sprintf("epoch is stale1;in=%s;search=%s.", region.GetRegionEpoch(), searchRegion.GetRegionEpoch()))
		//peer changed,ConfVer must be changed.
		var changePeers []*metapb.Peer
		if searchRegionPeerLen > regionPeerLen {
			//remove nodes;
			if searchRegionPeerLen-regionPeerLen != 1 {
				panic("should only one conf change")
			}
			diff := GetDiffPeers(searchRegion, region)
			if len(diff) != 1 {
				panic("should only one different peer")
			} else {
				changePeers = append(changePeers, diff[0])
			}
			if len(GetDiffPeers(region, searchRegion)) != 0 {
				panic("should include all peers")
			}
		} else if searchRegionPeerLen < regionPeerLen {
			//add nodes;
			if regionPeerLen-searchRegionPeerLen != 1 {
				panic("should only one conf change")
			}
			diff := GetDiffPeers(region, searchRegion)
			if len(diff) != 1 {
				panic("should only one different peer")
			} else {
				changePeers = append(changePeers, diff[0])
			}
			if len(GetDiffPeers(searchRegion, region)) != 0 {
				panic("should include all peers")
			}
		} else {
			MustSamePeers(searchRegion, region)
			if searchRegion.RegionEpoch.ConfVer+1 != region.RegionEpoch.ConfVer {
				panic(fmt.Sprintf("unmatched conf version:in=%s;search=%s.", region.GetRegionEpoch(), searchRegion.GetRegionEpoch()))
			}
			//TODO: why???
			//if searchRegion.RegionEpoch.Version+1 != region.RegionEpoch.Version {
			//	panic(fmt.Sprintf("unmatched version:in=%s;search=%s.", region.GetRegionEpoch(), searchRegion.GetRegionEpoch()))
			//}
		}

		// update the region.
		err := c.putRegion(regionInfo)
		if err != nil {
			panic(fmt.Sprintf("update inexistent region(%d) err:%s ", region.GetId(), err.Error()))
		}
		log.Debug("handleHeartbeatConfVersion update ok", zap.Uint64("region", region.GetId()))
		//理论上来说，最多就一个node变化了.
		if len(changePeers) > 0 {
			if len(changePeers) != 1 {
				panic("should only one change peer")
			} else {
				c.setRegionCount(changePeers)
			}
		}
	} else {
		if !IsSamePeers(searchRegion, region) {
			log.Warn("handleHeartbeatConfVersion not same peer",
				zap.String("search", searchRegion.String()),
				zap.String("region", region.String()))
		}
	}
	return nil
}

func (c RaftCluster) setRegionCount(changes []*metapb.Peer) {
	c.Lock()
	for _, cp := range changes {
		c.core.Stores.SetRegionCount(cp.GetStoreId(), c.core.Regions.GetStoreRegionCount(cp.GetStoreId()))
	}
	c.Unlock()
}

func IsSamePeers(left *metapb.Region, right *metapb.Region) bool {
	if len(left.GetPeers()) != len(right.GetPeers()) {
		return false
	}
	for _, p := range left.GetPeers() {
		if FindPeer(right, p.GetStoreId()) == nil {
			return false
		}
	}
	return true
}

func MustSamePeers(left *metapb.Region, right *metapb.Region) {
	if len(left.GetPeers()) != len(right.GetPeers()) {
		panic(fmt.Sprintf("unmatched peers length,left(%s)rignt(%s)", left.String(), right))
	}
	for _, p := range left.GetPeers() {
		if FindPeer(right, p.GetStoreId()) == nil {
			panic(fmt.Sprintf("not found the peer(%s),left(%s)rignt(%s)", p, left.String(), right))
		}
	}
}

func GetDiffPeers(left *metapb.Region, right *metapb.Region) []*metapb.Peer {
	peers := make([]*metapb.Peer, 0, 1)
	for _, p := range left.GetPeers() {
		if FindPeer(right, p.GetStoreId()) == nil {
			peers = append(peers, p)
		}
	}
	return peers
}

func FindPeer(region *metapb.Region, storeID uint64) *metapb.Peer {
	for _, p := range region.GetPeers() {
		if p.GetStoreId() == storeID {
			return p
		}
	}
	return nil
}

func (c *RaftCluster) updateStoreStatusLocked(id uint64) {
	leaderCount := c.core.GetStoreLeaderCount(id)
	regionCount := c.core.GetStoreRegionCount(id)
	pendingPeerCount := c.core.GetStorePendingPeerCount(id)
	leaderRegionSize := c.core.GetStoreLeaderRegionSize(id)
	regionSize := c.core.GetStoreRegionSize(id)
	c.core.UpdateStoreStatus(id, leaderCount, regionCount, pendingPeerCount, leaderRegionSize, regionSize)
}

func makeStoreKey(clusterRootPath string, storeID uint64) string {
	return path.Join(clusterRootPath, "s", fmt.Sprintf("%020d", storeID))
}

func makeRaftClusterStatusPrefix(clusterRootPath string) string {
	return path.Join(clusterRootPath, "status")
}

func makeBootstrapTimeKey(clusterRootPath string) string {
	return path.Join(makeRaftClusterStatusPrefix(clusterRootPath), "raft_bootstrap_time")
}

func checkBootstrapRequest(clusterID uint64, req *schedulerpb.BootstrapRequest) error {
	// TODO: do more check for request fields validation.

	storeMeta := req.GetStore()
	if storeMeta == nil {
		return errors.Errorf("missing store meta for bootstrap %d", clusterID)
	} else if storeMeta.GetId() == 0 {
		return errors.New("invalid zero store id")
	}

	return nil
}

func (c *RaftCluster) getClusterID() uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.meta.GetId()
}

func (c *RaftCluster) putMetaLocked(meta *metapb.Cluster) error {
	if c.storage != nil {
		if err := c.storage.SaveMeta(meta); err != nil {
			return err
		}
	}
	c.meta = meta
	return nil
}

// GetRegionByKey gets region and leader peer by region key from cluster.
func (c *RaftCluster) GetRegionByKey(regionKey []byte) (*metapb.Region, *metapb.Peer) {
	region := c.core.SearchRegion(regionKey)
	if region == nil {
		return nil, nil
	}
	return region.GetMeta(), region.GetLeader()
}

// GetPrevRegionByKey gets previous region and leader peer by the region key from cluster.
func (c *RaftCluster) GetPrevRegionByKey(regionKey []byte) (*metapb.Region, *metapb.Peer) {
	region := c.core.SearchPrevRegion(regionKey)
	if region == nil {
		return nil, nil
	}
	return region.GetMeta(), region.GetLeader()
}

// GetRegionInfoByKey gets regionInfo by region key from cluster.
func (c *RaftCluster) GetRegionInfoByKey(regionKey []byte) *core.RegionInfo {
	return c.core.SearchRegion(regionKey)
}

// ScanRegions scans region with start key, until the region contains endKey, or
// total number greater than limit.
func (c *RaftCluster) ScanRegions(startKey, endKey []byte, limit int) []*core.RegionInfo {
	return c.core.ScanRange(startKey, endKey, limit)
}

// GetRegionByID gets region and leader peer by regionID from cluster.
func (c *RaftCluster) GetRegionByID(regionID uint64) (*metapb.Region, *metapb.Peer) {
	region := c.GetRegion(regionID)
	if region == nil {
		return nil, nil
	}
	return region.GetMeta(), region.GetLeader()
}

// GetRegion searches for a region by ID.
func (c *RaftCluster) GetRegion(regionID uint64) *core.RegionInfo {
	return c.core.GetRegion(regionID)
}

// GetMetaRegions gets regions from cluster.
func (c *RaftCluster) GetMetaRegions() []*metapb.Region {
	return c.core.GetMetaRegions()
}

// GetRegions returns all regions' information in detail.
func (c *RaftCluster) GetRegions() []*core.RegionInfo {
	return c.core.GetRegions()
}

// GetRegionCount returns total count of regions
func (c *RaftCluster) GetRegionCount() int {
	return c.core.GetRegionCount()
}

// GetStoreRegions returns all regions' information with a given storeID.
func (c *RaftCluster) GetStoreRegions(storeID uint64) []*core.RegionInfo {
	return c.core.GetStoreRegions(storeID)
}

// RandLeaderRegion returns a random region that has leader on the store.
func (c *RaftCluster) RandLeaderRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	return c.core.RandLeaderRegion(storeID, opts...)
}

// RandFollowerRegion returns a random region that has a follower on the store.
func (c *RaftCluster) RandFollowerRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	return c.core.RandFollowerRegion(storeID, opts...)
}

// RandPendingRegion returns a random region that has a pending peer on the store.
func (c *RaftCluster) RandPendingRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	return c.core.RandPendingRegion(storeID, opts...)
}

// GetPendingRegionsWithLock return pending regions subtree by storeID
func (c *RaftCluster) GetPendingRegionsWithLock(storeID uint64, callback func(core.RegionsContainer)) {
	c.core.GetPendingRegionsWithLock(storeID, callback)
}

// GetLeadersWithLock return leaders subtree by storeID
func (c *RaftCluster) GetLeadersWithLock(storeID uint64, callback func(core.RegionsContainer)) {
	c.core.GetLeadersWithLock(storeID, callback)
}

// GetFollowersWithLock return leaders subtree by storeID
func (c *RaftCluster) GetFollowersWithLock(storeID uint64, callback func(core.RegionsContainer)) {
	c.core.GetFollowersWithLock(storeID, callback)
}

// GetLeaderStore returns all stores that contains the region's leader peer.
func (c *RaftCluster) GetLeaderStore(region *core.RegionInfo) *core.StoreInfo {
	return c.core.GetLeaderStore(region)
}

// GetFollowerStores returns all stores that contains the region's follower peer.
func (c *RaftCluster) GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo {
	return c.core.GetFollowerStores(region)
}

// GetRegionStores returns all stores that contains the region's peer.
func (c *RaftCluster) GetRegionStores(region *core.RegionInfo) []*core.StoreInfo {
	return c.core.GetRegionStores(region)
}

func (c *RaftCluster) getStoreCount() int {
	return c.core.GetStoreCount()
}

// GetStoreRegionCount returns the number of regions for a given store.
func (c *RaftCluster) GetStoreRegionCount(storeID uint64) int {
	return c.core.GetStoreRegionCount(storeID)
}

// GetAverageRegionSize returns the average region approximate size.
func (c *RaftCluster) GetAverageRegionSize() int64 {
	return c.core.GetAverageRegionSize()
}

// DropCacheRegion removes a region from the cache.
func (c *RaftCluster) DropCacheRegion(id uint64) {
	c.RLock()
	defer c.RUnlock()
	if region := c.GetRegion(id); region != nil {
		c.core.RemoveRegion(region)
	}
}

// GetMetaStores gets stores from cluster.
func (c *RaftCluster) GetMetaStores() []*metapb.Store {
	return c.core.GetMetaStores()
}

// GetStores returns all stores in the cluster.
func (c *RaftCluster) GetStores() []*core.StoreInfo {
	return c.core.GetStores()
}

// GetStore gets store from cluster.
func (c *RaftCluster) GetStore(storeID uint64) *core.StoreInfo {
	return c.core.GetStore(storeID)
}

func (c *RaftCluster) putStore(store *metapb.Store) error {
	c.Lock()
	defer c.Unlock()

	if store.GetId() == 0 {
		return errors.Errorf("invalid put store %v", store)
	}

	// Store address can not be the same as other stores.
	for _, s := range c.GetStores() {
		// It's OK to start a new store on the same address if the old store has been removed.
		if s.IsTombstone() {
			continue
		}
		if s.GetID() != store.GetId() && s.GetAddress() == store.GetAddress() {
			return errors.Errorf("duplicated store address: %v, already registered by %v", store, s.GetMeta())
		}
	}

	s := c.GetStore(store.GetId())
	if s == nil {
		// Add a new store.
		s = core.NewStoreInfo(store)
	} else {
		// Update an existed store.
		s = s.Clone(
			core.SetStoreAddress(store.Address),
		)
	}
	return c.putStoreLocked(s)
}

// RemoveStore marks a store as offline in cluster.
// State transition: Up -> Offline.
func (c *RaftCluster) RemoveStore(storeID uint64) error {
	op := errcode.Op("store.remove")
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return op.AddTo(core.NewStoreNotFoundErr(storeID))
	}

	// Remove an offline store should be OK, nothing to do.
	if store.IsOffline() {
		return nil
	}

	if store.IsTombstone() {
		return op.AddTo(core.StoreTombstonedErr{StoreID: storeID})
	}

	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Offline))
	log.Warn("store has been offline",
		zap.Uint64("store-id", newStore.GetID()),
		zap.String("store-address", newStore.GetAddress()))
	return c.putStoreLocked(newStore)
}

// BuryStore marks a store as tombstone in cluster.
// State transition:
// Case 1: Up -> Tombstone (if force is true);
// Case 2: Offline -> Tombstone.
func (c *RaftCluster) BuryStore(storeID uint64, force bool) error { // revive:disable-line:flag-parameter
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}

	// Bury a tombstone store should be OK, nothing to do.
	if store.IsTombstone() {
		return nil
	}

	if store.IsUp() {
		if !force {
			return errors.New("store is still up, please remove store gracefully")
		}
		log.Warn("forcedly bury store", zap.Stringer("store", store.GetMeta()))
	}

	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Tombstone))
	log.Warn("store has been Tombstone",
		zap.Uint64("store-id", newStore.GetID()),
		zap.String("store-address", newStore.GetAddress()))
	return c.putStoreLocked(newStore)
}

// BlockStore stops balancer from selecting the store.
func (c *RaftCluster) BlockStore(storeID uint64) error {
	return c.core.BlockStore(storeID)
}

// UnblockStore allows balancer to select the store.
func (c *RaftCluster) UnblockStore(storeID uint64) {
	c.core.UnblockStore(storeID)
}

// AttachAvailableFunc attaches an available function to a specific store.
func (c *RaftCluster) AttachAvailableFunc(storeID uint64, f func() bool) {
	c.core.AttachAvailableFunc(storeID, f)
}

// SetStoreState sets up a store's state.
func (c *RaftCluster) SetStoreState(storeID uint64, state metapb.StoreState) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}

	newStore := store.Clone(core.SetStoreState(state))
	log.Warn("store update state",
		zap.Uint64("store-id", storeID),
		zap.Stringer("new-state", state))
	return c.putStoreLocked(newStore)
}

// SetStoreWeight sets up a store's leader/region balance weight.
func (c *RaftCluster) SetStoreWeight(storeID uint64, leaderWeight, regionWeight float64) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}

	if err := c.s.storage.SaveStoreWeight(storeID, leaderWeight, regionWeight); err != nil {
		return err
	}

	newStore := store.Clone(
		core.SetLeaderWeight(leaderWeight),
		core.SetRegionWeight(regionWeight),
	)

	return c.putStoreLocked(newStore)
}

func (c *RaftCluster) putStoreLocked(store *core.StoreInfo) error {
	if c.storage != nil {
		if err := c.storage.SaveStore(store.GetMeta()); err != nil {
			return err
		}
	}
	c.core.PutStore(store)
	return nil
}

func (c *RaftCluster) checkStores() {
	var offlineStores []*metapb.Store
	var upStoreCount int
	stores := c.GetStores()
	for _, store := range stores {
		// the store has already been tombstone
		if store.IsTombstone() {
			continue
		}

		if store.IsUp() {
			upStoreCount++
			continue
		}

		offlineStore := store.GetMeta()
		// If the store is empty, it can be buried.
		regionCount := c.core.GetStoreRegionCount(offlineStore.GetId())
		if regionCount == 0 {
			if err := c.BuryStore(offlineStore.GetId(), false); err != nil {
				log.Error("bury store failed",
					zap.Stringer("store", offlineStore),
					zap.Error(err))
			}
		} else {
			offlineStores = append(offlineStores, offlineStore)
		}
	}

	if len(offlineStores) == 0 {
		return
	}

	if upStoreCount < c.GetMaxReplicas() {
		for _, offlineStore := range offlineStores {
			log.Warn("store may not turn into Tombstone, there are no extra up store has enough space to accommodate the extra replica", zap.Stringer("store", offlineStore))
		}
	}
}

// RemoveTombStoneRecords removes the tombStone Records.
func (c *RaftCluster) RemoveTombStoneRecords() error {
	c.Lock()
	defer c.Unlock()

	for _, store := range c.GetStores() {
		if store.IsTombstone() {
			// the store has already been tombstone
			err := c.deleteStoreLocked(store)
			if err != nil {
				log.Error("delete store failed",
					zap.Stringer("store", store.GetMeta()),
					zap.Error(err))
				return err
			}
			log.Info("delete store successed",
				zap.Stringer("store", store.GetMeta()))
		}
	}
	return nil
}

func (c *RaftCluster) deleteStoreLocked(store *core.StoreInfo) error {
	if c.storage != nil {
		if err := c.storage.DeleteStore(store.GetMeta()); err != nil {
			return err
		}
	}
	c.core.DeleteStore(store)
	return nil
}

func (c *RaftCluster) collectHealthStatus() {
	client := c.s.GetClient()
	members, err := GetMembers(client)
	if err != nil {
		log.Error("get members error", zap.Error(err))
	}
	unhealth := c.s.CheckHealth(members)
	for _, member := range members {
		if _, ok := unhealth[member.GetMemberId()]; ok {
			continue
		}
	}
}

func (c *RaftCluster) takeRegionStoresLocked(region *core.RegionInfo) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(region.GetPeers()))
	for _, p := range region.GetPeers() {
		if store := c.core.TakeStore(p.StoreId); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

func (c *RaftCluster) allocID() (uint64, error) {
	return c.id.Alloc()
}

// AllocPeer allocs a new peer on a store.
func (c *RaftCluster) AllocPeer(storeID uint64) (*metapb.Peer, error) {
	peerID, err := c.allocID()
	if err != nil {
		log.Error("failed to alloc peer", zap.Error(err))
		return nil, err
	}
	peer := &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
	return peer, nil
}

// GetConfig gets config from cluster.
func (c *RaftCluster) GetConfig() *metapb.Cluster {
	c.RLock()
	defer c.RUnlock()
	return proto.Clone(c.meta).(*metapb.Cluster)
}

func (c *RaftCluster) putConfig(meta *metapb.Cluster) error {
	c.Lock()
	defer c.Unlock()
	if meta.GetId() != c.clusterID {
		return errors.Errorf("invalid cluster %v, mismatch cluster id %d", meta, c.clusterID)
	}
	return c.putMetaLocked(proto.Clone(meta).(*metapb.Cluster))
}

// GetOpt returns the scheduling options.
func (c *RaftCluster) GetOpt() *config.ScheduleOption {
	return c.opt
}

// GetLeaderScheduleLimit returns the limit for leader schedule.
func (c *RaftCluster) GetLeaderScheduleLimit() uint64 {
	return c.opt.GetLeaderScheduleLimit()
}

// GetRegionScheduleLimit returns the limit for region schedule.
func (c *RaftCluster) GetRegionScheduleLimit() uint64 {
	return c.opt.GetRegionScheduleLimit()
}

// GetReplicaScheduleLimit returns the limit for replica schedule.
func (c *RaftCluster) GetReplicaScheduleLimit() uint64 {
	return c.opt.GetReplicaScheduleLimit()
}

// GetPatrolRegionInterval returns the interval of patroling region.
func (c *RaftCluster) GetPatrolRegionInterval() time.Duration {
	return c.opt.GetPatrolRegionInterval()
}

// GetMaxStoreDownTime returns the max down time of a store.
func (c *RaftCluster) GetMaxStoreDownTime() time.Duration {
	return c.opt.GetMaxStoreDownTime()
}

// GetMaxReplicas returns the number of replicas.
func (c *RaftCluster) GetMaxReplicas() int {
	return c.opt.GetMaxReplicas()
}

// isPrepared if the cluster information is collected
func (c *RaftCluster) isPrepared() bool {
	c.RLock()
	defer c.RUnlock()
	return c.prepareChecker.check(c)
}

func (c *RaftCluster) putRegion(region *core.RegionInfo) error {
	c.Lock()
	defer c.Unlock()
	c.core.PutRegion(region)
	return nil
}

type prepareChecker struct {
	reactiveRegions map[uint64]int
	start           time.Time
	sum             int
	isPrepared      bool
}

func newPrepareChecker() *prepareChecker {
	return &prepareChecker{
		start:           time.Now(),
		reactiveRegions: make(map[uint64]int),
	}
}

// Before starting up the scheduler, we need to take the proportion of the regions on each store into consideration.
func (checker *prepareChecker) check(c *RaftCluster) bool {
	if checker.isPrepared || time.Since(checker.start) > collectTimeout {
		return true
	}
	// The number of active regions should be more than total region of all stores * collectFactor
	if float64(c.core.Length())*collectFactor > float64(checker.sum) {
		return false
	}
	for _, store := range c.GetStores() {
		if !store.IsUp() {
			continue
		}
		storeID := store.GetID()
		// For each store, the number of active regions should be more than total region of the store * collectFactor
		if float64(c.core.GetStoreRegionCount(storeID))*collectFactor > float64(checker.reactiveRegions[storeID]) {
			return false
		}
	}
	checker.isPrepared = true
	return true
}

func (checker *prepareChecker) collect(region *core.RegionInfo) {
	for _, p := range region.GetPeers() {
		checker.reactiveRegions[p.GetStoreId()]++
	}
	checker.sum++
}
