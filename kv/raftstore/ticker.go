package raftstore

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
)

type ticker struct {
	regionID  uint64
	tick      int64
	schedules []tickSchedule
}

type tickSchedule struct {
	runAt    int64
	interval int64
}

func newTicker(regionID uint64, cfg *config.Config) *ticker {
	baseInterval := cfg.RaftBaseTickInterval
	t := &ticker{
		regionID:  regionID,
		schedules: make([]tickSchedule, 6),
	}
	t.schedules[int(PeerTickRaft)].interval = 1
	t.schedules[int(PeerTickRaftLogGC)].interval = int64(cfg.RaftLogGCTickInterval / baseInterval)
	t.schedules[int(PeerTickSplitRegionCheck)].interval = int64(cfg.SplitRegionCheckTickInterval / baseInterval)
	t.schedules[int(PeerTickSchedulerHeartbeat)].interval = int64(cfg.SchedulerHeartbeatTickInterval / baseInterval)
	util.RSDebugf("newTicker(%d) base(%d)raft(%d)raftLogGC(%d)SplitRegionCheck(%d)SchedulerHeartbeat(%d)", regionID,
		baseInterval, t.schedules[int(PeerTickRaft)].interval,
		t.schedules[int(PeerTickRaftLogGC)].interval,
		t.schedules[int(PeerTickSplitRegionCheck)].interval,
		t.schedules[int(PeerTickSchedulerHeartbeat)].interval)
	return t
}

const SnapMgrGcTickInterval = 1 * time.Minute

func newStoreTicker(cfg *config.Config) *ticker {
	baseInterval := cfg.RaftBaseTickInterval
	t := &ticker{
		schedules: make([]tickSchedule, 4),
	}
	t.schedules[int(StoreTickSchedulerStoreHeartbeat)].interval = int64(cfg.SchedulerStoreHeartbeatTickInterval / baseInterval)
	t.schedules[int(StoreTickSnapGC)].interval = int64(SnapMgrGcTickInterval / baseInterval)
	return t
}

// tickClock should be called when peerMsgHandler received tick message.
func (t *ticker) tickClock() {
	t.tick++
}

// schedule arrange the next run for the PeerTick.
func (t *ticker) schedule(tp PeerTick) {
	sched := &t.schedules[int(tp)]
	if sched.interval <= 0 {
		sched.runAt = -1
		return
	}
	sched.runAt = t.tick + sched.interval
}

// isOnTick checks if the PeerTick should run.
func (t *ticker) isOnTick(tp PeerTick) bool {
	sched := &t.schedules[int(tp)]
	return sched.runAt == t.tick
}

func (t *ticker) isOnStoreTick(tp StoreTick) bool {
	sched := &t.schedules[int(tp)]
	return sched.runAt == t.tick
}

func (t *ticker) scheduleStore(tp StoreTick) {
	sched := &t.schedules[int(tp)]
	if sched.interval <= 0 {
		sched.runAt = -1
		return
	}
	sched.runAt = t.tick + sched.interval
}

type tickDriver struct {
	baseTickInterval time.Duration
	newRegionCh      chan uint64
	regions          map[uint64]struct{}
	router           *router
	storeTicker      *ticker
}

func newTickDriver(baseTickInterval time.Duration, router *router, storeTicker *ticker) *tickDriver {
	return &tickDriver{
		baseTickInterval: baseTickInterval,
		newRegionCh:      make(chan uint64),
		regions:          make(map[uint64]struct{}),
		router:           router,
		storeTicker:      storeTicker,
	}
}

func (r *tickDriver) run() {
	util.RSDebugf("tick running...")
	timer := time.Tick(r.baseTickInterval)
	for {
		select {
		case <-timer:
			for regionID := range r.regions {
				if r.router.send(regionID, message.NewPeerMsg(message.MsgTypeTick, regionID, nil)) != nil {
					log.Warnf("tickDriver remove region %d", regionID)
					delete(r.regions, regionID)
				}
			}
			r.tickStore()
		case regionID, ok := <-r.newRegionCh:
			if !ok {
				return
			}

			r.regions[regionID] = struct{}{}
		}
	}
	util.RSDebugf("tick exist...")
}

func (r *tickDriver) stop() {
	close(r.newRegionCh)
}

func (r *tickDriver) tickStore() {
	r.storeTicker.tickClock()
	for i := range r.storeTicker.schedules {
		if r.storeTicker.isOnStoreTick(StoreTick(i)) {
			r.router.sendStore(message.NewMsg(message.MsgTypeStoreTick, StoreTick(i)))
		}
	}
}
