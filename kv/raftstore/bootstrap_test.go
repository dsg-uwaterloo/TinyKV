package raftstore

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/stretchr/testify/require"
)

func TestBootstrapStore(t *testing.T) {
	engines := util.NewTestEngines()
	defer engines.Destroy()
	require.Nil(t, BootstrapStore(engines, 1, 1))
	require.NotNil(t, BootstrapStore(engines, 1, 1))
	_, err := PrepareBootstrap(engines, 1, 1, 1)
	require.Nil(t, err)
	region := new(metapb.Region)
	require.Nil(t, engine_util.GetMeta(engines.Kv, meta.PrepareBootstrapKey, region))
	_, err = meta.GetRegionLocalState(engines.Kv, 1)
	require.Nil(t, err)
	_, err = meta.GetApplyState(engines.Kv, 1)
	require.Nil(t, err)
	_, err = meta.GetRaftLocalState(engines.Raft, 1)
	require.Nil(t, err)

	require.Nil(t, ClearPrepareBootstrapState(engines))
	require.Nil(t, ClearPrepareBootstrap(engines, 1))
	empty, err := isRangeEmpty(engines.Kv, meta.RegionMetaPrefixKey(1), meta.RegionMetaPrefixKey(2))
	require.Nil(t, err)
	require.True(t, empty)

	empty, err = isRangeEmpty(engines.Kv, meta.RegionRaftPrefixKey(1), meta.RegionRaftPrefixKey(2))
	require.Nil(t, err)
	require.True(t, empty)
}

var idx uint64

func newRegion(start, end string) *regionItem {
	idx++
	tmp := &metapb.Region{
		Id:       idx,
		StartKey: []byte(start),
		EndKey:   []byte(end),
	}
	return &regionItem{tmp}
}

func newRegion1(start string) *regionItem {
	idx++
	tmp := &metapb.Region{
		Id:       idx,
		StartKey: []byte(start),
	}
	return &regionItem{tmp}
}

func TestRegionRange(t *testing.T) {
	tmp := &metapb.Region{Id: 1}
	sm := newStoreMeta()
	regions := sm.getOverlapRegions(&metapb.Region{})
	require.Nil(t, regions)
	//insert;
	sm.regionRanges.ReplaceOrInsert(&regionItem{tmp})
	regions = sm.getOverlapRegions(&metapb.Region{})
	require.NotNil(t, regions)
	//delete;
	sm.regionRanges.Delete(&regionItem{tmp})
	regions = sm.getOverlapRegions(&metapb.Region{})
	require.Nil(t, regions)

	//
	sm.regionRanges.ReplaceOrInsert(newRegion("15", "25"))
	sm.regionRanges.ReplaceOrInsert(newRegion("35", "45"))
	sm.regionRanges.ReplaceOrInsert(newRegion("55", "56"))
	//
	check := func(split []byte) {
		var result *regionItem
		item := &regionItem{region: &metapb.Region{StartKey: split}}
		sm.regionRanges.DescendLessOrEqual(item, func(i btree.Item) bool {
			result = i.(*regionItem)
			return false
		})
		if result == nil {
			t.Logf("check(%s)DescendLessOrEqual is nil", string(split))
		} else {
			t.Logf("check(%s)DescendLessOrEqual %+v", string(split), result.region)
		}
		//
		item = &regionItem{region: &metapb.Region{StartKey: split}}
		sm.regionRanges.AscendLessThan(item, func(i btree.Item) bool {
			result = i.(*regionItem)
			return false
		})
		if result == nil {
			t.Logf("check(%s)AscendLessThan is nil", string(split))
		} else {
			t.Logf("check(%s)AscendLessThan %+v", string(split), result.region)
		}
		item = &regionItem{region: &metapb.Region{StartKey: split}}
		sm.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
			result = i.(*regionItem)
			return false
		})
		if result == nil {
			t.Logf("check(%s)AscendGreaterOrEqual is nil", string(split))
		} else {
			t.Logf("check(%s)AscendGreaterOrEqual %+v", string(split), result.region)
		}
		t.Log("")
	}
	check([]byte("10"))
	check([]byte("20"))
	check([]byte("35"))
	check([]byte("15"))
	check([]byte("25"))
	check([]byte("50"))
}

func TestRegionRange2(t *testing.T) {
	sm := newStoreMeta()
	//[1,2)
	sm.regionRanges.ReplaceOrInsert(newRegion("2", "4"))
	//
	endstrs := []string{"0", "1", "2", "3", "4", "5", "6", "7"}
	for i := 0; i < len(endstrs)-1; i++ {
		start := endstrs[i]
		for _, end := range endstrs[i+1:] {
			if check(start, end, sm, t) {
				t.Errorf("[%s,%s) ok", start, end)
			}
		}
	}

}

func (r *regionItem) Contains(key []byte) bool {
	start, end := r.region.GetStartKey(), r.region.GetEndKey()
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

func check(start, end string, sm *storeMeta, t *testing.T) bool {
	var result *regionItem
	item := &regionItem{region: &metapb.Region{StartKey: []byte(start)}}
	sm.regionRanges.DescendLessOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem)
		return false
	})
	if result == nil || !result.Contains([]byte(start)) {
		return false
	}
	return true
	//region := result.region
	//if engine_util.ExceedEndKey(region.StartKey, []byte(end)) {
	//	t.Logf("%s > %s", region.StartKey, end)
	//	return true
	//}
	//return false
}
