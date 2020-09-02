// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"strings"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	prevEntry pb.Entry
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	rl := new(RaftLog)
	rl.storage = storage
	first, err := storage.FirstIndex()
	if err != nil {
		panic("newLog storage.FirstIndex err:" + err.Error())
	}
	last, err := storage.LastIndex()
	if err != nil {
		panic("newLog storage.LastIndex err:" + err.Error())
	}
	rl.committed = first - 1
	rl.applied = first - 1
	rl.stabled = last
	//log.Warnf("first(%d)last(%d)", first, last)
	if last >= first { //not empty
		ents, err := storage.Entries(first, last+1)
		if err != nil {
			panic("newLog storage.Entries err:" + err.Error())
		}
		//do copy
		rl.entries = make([]pb.Entry, len(ents))
		copy(rl.entries, ents)
		debugf("load from storage %d entries", len(ents))
	}
	rl.prevEntry.Index = rl.applied
	if rl.applied > 0 {
		rl.prevEntry.Term, _ = rl.storage.Term(rl.applied)
	}
	return rl
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact(id uint64) {
	// Your Code Here (2C).
	//no entries;do not to compact;
	if len(l.entries) == 0 {
		return
	}
	firstEntry := l.entries[0]
	sFirst, err := l.storage.FirstIndex()
	if err != nil {
		log.Warnf("%d maybeCompact FirstIndex err:%s", id, err.Error())
		return
	}
	if sFirst == firstEntry.Index {
		return
	}
	//no way;
	if sFirst < firstEntry.Index {
		log.Fatalf("%d maybeCompact logic error: raftLog first(%d) and storage first(%d)", id, firstEntry.Index, sFirst)
		return
	}
	//sFirst > firstEntry.Index
	_, err = l.storage.Term(sFirst)
	if err != nil {
		log.Warnf("%d maybeCompact storage.Term(%d) err:%s", id, sFirst, err.Error())
		return
	}
	if l.applied < sFirst {
		//一般情况下，storage中，compact是小于apply的；那么在这里，应该跟storage中的apply是一致的.
		log.Warnf("%d apply(%d) < storage.FirstIndex(%d)", id, l.applied, sFirst)
	}
	//compact position;
	//sLast, _ := l.storage.LastIndex()
	//pos, err := l.pos(sFirst)
	//if err != nil {
	//	if err == ErrUnavailable {
	//		//这说明storage中的日志比raftLog中的日志多，这不可能!!!
	//		log.Fatalf("%d maybeCompact logic error: storage.first(%d) was not exist at raftLog", id, sFirst)
	//		return
	//	}
	//	if err == ErrCompacted {
	//		//这表示已经compact了，这不可能.
	//		log.Fatalf("maybeCompact logic error: storage first(%d) was not compacted at raftLog", sFirst)
	//		return
	//	}
	//	log.Fatalf("maybeCompact logic error(%d) err:%s", sFirst, err.Error())
	//	return
	//}
	prev, err := l.pos(sFirst - 1)
	if err != nil {
		log.Fatalf("%d maybeCompact logic error(prev:%d) err:%s", id, sFirst, err.Error())
		return
	}
	prevNode := l.entries[prev]
	l.prevEntry.Index = prevNode.GetIndex()
	l.prevEntry.Term = prevNode.GetTerm()
	//compact entries;
	if uint64(len(l.entries)) > prev+1 {
		l.entries = l.entries[prev+1:]
	} else {
		l.entries = l.entries[:0]
	}
	log.Infof("raft-%d compact [%d,%d)", id, firstEntry.Index, sFirst)
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}
	pos, err := l.pos(l.stabled + 1)
	if err == nil {
		return dupEntries(l.entries[pos:])
	}
	//log.Warnf("no entries %v", err)
	return []pb.Entry{}
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.committed < l.applied {
		panic(fmt.Sprintf("nextEnts,commit=%d;applied=%d.", l.committed, l.applied))
	}
	if l.committed == l.applied {
		return []pb.Entry{}
	}
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}
	cpos, err := l.pos(l.committed)
	if err != nil {
		slast, _ := l.storage.LastIndex()
		panic(fmt.Sprintf("nextEnts,commit=%d;applied=%d,stabled=%d,len=%d,storage.last=%d;err:%s",
			l.committed, l.applied, l.stabled, len(l.entries), slast, err.Error()))
	}
	start, err := l.pos(l.applied + 1)
	if err != nil { //如果报错，说明这个位置没有数据，那么直接返回空。
		panic(fmt.Sprintf("nextEnts,commit=%d;applied=%d,len=%d;err:%s", l.committed, l.applied, len(l.entries), err.Error()))
	}
	// [,)
	return dupEntries(l.entries[start : cpos+1])
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	elen := len(l.entries)
	if elen > 0 {
		return l.entries[elen-1].Index
	}
	return l.prevEntry.Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	pos, err := l.pos(i)
	if err != nil {
		if err == ErrCompacted {
			if i == l.prevEntry.Index {
				return l.prevEntry.Term, nil
			}
			return 0, err
		}
		return 0, err
	}
	return l.entries[pos].Term, nil
}

//----------------------------------
func dupEntries(ents []pb.Entry) (dst []pb.Entry) {
	if len(ents) > 0 {
		dst = make([]pb.Entry, len(ents))
		copy(dst, ents)
	}
	return dst
}

func (l *RaftLog) startAt(start uint64) (ents []pb.Entry, err error) {
	start, err = l.pos(start)
	if err != nil {
		return ents, err
	}
	return dupEntries(l.entries[start:]), err
}

func (l *RaftLog) String() string {
	return fmt.Sprintf(`{"applied":%d,"commited":%d,"stabled":%d,"entries<%d>":%s,"pendingSnapshot":%s}`,
		l.applied, l.committed, l.stabled, len(l.entries), entries2Str(1, l.entries), snapshot2Str(l.pendingSnapshot))
}

func snapshot2Str(sp *pb.Snapshot) string {
	if sp == nil {
		return "nil"
	}
	return fmt.Sprintf(`{dlen(%d) %v}`, len(sp.Data), sp.Metadata)
}

func entry2Str(e *pb.Entry) string {
	return fmt.Sprintf(`"%v,%d,%d,dlen(%d)`, e.EntryType, e.Term, e.Index, len(e.Data))
}

func entries2Str(n int, entries []pb.Entry) string {
	var builder strings.Builder
	builder.WriteByte('[')
	for idx, e := range entries {
		builder.WriteByte('{')
		{ //write entry
			builder.WriteString(entry2Str(&e))
		}
		builder.WriteByte('}')
		if idx < len(entries)-1 {
			if idx < n {
				builder.WriteByte(',')
			} else {
				builder.WriteString("...")
				break
			}
		}
	}
	builder.WriteByte(']')
	return builder.String()
}
