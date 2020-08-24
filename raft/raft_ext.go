package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (l *RaftLog) pos(idx uint64) (uint64, error) {
	elen := len(l.entries)
	if elen == 0 {
		return 0, ErrCompacted
	}
	e := l.entries[0]
	if idx < e.Index {
		return 0, ErrCompacted
	}
	off := idx - e.Index
	if off >= uint64(elen) {
		return 0, ErrUnavailable
	}
	return off, nil
}

func snapshotEqual(a, b *pb.Snapshot) bool {
	return false
}
