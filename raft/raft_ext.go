package raft

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"strings"
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

func (r *Raft) isLeader() bool {
	return r.State == StateLeader
}

func (r *Raft) prs2string() string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("Prs(%d){", len(r.Prs)))
	for id, pr := range r.Prs {
		builder.WriteString(fmt.Sprintf("%d:{%d,%d},", id, pr.Match, pr.Next))
	}
	builder.WriteByte('}')
	return builder.String()
}

func (r *Raft) LatestFollower() uint64 {
	if r.State != StateLeader {
		log.Warnf("%s was not leader", r.tag)
		return 0
	}
	if len(r.Prs) <= 1 {
		log.Warnf("%s no follower ", r.tag)
		return 0
	}
	maxLogIdx := r.RaftLog.LastIndex()
	var biggestId uint64
	var biggestLog uint64
	for id, pr := range r.Prs {
		if id == r.id {
			continue
		}
		if pr.Match == maxLogIdx {
			return id
		}
		if pr.Match > biggestLog {
			biggestLog = pr.Match
			biggestId = id
		}
	}
	if biggestId == 0 {
		log.Fatalf("%s logic error.prs=%s", r.tag, r.prs2string())
	}
	return biggestId
}
