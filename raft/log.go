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
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
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
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	l := &RaftLog{
		storage: storage,
	}
	hardState, _, _ := storage.InitialState()
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	l.entries = make([]pb.Entry, 1)
	l.committed = hardState.Commit
	l.stabled = lastIndex
	if lastIndex >= firstIndex {
		entries, err := storage.Entries(firstIndex, lastIndex+1)
		if err != nil {
			log.Fatalf("fails to new raft log, [error:%v]", err)
		}
		l.entries = append(l.entries, entries...)
	}
	if firstIndex != 1 {
		l.entries[0].Index = firstIndex - 1
		term, err := l.storage.Term(firstIndex - 1)
		if err != nil {
			log.Fatalf("fails to get the truncated entry term, [error:%v]", err)
		}
		l.entries[0].Term = term
	}
	snap, err := storage.Snapshot()
	if err != nil && err != ErrSnapshotTemporarilyUnavailable {
		log.Fatalf("fails to new raft log, [error:%v]", err)
	}
	if snap.Metadata != nil {
		l.entries[0].Term = snap.Metadata.Term
		l.entries[0].Index = snap.Metadata.Index
		l.entries = l.entries[:1]
		index := l.entries[0].Index + 1
		if lastIndex >= index {
			entries, err := storage.Entries(index, lastIndex+1)
			if err != nil {
				log.Fatalf("fails to new raft log, [error:%v]", err)
			}
			l.entries = append(l.entries, entries...)
		}
	}
	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 1 {
		return []pb.Entry{}
	}
	return l.entries[1:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	index, _ := l.GetLastIncludedIndexAndTerm()
	offset := int(l.stabled - index)
	if offset < 0 {
		panic("stabled index less last included index")
	}
	le := len(l.entries)
	if offset+1 == le {
		return []pb.Entry{}
	}
	if offset+1 > le {
		log.Panicf("stabled index out bound of raft log entries,[%v, len: %v]", offset+1, le)
	}
	return l.entries[offset+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	index, _ := l.GetLastIncludedIndexAndTerm()
	if l.applied == l.committed {
		return []pb.Entry{}
	}
	return l.entries[l.applied-index+1 : l.committed-index+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	offset := l.entries[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if i-offset >= uint64(len(l.entries)) {
		return 0, ErrUnavailableInInstability
	}
	return l.entries[i-offset].Term, nil
}

func (l *RaftLog) MustTerm(i uint64) uint64 {
	term, err := l.Term(i)
	if err != nil {
		log.Panicf("fails to get term [index:%v], [error:%v]", i, err)
	}
	return term
}

func (l *RaftLog) GetLastIncludedIndexAndTerm() (index, term uint64) {
	return l.entries[0].Index, l.entries[0].Term
}

func (l *RaftLog) GetOffset(index uint64) uint64 {
	return index - l.entries[0].Index
}
