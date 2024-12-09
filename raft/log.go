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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pkg/errors"
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
	// firstIndex is the index of the first entry in entries include dummy entry
	// also mean snapshot log length
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	l := &RaftLog{
		storage: storage,
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	l.entries = append([]pb.Entry{{Index: firstIndex - 1}}, entries...)
	l.committed = firstIndex - 1
	l.applied = firstIndex - 1
	l.stabled = lastIndex

	l.firstIndex = firstIndex - 1

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
	return l.entries[1:]
}

func (l *RaftLog) hasUnstableEntries() bool {
	return l.stabled < l.LastIndex()
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	unstableIndex := l.stabled + 1
	return l.entries[unstableIndex-l.firstIndex:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.entries[l.applied-l.firstIndex+1 : l.committed-l.firstIndex+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.entries[0].Index + uint64(len(l.entries)) - 1
}

func (l *RaftLog) FirstIndex() uint64 {
	return l.entries[0].Index + 1
}

func (l *RaftLog) lastTerm() uint64 {
	// Your Code Here (2A).
	term, err := l.Term(l.LastIndex())
	if err != nil {
		panic(err)
	}
	return term
}

func (l *RaftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	// Your Code Here (2A).
	if lastTerm > l.lastTerm() {
		return true
	}
	if lastTerm == l.lastTerm() && lastIndex >= l.LastIndex() {
		return true
	}
	return false
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	var term uint64
	var err error
	if i < l.firstIndex {
		return 0, errors.New("index less than start index")
	}
	i -= l.firstIndex
	if i < 0 || i >= uint64(len(l.entries)) {
		err = errors.New("index out of range")
	} else {
		term = l.entries[i].Term
	}
	return term, err
}

func (l *RaftLog) appendEntries(entries ...*pb.Entry) {
	for _, entry := range entries {
		l.entries = append(l.entries, *entry)
	}
}

func (l *RaftLog) commitTo(toCommit uint64) bool {
	// Your Code Here (2A).
	if l.committed < toCommit && toCommit <= l.LastIndex() {
		l.committed = toCommit
		return true
	}
	return false
}

func (l *RaftLog) applyTo(toApply uint64) {
	if toApply > l.applied {
		l.applied = toApply
	}
}

func (l *RaftLog) stableTo(toStable uint64) {
	l.stabled = toStable
}

// Entries return [start, end)
func (l *RaftLog) Entries(start, end uint64) ([]pb.Entry, error) {
	// Your Code Here (2A).
	if start > end {
		return nil, errors.New("start index greater than end index")
	}
	if start == end {
		return nil, nil
	}

	if start < l.firstIndex {
		return nil, errors.New("start index less than 0")
	}
	if end > l.LastIndex()+1 {
		return nil, errors.New("end index greater than last index")
	}
	return l.entries[start-l.firstIndex : end-l.firstIndex], nil
}

func (l *RaftLog) truncateEntries(index uint64) {
	if index < l.firstIndex {
		l.entries = nil
		return
	}
	if index < l.LastIndex() {
		l.entries = l.entries[:index+1]
		if l.stabled > index {
			l.stabled = index
		}
	}
}

func (l *RaftLog) findConflict(entries []*pb.Entry) uint64 {
	for _, entry := range entries {
		if l.LastIndex() < entry.Index || l.entries[entry.Index].Term != entry.Term {
			return entry.Index
		}
	}
	return 0
}

func (l *RaftLog) findConflictByTerm(start uint64, term int64) (conflictIndex, conflictTerm uint64) {
	for i := start; i >= 0; i-- {
		if ourTerm, err := l.Term(i); err != nil {
			return i, 0
		} else if ourTerm != uint64(term) {
			return i, ourTerm
		}
	}
	return 0, 0
}

// append entries to the log, manage the stable and unstable entries
func (l *RaftLog) append(entries ...*pb.Entry) {
	if len(entries) == 0 {
		return
	}
	firstIndex := entries[0].Index
	if firstIndex > l.stabled {
		if firstIndex < l.LastIndex()+1 {
			l.truncateEntries(firstIndex - 1)
		}
		l.appendEntries(entries...)
		return
	}
	l.truncateEntries(l.stabled)
	conflictIndex := l.findConflict(entries)
	if conflictIndex == 0 {
		l.truncateEntries(l.stabled)
		if (l.stabled - firstIndex + 1) < uint64(len(entries)) {
			l.appendEntries(entries[l.stabled-firstIndex+1:]...)
		}
	} else {
		l.stabled = conflictIndex - 1
		l.truncateEntries(conflictIndex - 1)
		l.appendEntries(entries[conflictIndex-firstIndex:]...)
	}
}

func (l *RaftLog) hastNextCommittedEntries() bool {
	start := l.applied + 1
	end := min(l.committed, l.stabled) + 1
	return start < end
}

func (l *RaftLog) nextCommittedEntries() []pb.Entry {
	start := l.applied + 1
	end := min(l.committed, l.stabled) + 1
	if start >= end {
		return []pb.Entry{}
	}
	return l.entries[start:end]
}
