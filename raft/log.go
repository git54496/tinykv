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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

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

	// 用在Storage中
	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// 用在Storage中
	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// 这个即：大于stabled的都是unstable的内容
	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64


	// todo: 这个是unstable的entries，初始化的时候来自于Storage中，不知道是不是代表unstable
	// all entries that have not yet compact.
	entries []pb.Entry

	// 和Leader同步时传到stable上的snapshot数据
	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64

}

func (l *RaftLog) appendEntries(entries ...pb.Entry) {
	l.entries = append(l.entries, entries...)
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}

	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()

	entries := make([]pb.Entry, 0)
	// 如果MemStorage中没有entries，firstIndex会等于lastIndex
	// 这里就是把storage中的entries提取出来
	if firstIndex <= lastIndex {
		entries, err = storage.Entries(firstIndex, lastIndex+1)
		if err != nil {
			panic(err)
		}
	}

	return &RaftLog{
		storage:    storage,
		committed:  hardState.Commit,
		applied:    firstIndex - 1,
		stabled:    lastIndex,
		firstIndex: firstIndex,
		entries:    entries,
	}
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
	return nil
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.stabled-l.firstIndex+1:]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		// INDEX表示的是一个全局唯一的标识
		return l.entries[len(l.entries)-1].Index
	}
	index, _ := l.storage.LastIndex()
	//if !IsEmptySnap(l.pendingSnapshot) {
	//	index = max(index, l.pendingSnapshot.Metadata.Index)
	//}
	return index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && i >= l.firstIndex {
		if i > l.LastIndex() {
			return 0, ErrUnavailable
		}
		return l.entries[i-l.firstIndex].Term, nil
	}
	// todo: 这里涉及到了storage，先不管
	//term, err := l.storage.Term(i)
	//if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
	//	if i == l.pendingSnapshot.Metadata.Index {
	//		return l.pendingSnapshot.Metadata.Term, nil
	//	}
	//	if i < l.pendingSnapshot.Metadata.Index {
	//		return term, ErrCompacted
	//	}
	//}
	return 0, nil
}


// Entries returns entries in [lo, hi)
func (l *RaftLog) Entries(lo, hi uint64) []pb.Entry {
	if lo >= l.firstIndex && hi-l.firstIndex <= uint64(len(l.entries)) {
		return l.entries[lo-l.firstIndex : hi-l.firstIndex]
	}
	ents, _ := l.storage.Entries(lo, hi)
	return ents
}
