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
	"errors"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	// Match表示当前同步的值，Next表示接下俩想要的值
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool
	// 用来表示其他节点对本节点MsgVote的回应，超过半数即成功当选or失败
	Agreed   int
	Rejected int

	// msgs need to send
	// 因为暂时不涉及web调用，且testing中只有一个节点的情况，所以往其他节点发送的消息，可以直接放在这个slice中
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	realElectionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	// Your Code Here (2A).
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// 得从storage中读取初始配置
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}

	// 能从config中读取的就从config中读取
	raft := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		Rejected:         0,
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	if hardState.Vote == c.ID {
		raft.Agreed = 1
	}
	if c.peers == nil {
		c.peers = confState.Nodes
	}

	for _, p := range c.peers {
		raft.Prs[p] = &Progress{
			Match: 0,
			Next:  0,
		}
	}
	raft.resetElectionTimeout()
	return raft
}


func (r *Raft) resetElectionTimeout() {
	r.realElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// 发送MsgApp
// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	lastIndex := r.RaftLog.LastIndex()
	preLogIndex := r.Prs[to].Next - 1

	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return true
		}
		return false
	}
	entries := r.RaftLog.Entries(r.Prs[to].Next, lastIndex+1)



	sendEntries := make([]*pb.Entry, 0, len(entries))
	for _, e := range entries {
		sendEntries = append(sendEntries, &pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Term:      e.Term,
			Index:     e.Index,
			Data:      e.Data,
		})
	}
	//if len(sendEntries) > 0 {
	//log.Infof("%d send index [%d %d] to %d\n", r.Id, sendEntries[0].Index, sendEntries[len(sendEntries)-1].Index, to)
	//}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: preLogTerm,
		Index:   preLogIndex,
		Entries: sendEntries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}


func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		if err == ErrSnapshotTemporarilyUnavailable {
			return
		}
		panic(err)
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
	})
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.isLeader() {
		r.tickHeartbeatElapsed()
	} else {
		r.tickElectionElapsed()
	}
}

func (r *Raft) tickElectionElapsed() {
	r.electionElapsed++
	//log.Debugf("%d electionElapsed: %d, realElectionTimeout: %d\n", r.Id, r.electionElapsed, r.realElectionTimeout)
	if r.electionElapsed >= r.realElectionTimeout {
		r.campaign()
	}
}

// to be candidate
func (r *Raft) campaign() {
	//fmt.Printf("%d campaign\n", r.Id)
	r.becomeCandidate()
	r.bCastVoteReq()
}


func (r *Raft) bCastVoteReq() {
	//fmt.Printf("%d vote req\n", r.Id)
	lastIndex := r.RaftLog.LastIndex()
	lastTerm := mustTerm(r.RaftLog.Term(lastIndex))

	for peer := range r.Prs {
		if peer != r.id {
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      peer,
				From:    r.id,
				Term:    r.Term,
				LogTerm: lastTerm,
				Index:   lastIndex,
			}
			r.msgs = append(r.msgs, msg)
		}
	}
}


func (r *Raft) tickHeartbeatElapsed() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed == r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.bCastHeartbeat()
	}
}

func (r *Raft) bCastHeartbeat() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

func (r *Raft) isLeader() bool {
	return r.State == StateLeader
}

func (r *Raft) isFollower() bool {
	return r.State == StateFollower
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	log.Infof("%d become follower, term: %d, lastIndex: %d\n", r.id, r.Term, r.RaftLog.LastIndex())
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.votes = nil
	r.Agreed = 0
	r.Rejected = 0
	r.resetTick()
}

// 候选人term++
// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term ++
	r.State = StateCandidate
	r.Vote = r.id

	r.votes = make(map[uint64]bool)
	r.Agreed = 1
	r.Rejected = 0
	r.votes[r.id] = true
	r.resetTick()

	log.Infof("%d become candidate, term: %d, lastIndex: %d\n", r.id, r.Term, r.RaftLog.LastIndex())
	//fmt.Printf("%d become candidate\n", r.Id)
	if r.Agreed > len(r.Prs)/2 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Infof("%d become leader, Term: %d, lastIndex: %d\n", r.id, r.Term, r.RaftLog.LastIndex())
	r.State = StateLeader

	// todo: 这里的日志这样子设置是对的吗？
	for _, v := range r.Prs {
		// 初始化，一开始Leader不知道follower和自己日志匹配的最大index
		v.Match = 0
		// unstable的第一个entry即为下一个要发送的entry位置
		v.Next = r.RaftLog.LastIndex() + 1
	}

	r.resetTick()
	// immediately append a noop entry
	r.appendEntries(&pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
		Data:      nil,
	})
	r.bCastAppend()
	// for only one node
	//r.advanceCommitIndex()
}

func (r *Raft) bCastAppend() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

func (r *Raft) advanceCommitIndex() {
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term
	// current Term:
	// set commitIndex = N ($5.3, $5.4).
	matchIndex := make([]uint64, 0, len(r.Prs))
	for _, v := range r.Prs {
		matchIndex = append(matchIndex, v.Match)
	}
	// reverse sort, for even num of node
	sort.Sort(sort.Reverse(uint64Slice(matchIndex)))
	// r only commit own log
	N := matchIndex[len(matchIndex)/2]
	advanced := false
	if N > r.RaftLog.committed {
		//fmt.Println("N is ", N, " last is ", r.RaftLog.LastIndex())
		term := mustTerm(r.RaftLog.Term(N))
		if term == r.Term {
			r.RaftLog.committed = N
			advanced = true
		}
	}
	if advanced {
		r.bCastAppend()
	}
}


func (r *Raft) appendEntries(entries ...*pb.Entry) {
	es := make([]pb.Entry, 0, len(entries))
	for _, e := range entries {
		es = append(es, pb.Entry{
			EntryType: e.EntryType,
			Term:      e.Term,
			Index:     e.Index,
			Data:      e.Data,
		})
	}

	// 往unstable中添加entries
	r.RaftLog.appendEntries(es...)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
}

func (r *Raft) resetTick() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetElectionTimeout()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}


func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResp(m)
	}
}


func (r *Raft) handleVoteResp(m pb.Message) {
	//fmt.Printf("%d handle voteResp: %+v\n", r.Id, m)
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		r.Vote = m.From
		return
	}
	if !m.Reject {
		//log.Infof("%d receive agree from %d\n", r.Id, m.From)
		r.votes[m.From] = true
		r.Agreed += 1
	} else {
		r.votes[m.From] = false
		r.Rejected += 1
	}
	if r.Agreed > len(r.Prs)/2 {
		r.becomeLeader()
	} else if r.Rejected > len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}


func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgPropose:

	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	}
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		r.handleVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)

	}
}


func (r *Raft) handleVote(m pb.Message) {
	//fmt.Printf("%d handle vote %+v\n", r.Id, m)
	// req term is lower
	if r.Term > m.Term {
		r.sendVoteResp(m.From, true)
		//fmt.Println("here1")
		return
	}
	// the req node does not have newer log
	if !r.hasNewerLogs(m.LogTerm, m.Index) {
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
		}
		r.sendVoteResp(m.From, true)
		//fmt.Println("here2")
		return
	}
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
		r.sendVoteResp(m.From, false)
		//fmt.Println("here3")
		return
	}
	// Repeat requests
	if r.Vote == m.From {
		r.sendVoteResp(m.From, false)
		//fmt.Println("here4")
		return
	}

	if r.isFollower() && r.Vote == None {
		r.Vote = m.From
		r.sendVoteResp(m.From, false)
		//fmt.Println("here5")
		return
	}
	//fmt.Println("here6")
	r.sendVoteResp(m.From, true)
}


func (r *Raft) hasNewerLogs(logTerm, index uint64) bool {
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm := mustTerm(r.RaftLog.Term(lastIndex))
	if logTerm > lastLogTerm || (logTerm == lastLogTerm && index >= lastIndex) {
		return true
	}
	return false
}


func (r *Raft) sendVoteResp(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendAppendResp(m.From, true, 0)
		return
	}

	r.becomeFollower(m.Term, m.From)


	// check prevLogIndex and prevLogTerm
	//term, err := r.RaftLog.Term(m.Index)
	//
	////if err != nil || term != m.LogTerm {
	////	r.sendAppendResp(m.From, true, 0)
	////	return
	////}
	//r.sendAppendResp(m.From, false, lastNewEntryIndex)

}


func (r *Raft) sendAppendResp(to uint64, reject bool, matched uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
		Index:   matched,
	}
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).

}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
