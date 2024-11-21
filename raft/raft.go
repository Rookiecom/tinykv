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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sync"
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
	Match, Next uint64
}

type Raft struct {
	sync.RWMutex
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

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// random election timeout
	randomElectTimeout int
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
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             None,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		State:            StateFollower,
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
	}
	r.resetRandomElectionTimeout()
	for _, id := range c.peers {
		r.Prs[id] = &Progress{}
		r.votes[id] = false
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	//r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppend, To: to, From: r.id, Term: r.Term, Entries: r.RaftLog.unstablePointerEntries(), Commit: r.RaftLog.committed})
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeat, To: to, From: r.id, Term: r.Term, Commit: r.RaftLog.committed})
}

func (r *Raft) sendPropose(m *pb.Message) {
	m.To = r.Lead
	r.msgs = append(r.msgs, *m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomElectTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	}
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
		r.msgs = nil
	}
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomElectionTimeout()

	// reset vote
	for id := range r.votes {
		r.votes[id] = false
	}
}

func (r *Raft) resetRandomElectionTimeout() {
	r.randomElectTimeout = r.electionTimeout + rand.Int()%r.electionTimeout
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.reset(term)
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.reset(r.Term + 1)
	if len(r.votes) == 1 {
		r.becomeLeader()
		return
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.reset(r.Term)
	r.Lead = r.id
	r.Step(
		pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			To:      r.id,
			From:    r.id,
			Term:    r.Term,
			LogTerm: r.Term,
			Index:   r.RaftLog.LastIndex() + 1,
			Entries: []*pb.Entry{{Data: nil}},
			Commit:  r.RaftLog.committed,
		},
	)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// check the term of the message
	switch {
	case m.Term == 0:
	// local message
	case r.Term < m.Term:
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgSnapshot || m.MsgType == pb.MessageType_MsgHeartbeat {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case r.Term > m.Term:

	}
	switch r.State {
	case StateFollower:
		return r.followerStep(&m)
	case StateCandidate:
		return r.candidateStep(&m)
	case StateLeader:
		return r.leaderStep(&m)
	}
	return nil
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Term: r.Term}
	switch r.State {
	case StateFollower:
		if r.Term > m.Term {
			msg.Reject = true
		} else if ((r.Vote == m.From) || (r.Vote == None && r.Lead == None)) && r.RaftLog.isUpToDate(m.LogTerm, m.Index) {
			r.Vote = m.From
			msg.Reject = false
		} else {
			msg.Reject = true
		}
	case StateLeader, StateCandidate:
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
			r.Vote = m.From
			msg.Reject = false
		} else if r.Vote == m.From {
			msg.Reject = false
		} else {
			msg.Reject = true
		}
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	count := 0
	for _, vote := range r.votes {
		if vote {
			count++
		}
	}
	if count >= len(r.Prs)/2+1 {
		r.becomeLeader()
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.Lead = m.From
	r.electionElapsed = 0
	r.RaftLog.commitTo(m.Commit)
}

func (r *Raft) broadcastRequestVote() {
	r.Vote = r.id
	r.votes[r.id] = true
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		Term:    r.Term,
		LogTerm: r.RaftLog.lastTerm(),
		Index:   r.RaftLog.LastIndex(),
	}
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		msg.To = id
		r.msgs = append(r.msgs, msg)
	}
}

func (r *Raft) broadcastHeartbeat() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

func (r *Raft) broadcastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// check whether leader is valid or not in the current term
func (r *Raft) checkLeader(term uint64) bool {
	if r.Term == term && r.State == StateLeader {
		return true
	}
	return false
}

// check whether the candidate is valid or not in the current term
func (r *Raft) checkCandidate(term uint64) bool {
	if r.Term >= term && r.State == StateCandidate {
		return true
	}
	return false
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

func (r *Raft) followerStep(m *pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.broadcastRequestVote()
	case pb.MessageType_MsgBeat:
		return ErrProposalDropped
	case pb.MessageType_MsgPropose:
		r.sendPropose(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(*m)
	case pb.MessageType_MsgAppendResponse:
		return ErrProposalDropped
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(*m)
	case pb.MessageType_MsgRequestVoteResponse:
		return ErrProposalDropped
	case pb.MessageType_MsgSnapshot:
		// TODO handle snapshot
		break
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(*m)
	case pb.MessageType_MsgHeartbeatResponse:
		return ErrProposalDropped
	case pb.MessageType_MsgTransferLeader:
		// TODO handle transfer leader
		break
	case pb.MessageType_MsgTimeoutNow:
		break
	}
	return nil
}

func (r *Raft) candidateStep(m *pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.broadcastRequestVote()
	case pb.MessageType_MsgBeat:
		return ErrProposalDropped
	case pb.MessageType_MsgPropose:
		break
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(*m)
	case pb.MessageType_MsgAppendResponse:
		return ErrProposalDropped
	case pb.MessageType_MsgRequestVote:
		break
	case pb.MessageType_MsgRequestVoteResponse:
		if r.checkCandidate(m.Term) {
			r.handleRequestVoteResponse(*m)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case pb.MessageType_MsgSnapshot:
		// TODO handle snapshot
		break
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(*m)
	case pb.MessageType_MsgHeartbeatResponse:
		return ErrProposalDropped
	case pb.MessageType_MsgTransferLeader:
		// TODO handle transfer leader
		break
	case pb.MessageType_MsgTimeoutNow:
		break
	}
	return nil
}

func (r *Raft) leaderStep(m *pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		return ErrProposalDropped
	case pb.MessageType_MsgBeat:
		r.broadcastHeartbeat()
	case pb.MessageType_MsgPropose:
		r.broadcastAppend()
	case pb.MessageType_MsgAppend:
		if r.checkLeader(m.Term) {
			r.broadcastAppend()
		} else {
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(*m)
		}
	case pb.MessageType_MsgAppendResponse:
		// TODO handle append response
		// update the state of the follower log replication progress

	case pb.MessageType_MsgRequestVote:
		// TODO handle vote request
		break
	case pb.MessageType_MsgRequestVoteResponse:
		// TODO need to confirm whether return ErrProposalDropped
		return ErrProposalDropped
	case pb.MessageType_MsgSnapshot:
		// TODO handle snapshot
		break
	case pb.MessageType_MsgHeartbeat:
		// TODO need to confirm whether return ErrProposalDropped
		return ErrProposalDropped
	case pb.MessageType_MsgHeartbeatResponse:
		break
	case pb.MessageType_MsgTransferLeader:
		// TODO hankdle transfer leader
		break
	case pb.MessageType_MsgTimeoutNow:
		break
	}
	return nil
}
