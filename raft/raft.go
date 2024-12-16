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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
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

type processStateType uint64

const (
	processStateProbe processStateType = iota
	processStateReplicate
	processStateSnapshot
)

var pstmap = [...]string{
	"Probe",
	"Replicate",
	"Snapshot",
}

func (pst processStateType) String() string {
	return pstmap[uint64(pst)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

const maxSendMsgCount = 100

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
	Match, Next     uint64
	state           processStateType
	inflightIndex   uint64
	inflightCommit  uint64
	snapshotSending bool
}

func (pr *Progress) reset() {
	pr.inflightIndex = 0
	pr.inflightCommit = 0
	pr.snapshotSending = false
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
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	r.Term = hardState.GetTerm()
	r.Vote = hardState.GetVote()
	r.RaftLog.commitTo(hardState.GetCommit())
	r.RaftLog.applyTo(c.Applied)
	r.resetRandomElectionTimeout()
	if len(c.peers) != 0 {
		for _, id := range c.peers {
			r.Prs[id] = &Progress{}
			r.votes[id] = false
		}
	} else {
		for _, id := range confState.Nodes {
			r.Prs[id] = &Progress{}
			r.votes[id] = false
		}
	}

	return r
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	//r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppend, To: to, From: r.id, Term: r.Term, Entries: r.RaftLog.unstablePointerEntries(), Commit: r.RaftLog.committed})
	pr := r.Prs[to]
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	next := pr.Next
	if next < r.RaftLog.FirstIndex() {
		pr.state = processStateSnapshot
	}
	if pr.state != processStateSnapshot {
		term, err := r.RaftLog.Term(next - 1)
		if err != nil {
			return false
		}
		msg.LogTerm = term
		msg.Index = next - 1
	}
	switch r.Prs[to].state {
	case processStateProbe:
		entries, err := r.RaftLog.Entries(next, next+1)
		if err != nil {
			return false
		}
		msg.Entries = tranEntries(entries)
		break
	case processStateReplicate:
		if r.RaftLog.LastIndex() <= pr.inflightIndex && r.RaftLog.committed <= pr.inflightCommit {
			return false
		}
		pr.inflightCommit = r.RaftLog.committed
		entries, err := r.RaftLog.Entries(next, min(r.RaftLog.LastIndex()+1, next+maxSendMsgCount))
		if err != nil {
			return false
		}
		if len(entries) != 0 {
			pr.inflightIndex = entries[len(entries)-1].Index
		}
		msg.Entries = tranEntries(entries)
		break
	case processStateSnapshot:
		if pr.snapshotSending {
			return false
		}
		msg.MsgType = pb.MessageType_MsgSnapshot
		snap, err := r.RaftLog.storage.Snapshot()
		if err != nil {
			return false
		}
		log.RaftLog(log.DSnap, "S%d send snapshot to S%d, index %d, term %d", r.id, to, snap.Metadata.Index, snap.Metadata.Term)
		msg.Snapshot = &snap
		pr.snapshotSending = true
		break
	}
	log.RaftLog(log.DLog, "S%d send append to S%d(%s), index %d, term %d, commit %d", r.id, to, pr.state.String(), msg.Index, msg.LogTerm, msg.Commit)
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeat, To: to, From: r.id, Term: r.Term, Commit: r.RaftLog.committed})
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
	}
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomElectionTimeout()

	// reset vote
	r.votes = map[uint64]bool{}
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
	log.RaftLog(log.DInfo, "S%d become candidate", r.id)
	r.State = StateCandidate
	r.reset(r.Term + 1)
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.RaftLog(log.DLeader, "S%d become leader", r.id)
	r.State = StateLeader
	r.reset(r.Term)
	r.Lead = r.id
	// reset process
	for id, process := range r.Prs {
		if r.id == id {
			continue
		}
		process.state = processStateReplicate
		process.Match = r.RaftLog.LastIndex()
		process.Next = r.RaftLog.LastIndex() + 1
		process.inflightIndex = 0
		process.inflightCommit = 0
	}

	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Term: r.Term, Index: r.RaftLog.LastIndex() + 1, Data: nil}}})
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
		if m.MsgType == pb.MessageType_MsgRequestVote {
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Term: r.Term, Reject: true})
		} else if m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgSnapshot {
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, From: r.id, To: m.From, Term: r.Term, Reject: true})
		}
		return nil
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
		} else if ((r.Vote == m.From) || (r.Vote == None && r.Lead == None)) && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
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
	log.RaftLog(log.DVote, "S%d response vote to S%d, reject %v", r.id, m.From, msg.Reject)
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
	if len(r.votes) == len(r.Prs) {
		r.becomeFollower(r.Term, None)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, From: r.id, To: m.From, Term: r.Term, Reject: true, Commit: r.RaftLog.committed, Index: m.Index}
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.msgs = append(r.msgs, msg)
		log.RaftLog(log.DLog, "S%d reject append from S%d, index %d, term %d", r.id, m.From, m.Index, m.LogTerm)
		return
	}
	r.RaftLog.append(m.Entries...)
	r.commitTo(min(m.Commit, m.Index+uint64(len(m.Entries))))
	msg.Index = m.Index + uint64(len(m.Entries))
	msg.Reject = false
	r.msgs = append(r.msgs, msg)
	log.RaftLog(log.DLog, "S%d accept append from S%d, index %d, term %d", r.id, m.From, m.Index, m.LogTerm)
	return
}

func (r *Raft) appendEntries(m *pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range m.Entries {
		entry.Index = lastIndex + uint64(i) + 1
		entry.Term = r.Term
	}
	r.RaftLog.appendEntries(m.Entries...)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
}

func (r *Raft) commitTo(toCommit uint64) bool {
	if r.RaftLog.commitTo(toCommit) {
		log.RaftLog(log.DCommit, "S%d commit index to %d", r.id, toCommit)
		return true
	}
	return false
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if len(r.Prs) == 1 {
		r.commitTo(r.RaftLog.LastIndex())
		return
	}
	pr := r.Prs[m.From]
	switch r.Prs[m.From].state {
	case processStateProbe:
		if m.Reject {
			log.RaftLog(log.DLog, "S%d receive reject from S%d, index %d, term %d, next %d-->%d", r.id, m.From, m.Index, m.LogTerm, pr.Next, min(pr.Next, m.Index))
			if m.Index < pr.Next {
				pr.Next = m.Index
			} else {
				return
			}
			if pr.Next < r.RaftLog.FirstIndex() {
				pr.state = processStateSnapshot
			}
		} else {
			pr.Match = pr.Next
			pr.Next++
			pr.state = processStateReplicate
		}
	case processStateReplicate:
		if m.Reject {
			pr.inflightIndex = 0
			pr.inflightCommit = 0
			pr.Next--
			if pr.Next < r.RaftLog.FirstIndex() {
				pr.state = processStateSnapshot
			} else {
				pr.state = processStateProbe
			}
		} else {
			pr.Next = m.Index + 1
			pr.Match = m.Index
			pr.inflightIndex = max(pr.inflightIndex, m.Index)
		}
	case processStateSnapshot:
		if m.Reject {
			log.RaftLog(log.DSnap, "S%d receive reject from S%d, index %d, term %d", r.id, m.From, m.Index, m.LogTerm)
			return
		}
		pr.Match = m.Index
		pr.Next = m.Index + 1
		pr.state = processStateProbe
		pr.snapshotSending = false
	default:
	}
	r.refreshCommit()
	r.sendAppend(m.From)

}

func (r *Raft) refreshCommit() {
	matchArray := make([]uint64, 0)
	for id, process := range r.Prs {
		if id == r.id {
			continue
		}
		matchArray = append(matchArray, process.Match)
	}
	sort.Slice(matchArray, func(i, j int) bool {
		return matchArray[i] > matchArray[j]
	})
	var halfMatch uint64
	if len(matchArray)%2 == 0 {
		halfMatch = matchArray[len(matchArray)/2-1]
	} else {
		halfMatch = matchArray[len(matchArray)/2]
	}
	halfMatchTerm, err := r.RaftLog.Term(halfMatch)
	if err != nil {
		return
	}
	if halfMatchTerm != r.Term {
		return
	}
	if res := r.commitTo(halfMatch); res {
		r.broadcastAppend()
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.Lead = m.From
	r.electionElapsed = 0
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
	})
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
		log.RaftLog(log.DVote, "S%d request vote from S%d", r.id, id)
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
	msg := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, From: r.id, To: m.From, Term: r.Term, Index: r.RaftLog.LastIndex()}
	if m.Snapshot.Metadata.Index <= r.RaftLog.committed {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		log.RaftLog(log.DSnap, "S%d(commit %d) reject snapshot from S%d, index %d, term %d", r.id, r.RaftLog.committed, m.From, m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term)
		return
	}
	r.Prs = make(map[uint64]*Progress)
	r.votes = make(map[uint64]bool)
	for _, id := range m.Snapshot.Metadata.ConfState.Nodes {
		r.Prs[id] = &Progress{}
		r.votes[id] = false
	}
	r.RaftLog.receiveSnapshot(m.Snapshot)
	log.RaftLog(log.DSnap, "S%d receive snapshot from S%d, index %d, term %d", r.id, m.From, m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term)
	r.msgs = append(r.msgs, msg)
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
		if r.Lead == None {
			return ErrProposalDropped
		}
		m.To = r.Lead
		r.msgs = append(r.msgs, *m)
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
		r.handleSnapshot(*m)
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
		r.handleRequestVote(*m)
		break
	case pb.MessageType_MsgRequestVoteResponse:
		if r.checkCandidate(m.Term) {
			r.handleRequestVoteResponse(*m)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(*m)
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
		log.RaftLog(log.DLeader, "S%d receive propose", r.id)
		r.appendEntries(m)
		if len(r.Prs) == 1 {
			r.commitTo(r.RaftLog.LastIndex())
		}
		r.broadcastAppend()
	case pb.MessageType_MsgAppend:
		r.broadcastAppend()
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(*m)

	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(*m)
		break
	case pb.MessageType_MsgRequestVoteResponse:
		break
	case pb.MessageType_MsgSnapshot:
		break
	case pb.MessageType_MsgHeartbeat:
		break
	case pb.MessageType_MsgHeartbeatResponse:
		pr := r.Prs[m.From]
		if pr.Match < r.RaftLog.LastIndex() {
			// maybe the follower is down
			pr.reset()
			r.sendAppend(m.From)
		}
		r.sendAppend(m.From)
		break
	case pb.MessageType_MsgTransferLeader:
		// TODO hankdle transfer leader
		break
	case pb.MessageType_MsgTimeoutNow:
		break
	}
	return nil
}
