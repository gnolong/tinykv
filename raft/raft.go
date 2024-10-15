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
	"sync"
	"time"

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

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization. Only the methods needed by the code are exposed
// (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
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
	sync.Mutex
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

	randomizedElectionTimeout int
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
	initPrs := func(r *Raft, nodes []uint64) {
		r.Prs = map[uint64]*Progress{}
		for _, node := range nodes {
			r.Prs[node] = &Progress{}
		}
	}

	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{}
	r.id = c.ID
	r.electionTimeout = c.ElectionTick
	r.heartbeatTimeout = c.HeartbeatTick
	// should be before recovering hard state
	r.becomeFollower(None, None)
	hardState, confState, _ := c.Storage.InitialState()
	if hardState.Term != 0 || hardState.Vote != 0 {
		r.Term = hardState.Term
		r.Vote = hardState.Vote
	}
	if len(confState.Nodes) == 0 {
		initPrs(r, c.peers)
	} else {
		initPrs(r, confState.Nodes)
	}
	r.RaftLog = newLog(c.Storage)
	r.RaftLog.applied = c.Applied

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		Term:    r.Term,
		To:      to,
		From:    r.id,
		Commit:  r.RaftLog.committed,
	}
	next := r.Prs[to].Next
	if next > r.Prs[r.id].Next {
		log.Panicf("#%v, follower's next index greater leader's next index, [to:%v]",
			r.id, to)
	}
	m.Index = next - 1
	m.LogTerm = r.RaftLog.MustTerm(m.Index)
	lastIncludedIndex, _ := r.RaftLog.GetLastIncludedIndexAndTerm()
	if next <= lastIncludedIndex {
		log.Fatalf("#%v, fails to get previous log match index, [to:%v]",
			r.id, to)
	}
	offset := next - lastIncludedIndex
	if offset < uint64(len(r.RaftLog.entries)) {
		entris := r.RaftLog.entries[offset:]
		for i := range entris {
			m.Entries = append(m.Entries, &entris[i])
		}
	}
	r.msgs = append(r.msgs, m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		Term:    r.Term,
		To:      to,
		From:    r.id,
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	logTerm, logIndex := r.getLastTermAndIndex()
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      peer,
			From:    r.id,
			Term:    r.Term,
			LogTerm: logTerm,
			Index:   logIndex,
		})
	}
	r.handleVoteCnt()
}

func (r *Raft) bcastAppend() {
	r.validateStateLeader()
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

func (r *Raft) bcastHeartbeat() {
	r.validateStateLeader()
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
	r.heartbeatElapsed = 0
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	ifCampaign := func() {
		if r.electionElapsed >= r.randomizedElectionTimeout {
			// r.msgs = append(r.msgs, pb.Message{
			// 	From: r.id,
			// 	To: r.id,
			// 	MsgType: pb.MessageType_MsgHup,
			// })
			r.campaign()
		}
	}
	ifBcastBeat := func() {
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			// r.msgs = append(r.msgs, pb.Message{
			// 	From: r.id,
			// 	To: r.id,
			// 	MsgType: pb.MessageType_MsgBeat,
			// })
			r.bcastHeartbeat()
		}
	}

	r.lock()
	defer r.unLock()
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		ifCampaign()
	case StateCandidate:
		r.electionElapsed++
		ifCampaign()
	case StateLeader:
		r.heartbeatElapsed++
		ifBcastBeat()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.State = StateFollower
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.randomizeElectionTimeout()
	r.msgs = nil
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.Vote = r.id
	r.votes = map[uint64]bool{}
	r.votes[r.id] = true
	r.State = StateCandidate
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomizeElectionTimeout()
	r.msgs = nil
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.Lead = r.id
	r.State = StateLeader
	r.Vote = None
	r.votes = map[uint64]bool{}
	r.electionElapsed = 0
	r.randomizeElectionTimeout()
	// r.msgs = nil
	match := r.RaftLog.LastIndex()
	for id, pr := range r.Prs {
		if id == r.id {
			pr.Match = match
		} else {
			pr.Match = 0
		}
		pr.Next = match + 1
	}
	// noop entry
	m := &pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
	}
	r.appendEntry(m)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	r.lock()
	defer r.unLock()
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			r.campaign()
		}
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			r.bcastHeartbeat()
		}
	case pb.MessageType_MsgPropose:
		if r.State != StateCandidate {
			r.handlePropose(m)
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeatbeatResponse(m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	appendResponse := func(reject bool, t, in uint64) {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			Term:    r.Term,
			To:      m.From,
			From:    r.id,
			Reject:  reject,
			LogTerm: t,
			Index:   in,
		})
	}

	if r.Term > m.Term {
		appendResponse(true, 0, 0)
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	// m.Term >= r.Term
	// to candidates
	if r.State == StateCandidate {
		r.becomeFollower(m.Term, m.From)
	} else if r.State == StateFollower {
		r.Lead = m.From
	}
	// reset election elapsed time
	r.electionElapsed = 0

	// append entries
	lastTerm, lastIndex := r.getLastTermAndIndex()
	if lastIndex < m.Index {
		appendResponse(true, lastTerm, lastIndex)
		return
	}
	lastIncludedIndex, _ := r.RaftLog.GetLastIncludedIndexAndTerm()
	if m.Index < lastIncludedIndex {
		appendResponse(false, 0, r.RaftLog.committed)
		return
	}
	term := r.RaftLog.MustTerm(m.Index)
	if term == m.LogTerm {
		for _, ent := range m.Entries {
			_, lastIndex := r.getLastTermAndIndex()
			if lastIndex+1 == ent.Index {
				r.RaftLog.entries = append(r.RaftLog.entries, *ent)
			} else {
				term := r.RaftLog.MustTerm(ent.Index)
				if term != ent.Term {
					r.RaftLog.entries = r.RaftLog.entries[:ent.Index]
					// update stabled index
					r.RaftLog.stabled = min(r.RaftLog.stabled, ent.Index-1)
					r.RaftLog.entries = append(r.RaftLog.entries, *ent)
				}
			}
		}
		messLastIndex := m.Index + uint64(len(m.Entries))
		if m.Commit > r.RaftLog.committed {
			// process old appendEntries RPC Request
			r.RaftLog.committed = min(m.Commit, messLastIndex)
		}
		appendResponse(false, 0, messLastIndex)
	} else {
		appendResponse(true, 0, m.Index-1)
	}
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if r.State != StateLeader {
		return
	}
	p := r.Prs[m.From]
	if m.Reject {
		if p.Next > m.Index+1 {
			p.Next = m.Index + 1
		}
		if p.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	} else {
		if p.Match < m.Index {
			p.Match = m.Index
			p.Next = p.Match + 1
			if r.maybeCommit() {
				// update followers commit index
				r.bcastAppend()
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	appendResponse := func(reject bool) {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			Term:    r.Term,
			To:      m.From,
			From:    r.id,
			Reject:  reject,
		})
	}

	if r.Term > m.Term {
		appendResponse(true)
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		appendResponse(false)
		return
	}
	// m.Term == r.Term
	if r.State == StateCandidate {
		r.becomeFollower(m.Term, m.From)
	} else if r.State == StateFollower {
		r.Lead = m.From
	}
	// reset election elapsed time
	r.electionElapsed = 0
	appendResponse(false)
}

func (r *Raft) handleHeatbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if m.Term < r.Term || r.Prs[m.From].Match < r.Prs[r.id].Match {
		// followers don't have update-to-date logs
		r.sendAppend(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

func (r *Raft) handleRequestVote(m pb.Message) {
	appendResponse := func(reject bool) {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Term:    r.Term,
			To:      m.From,
			From:    r.id,
			Reject:  reject,
		})
	}

	if r.Term > m.Term {
		appendResponse(true)
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	// m.Term >= r.Term
	if (r.Vote == None || r.Vote == m.From) && r.Lead == None &&
		r.validateLastEntry4RequestVote(m.LogTerm, m.Index) {
		r.Vote = m.From
		appendResponse(false)
		return
	}
	appendResponse(true)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if r.State == StateCandidate && m.Term == r.Term {
		// avoid old votes
		r.votes[m.From] = !m.Reject
		r.handleVoteCnt()
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	if r.State == StateFollower {
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
		return
	}
	for _, ent := range m.Entries {
		r.appendEntry(ent)
	}
	r.bcastAppend()
}

func (r *Raft) handleVoteCnt() {
	t := 0
	f := 0
	for _, vote := range r.votes {
		if vote {
			t++
		} else {
			f++
		}
	}
	if t >= r.quorum() {
		r.becomeLeader()
		r.bcastAppend()
		return
	}
	if f >= r.quorum() {
		r.becomeFollower(r.Term, None)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) lock() {
	// r.Lock()
}

func (r *Raft) unLock() {
	// r.Unlock()
}

func (r *Raft) validateLastEntry4RequestVote(term uint64, index uint64) bool {
	t, in := r.getLastTermAndIndex()
	if t < term {
		return true
	} else if t == term && in <= index {
		return true
	}
	return false
}

func (r *Raft) randomizeElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *Raft) getLastTermAndIndex() (term uint64, index uint64) {
	in := r.RaftLog.LastIndex()
	t, err := r.RaftLog.Term(in)
	if err != nil {
		log.Fatalf("#%v, fails to validate last entry for vote, [error:%v]", r.id, err)
	}
	return t, in
}

func (r *Raft) appendEntry(ent *pb.Entry) {
	p := r.Prs[r.id]
	// TODO: term should be from hardstate?
	ent.Term = r.Term
	ent.Index = p.Next
	r.RaftLog.entries = append(r.RaftLog.entries, *ent)
	p.Match++
	p.Next++
	r.maybeCommit()
}

func (r *Raft) validateStateLeader() {
	if r.State != StateLeader {
		log.Panicf("#%v, fails to validate leader state, [state:%v]", r.id, r.State)
	}
}

func (r *Raft) maybeCommit() bool {
	matches := make(uint64Slice, 0)
	for _, progress := range r.Prs {
		matches = append(matches, progress.Match)
	}
	sort.Sort(sort.Reverse(matches))
	commit := matches[r.quorum()-1]
	term, err := r.RaftLog.Term(commit)
	if commit <= r.RaftLog.committed {
		return false
	}
	if err != nil {
		log.Panicf("#%v, fails to get commit index's term, [error:%v]", r.id, err)
	}
	if term == r.Term {
		r.RaftLog.committed = commit
		return true
	}
	return false
}

func (r *Raft) quorum() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) newHardState() *pb.HardState {
	return &pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
