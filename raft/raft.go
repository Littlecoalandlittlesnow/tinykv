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
)

const debugger bool = false

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
	hardState, confState, _ := c.Storage.InitialState()
	if len(confState.Nodes) != 0 {
		c.peers = confState.Nodes
	} //不能直接让c.peers=confState.Nodes，因为后者有可能初始为空
	r := &Raft{
		id:               c.ID,
		Term:             hardState.GetTerm(),
		Vote:             hardState.GetVote(),
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0 - rand.Intn(c.ElectionTick),
	}
	if debugger {
		log.Info("新建一个Raft，ID:", r.id, "，term:", r.Term)
	}
	for _, peer := range c.peers {
		if peer == r.id {
			r.Prs[peer] = &Progress{Next: r.RaftLog.LastIndex() + 1, Match: r.RaftLog.LastIndex()}
		} else {
			r.Prs[peer] = &Progress{Next: r.RaftLog.LastIndex() + 1, Match: 0}
		}
	}
	return r
}

func (r *Raft) sendAppendResponse(to uint64, reject bool, index uint64) {
	if debugger {
		log.Info("ID:", r.id, "term:", r.Term, "向ID:", to, "发送了一个MsgAppendResponse消息，它的最后一个ent的index:", index)
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   index,
	})
	return
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
	entries := r.RaftLog.getEntriesAfterIndex(prevLogIndex)
	if debugger {
		if len(entries) > 0 {
			log.Info("ID:", r.id, "term:", r.Term, "向ID", to, "发送了一个MsgAppend消息，发送的最后一个ent的index:", entries[len(entries)-1].GetIndex(), "term:", entries[len(entries)-1].GetTerm())
		} else {
			log.Info("ID:", r.id, "term:", r.Term, "向ID", to, "发送了一个MsgAppend消息，发送的entries为空")
		}
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		Index:   prevLogIndex,
		LogTerm: prevLogTerm,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if debugger {
		log.Info("ID:", r.id, "term:", r.Term, "向ID:", to, "发送了一个MsgHeartbeat消息")
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0 - rand.Intn(r.heartbeatTimeout)
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

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term < r.Term {
		return
	}
	r.State = StateFollower
	r.Term = term
	r.Vote = 0
	r.Lead = lead
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	if debugger {
		log.Info("结点", r.id, "term为", r.Term, "成为了Follower，leader为", lead)
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.Lead = 0
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	if debugger {
		log.Info("结点", r.id, "term为", r.Term, "成为了Candidate")
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.votes = make(map[uint64]bool)
	r.heartbeatElapsed = 0
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
	})
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	//论文中的初始化要求，可以理解为默认都没匹配，然后next取一个可以包含领导人所有日志的索引
	for peer, _ := range r.Prs {
		r.Prs[peer] = &Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1,
		}
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	if debugger {
		log.Info("结点", r.id, "term为", r.Term, "成为了Leader，它的已经提交的最后一个ent的index为", r.RaftLog.committed)
	}
	for peer, _ := range r.Prs {
		if peer != r.id {
			//r.sendHeartbeat(peer)
			//因为成为leader会自动加一个空ent，必须要立刻更新其他peer的ent，所以和论文中不一样发的是appendEntries消息而不是心跳
			r.sendAppend(peer)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//if debugger {
	//	log.Info("结点", r.id, "term为", r.Term, "进入了step函数，他的状态是", r.State, "，消息类型是", m.GetMsgType())
	//}
	switch r.State {
	case StateFollower:
		switch m.GetMsgType() {
		case pb.MessageType_MsgHup:
			r.handleHup(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateCandidate:
		switch m.GetMsgType() {
		case pb.MessageType_MsgHup:
			r.handleHup(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateLeader:
		switch m.GetMsgType() {
		case pb.MessageType_MsgBeat:
			r.handleBeat(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		}
	}
	return nil
}

func (r *Raft) handlePropose(m pb.Message) {
	//首先把信息中的entries加入到leader节点中
	for i, entry := range m.GetEntries() {
		entry.Term = r.Term
		entry.Index = r.RaftLog.LastIndex() + uint64(i) + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	if debugger {
		log.Info("结点", r.id, "term为", r.Term, "正在处理Propose，最后一个提交的ent的index为", r.RaftLog.committed)
	}
	//广播发送给跟随者这些entries
	for peer, _ := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if debugger {
		log.Infof("[%d][Term:%d]正在处理来自[%d][Term:%d]结点的选举请求", r.id, r.Term, m.GetFrom(), m.GetTerm())
	}
	if m.GetTerm() < r.Term {
		if debugger {
			log.Infof("[%d]拒绝了[%d]的选举请求，因为后者的term太小", r.id, m.GetFrom())
		}
		return
	}
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	if m.GetTerm() > r.Term {
		r.becomeFollower(m.GetTerm(), None)
	}
	r.Term = m.GetTerm()
	reject := true
	if r.Vote == None || r.Vote == m.GetFrom() { //该成员必须还没投票，或者投的就是消息发送者
		//日志更新判断
		logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		if debugger {
			log.Infof("请求结点[%d][term:%d]的最后一个ent的term：%d，最后一个ent的index：%d", m.GetFrom(), m.GetTerm(), m.LogTerm, m.GetIndex())
			log.Infof("被请求结点[%d][term:%d]的最后一个ent的term：%d，最后一个ent的index：%d", r.id, r.Term, logTerm, r.RaftLog.LastIndex())
		}
		if m.GetLogTerm() > logTerm || m.GetLogTerm() == logTerm && m.GetIndex() >= r.RaftLog.LastIndex() {
			reject = false
			r.Vote = m.GetFrom()
		}
	}
	if debugger {
		log.Infof("[%d][term:%d]返回给[%d][term:%d]的MsgRequestVoteResponse消息中的reject：%t", r.id, r.Term, m.GetFrom(), m.GetTerm(), reject)
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.GetFrom(),
		Term:    r.Term,
		Reject:  reject,
	})
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.GetTerm() < r.Term {
		return
	}
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	r.Term = m.GetTerm()
	r.votes[m.GetFrom()] = !m.GetReject()
	cntT, cntF := 0, 0
	for _, vote := range r.votes {
		if vote == true {
			cntT++
		} else {
			cntF++
		}
	}
	if debugger {
		log.Info("结点", r.id, "term为", r.Term, "接受了来自结点", m.From, "term为", m.Term, "的投票信息，投票结果为reject:", m.GetReject())
	}
	if cntT > len(r.Prs)/2 {
		r.becomeLeader()
	} else if cntF > len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) handleHup(m pb.Message) {
	if debugger {
		log.Info("结点", r.id, "term为", r.Term, "在进行handleHup")
	}
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
	index := r.RaftLog.LastIndex()
	term, _ := r.RaftLog.Term(index)
	for peer, _ := range r.Prs {
		if peer != r.id {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      peer,
				From:    r.id,
				Term:    r.Term,
				Index:   index,
				LogTerm: term,
			})
		}
	}
}

func (r *Raft) handleBeat(m pb.Message) {
	if debugger {
		log.Info("结点", r.id, "term为", r.Term, "正在进行handleBeat,来自结点", m.GetFrom(), "term为", m.GetTerm())
	}
	r.heartbeatElapsed = 0
	for peer, _ := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if debugger {
		log.Info("结点", r.id, "term为", r.Term, "正在进行handleAppendEntries,来自结点", m.GetFrom(), "term为", m.GetTerm())
	}
	if m.GetTerm() < r.Term {
		if debugger {
			log.Info("结点", r.id, "term为", r.Term, "拒绝了增加entries，因为发送者term太小")
		}
		r.sendAppendResponse(m.GetFrom(), true, 0)
		return
	}
	r.becomeFollower(m.GetTerm(), m.GetFrom())
	if m.GetIndex() > r.RaftLog.LastIndex() { //接收者的日志较少，需要返回拒绝并重置next
		if debugger {
			log.Info("结点", r.id, "term", r.Term, "拒绝了增加entries，因为接收者日志的index太小了，需要返回让leader存的next减少，index为", r.RaftLog.LastIndex())
		}
		r.sendAppendResponse(m.GetFrom(), true, r.RaftLog.LastIndex())
		return
	}
	Term, _ := r.RaftLog.Term(m.GetIndex())
	if m.GetIndex() >= r.RaftLog.firstIndex && Term != m.GetLogTerm() {
		r.sendAppendResponse(m.GetFrom(), true, r.RaftLog.LastIndex())
		if debugger {
			log.Info("结点", r.id, "term为", r.Term, "拒绝了增加entries，因为index的日志term不相同")
		}
		return
	}
	for _, entry := range m.GetEntries() {
		if m.GetIndex() <= r.RaftLog.LastIndex() {
			Term, _ := r.RaftLog.Term(entry.GetIndex())
			if Term != entry.GetTerm() {
				//println("你进入了ents删除")
				if len(r.RaftLog.entries) > 0 && int64(entry.GetIndex()-r.RaftLog.firstIndex) >= 0 {
					r.RaftLog.entries = r.RaftLog.entries[:entry.GetIndex()-r.RaftLog.firstIndex] //删除从这个节点开始的ents
				}
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
				r.RaftLog.stabled = min(r.RaftLog.stabled, entry.GetIndex()-1)
			}
		} else {
			//println("你进入了ents添加")
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}
	}
	//println("你进入了最后返回同意信息")
	if m.GetCommit() > r.RaftLog.committed { //论文中有提到接收者需要做的
		r.RaftLog.committed = min(m.GetCommit(), m.GetIndex()+uint64(len(m.GetEntries())))
	}
	if debugger {
		log.Info("结点", r.id, "term为", r.Term, "同意了增加entries，且最后一个提交的index为", r.RaftLog.committed, "，最后一个ent的index为", r.RaftLog.LastIndex())
	}
	r.sendAppendResponse(m.GetFrom(), false, r.RaftLog.LastIndex())
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if debugger {
		log.Info("结点", r.id, "term为", r.Term, "正在进行handleAppendEntriesResponse,来自结点", m.GetFrom(), "term为", m.GetTerm())
	}
	if m.GetTerm() < r.Term {
		return
	}
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	if m.GetReject() == true { //next减一然后重发
		r.Prs[m.GetFrom()].Next--
		r.sendAppend(m.GetFrom())
		return
	}
	if m.GetIndex() > r.Prs[m.GetFrom()].Match {
		r.Prs[m.GetFrom()].Match = m.GetIndex()
		r.Prs[m.GetFrom()].Next = m.GetIndex() + 1
		//判断是否可以提交
		i := r.RaftLog.committed + 1
		isCommitted := false
		for i <= r.RaftLog.LastIndex() {
			logTerm, _ := r.RaftLog.Term(i)
			if logTerm < r.Term { //不能对老任期内的日志条目进行提交
				i++
				continue
			}
			num := 0
			for _, peer := range r.Prs {
				if peer.Match >= i {
					num++
				}
			}
			if num > len(r.Prs)/2 {
				r.RaftLog.committed = i
				i++
				isCommitted = true
				continue
			}
			break
		}
		if isCommitted {
			if debugger {
				log.Info("ID:", r.id, "的提交ent更新了，且index:", r.RaftLog.committed)
			}
			for peer, _ := range r.Prs {
				if r.id != peer {
					r.sendAppend(peer)
				}
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if debugger {
		log.Info("结点", r.id, "term为", r.Term, "正在进行handleHeartbeat,来自结点", m.GetFrom(), "term为", m.GetTerm())
	}
	if m.GetTerm() < r.Term {
		return
	}
	r.Term = m.GetTerm()
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	r.becomeFollower(m.GetTerm(), m.From)
	LastlogTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if debugger {
		log.Info("结点", r.id, "term为", r.Term, "向结点", m.GetFrom(), "回复了一条MsgHeartbeatResponse消息")
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.GetFrom(),
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: LastlogTerm,
	})
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if debugger {
		log.Info("结点", r.id, "term为", r.Term, "正在进行handleHeartbeatResponse,来自结点", m.GetFrom(), "term为", m.GetTerm())
	}
	logTerm, _ := r.RaftLog.Term(r.RaftLog.committed)
	if r.RaftLog.committed > m.GetIndex() || r.RaftLog.committed == m.GetIndex() && logTerm > m.GetLogTerm() {
		r.sendAppend(m.GetFrom())
	}
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
