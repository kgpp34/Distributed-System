package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	// "6.824/labgob"
	"6.824/labrpc"
)

type PeerState string

const (
	Follower  PeerState = "Follower"
	Candidate PeerState = "Candidate"
	Leader    PeerState = "Leader"
)

const (
	HeartBeatTimeout = 125
	ElectronTimeout  = 500
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int

	//2B
	logs        []Entry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electronTimer  *time.Timer
	heartBeatTimer *time.Timer

	state PeerState

	applyCh        chan ApplyMsg
	applyCond      *sync.Cond   // used to wakeup applier goroutine after committing new entries
	replicatorCond []*sync.Cond // used to signal replicator goroutine to batch replicating entries
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.RLock()
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm

	rf.mu.RUnlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int

	//2B
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VotedGranted bool
}

// log entry struct
type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

type AppendEntriesRequest struct {
	Term     int
	LeaderId int
	// 2B
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Entry
}

func (request AppendEntriesRequest) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,PrevLogIndex:%v,PrevLogTerm:%v,LeaderCommit:%v,Entries:%v}", request.Term, request.LeaderId, request.PrevLogIndex, request.PrevLogTerm, request.LeaderCommit, request.Entries)
}

type AppendEntriesResponse struct {
	Term    int
	Success bool

	//2B
	//reduce the rejected AppendEntries RPC number
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) genElectronParams() *RequestVoteArgs {
	req := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	return req
}

func (rf *Raft) genAppendEntiesParams(prevLogIndex int) *AppendEntriesRequest {
	req := &AppendEntriesRequest{
		Term:     rf.currentTerm,
		LeaderId: rf.me,

		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.getLastLog().Term,

		Entries:      rf.logs,
		LeaderCommit: rf.commitIndex,
	}
	return req
}

func (rf *Raft) switchState(state string) {
	rf.state = PeerState(state)
}

func (rf *Raft) logUpToDate(lastLogIndex int, lastLogTerm int) bool {
	lastLog := rf.getLastLog()
	return lastLogTerm > lastLog.Term || (lastLogTerm == lastLog.Term && lastLogTerm >= lastLog.Index)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(req *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,} before processing requestVoteRequest %v and reply requestVoteResponse %v", rf.me, rf.state, rf.currentTerm, req, reply)

	if req.Term < rf.currentTerm || (req.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != req.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VotedGranted = false
		return
	}

	// switch to Follower
	if req.Term > rf.currentTerm {
		rf.switchState("Follower")
		rf.currentTerm = req.Term
		rf.votedFor = -1
	}

	// 2B electron restriction
	if !rf.logUpToDate(req.LastLogIndex, req.LastLogTerm) {
		reply.Term = rf.currentTerm
		reply.VotedGranted = false
		return
	}

	rf.votedFor = req.CandidateId
	rf.electronTimer.Reset(getElectronTimeout())

	reply.Term = rf.currentTerm
	reply.VotedGranted = true

}

func (rf *Raft) matchLog(reqPrevLogTerm int, reqPrevLogIndex int) bool {
	/* Reply false if log doesn't contain an entry at pervLogIndex whose term matches pervLogTerm. */
	if len(rf.logs) <= reqPrevLogIndex || rf.logs[reqPrevLogIndex].Term != reqPrevLogTerm {
		return false
	}

	return true
}

func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}

// follower receive leader append entries function
func (rf *Raft) AppendEntries(req *AppendEntriesRequest, response *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, req, response)

	if req.Term < rf.currentTerm {
		response.Success = false
		response.Term = rf.currentTerm
		return
	}

	if req.Term > rf.currentTerm {
		rf.currentTerm = req.Term
		rf.votedFor = -1
	}

	rf.switchState("Follower")
	rf.electronTimer.Reset(getElectronTimeout())

	if !rf.matchLog(req.PrevLogTerm, req.PrevLogIndex) {
		response.Success = false
		response.Term = rf.currentTerm

		lastIndex := rf.getLastLog().Index
		if lastIndex < req.PrevLogIndex {
			//follower log is not up to date
			// need to tell leader append entrys to follower
			response.ConflictIndex, response.ConflictTerm = lastIndex+1, -1
		} else {
			// last index >= pre log index
			firstIndex := rf.getFirstLog().Index
			response.ConflictTerm = rf.logs[req.PrevLogIndex-firstIndex].Term
			index := req.PrevLogIndex - 1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == response.ConflictTerm {
				index--
			}
			response.ConflictIndex = index
		}
		return
	}

	firstIndex := rf.getFirstLog().Index
	for index, entry := range req.Entries {
		if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
			// add leader entry to follower logs
			rf.logs = shrinkEntriesArray(rf.logs[:entry.Index-firstIndex], req.Entries[index:])
			break
		}
	}

	rf.advanceCommitIndexForFollower(req.LeaderCommit)
	response.Success = true
	response.Term = rf.currentTerm
}

func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
	if rf.commitIndex < leaderCommit {
		rf.commitIndex = min(rf.commitIndex, leaderCommit)
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func shrinkEntriesArray(raftEntry []Entry, leaderEntry []Entry) []Entry {
	raftEntry = append(raftEntry, leaderEntry...)
	return raftEntry
}

// 复制线程
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		// 如果不需要为这个 peer 复制条目，只要释放 CPU 并等待其他 goroutine 的信号，如果服务添加了新的命令
		// 如果这个peer需要复制条目，这个goroutine会多次调用replicateOneRound(peer)直到这个peer赶上，然后等待
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		// maybe a pipeline mechanism is better to trade-off the memory usage and catch up time
		rf.replicateOneRound(peer)
	}
}

// used by replicator goroutine to judge whether a peer needs replicating
func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}

	newLog := rf.appendNewEntryToLeader(command)
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, newLog, rf.currentTerm)
	rf.BroadcastHeartbeat(false)
	return newLog.Index, newLog.Term, true
}

func (rf *Raft) appendNewEntryToLeader(command interface{}) Entry {
	lastLog := rf.logs[len(rf.logs)-1]
	newLog := &Entry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   lastLog.Index + 1,
	}
	rf.logs = append(rf.logs, *newLog)
	return *newLog
}

// 心跳维持领导力
func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// need sending at once to maintain leadership
			go rf.replicateOneRound(peer)
		} else {
			// just signal replicator goroutine to send entries in batch
			rf.replicatorCond[peer].Signal()
		}
	}
}

// Leader 向Follwer 发送复制请求
func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.getFirstLog().Index {
		// only snapshot can catch up

	} else {
		// just entries can catch up
		request := rf.genAppendEntiesParams(prevLogIndex)
		rf.mu.RUnlock()
		response := new(AppendEntriesResponse)
		if rf.sendAppendEntries(peer, request, response) {
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer, request, response)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElectron() {
	req := rf.genElectronParams()
	rf.votedFor = rf.me
	DPrintf("{Node %v} start electron with req : %v", rf.me, req)

	grantedVoted := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			response := new(RequestVoteReply)
			if rf.sendRequestVote(peer, req, response) {
				// vote success
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.currentTerm == req.Term && rf.state == Candidate {
					if response.VotedGranted {
						grantedVoted += 1
						if grantedVoted > len(rf.peers)/2 {
							// become leader
							rf.switchState("Leader")
							rf.BroadcastHeartbeat(true)
						}
					} else if response.Term > rf.currentTerm {
						rf.switchState("Follower")
						rf.currentTerm = response.Term
						rf.votedFor = -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) advanceCommitIndexForLeader() {
	match := make([]int, len(rf.peers))
	copy(match, rf.matchIndex)
	sort.Ints(match)
	majority := match[(len(rf.peers)-1)/2]
	/* If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].Term == currentTerm: set commitIndex = N */
	if majority > rf.commitIndex && majority < len(rf.logs) && rf.logs[majority].Term == rf.currentTerm {
		rf.commitIndex = majority
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer int, req *AppendEntriesRequest, response *AppendEntriesResponse) {
	if rf.state == Leader && rf.currentTerm == req.Term {
		if response.Success {
			/* If successful: update nextIndex and matchIndex for follower. */
			rf.nextIndex[peer] += len(req.Entries)
			rf.matchIndex[peer] = rf.nextIndex[peer] - 1

			rf.advanceCommitIndexForLeader()
		} else {
			if response.Term > rf.currentTerm {
				rf.switchState("Follower")
				rf.currentTerm, rf.votedFor = response.Term, -1
				rf.persist()
			} else if response.Term == rf.currentTerm {
				rf.nextIndex[peer] = response.ConflictIndex
				if response.ConflictTerm != -1 {
					firstIndex := rf.getFirstLog().Index
					for i := req.PrevLogIndex; i >= firstIndex; i-- {
						if rf.logs[i-firstIndex].Term == response.ConflictTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
			}
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electronTimer.C:
			// re electron
			rf.mu.Lock()
			rf.switchState("Candidate")
			rf.currentTerm += 1
			rf.startElectron()
			rf.electronTimer.Reset(getElectronTimeout())
			rf.mu.Unlock()

		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.BroadcastHeartbeat(true)
				rf.heartBeatTimer.Reset(getHeartBeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

func getHeartBeatTimeout() time.Duration {
	return time.Duration(HeartBeatTimeout) * time.Millisecond
}

func getElectronTimeout() time.Duration {
	return time.Duration(ElectronTimeout+rand.Intn(ElectronTimeout)) * time.Millisecond
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		// Your initialization code here (2A, 2B, 2C).
		// 2A
		currentTerm:    0,
		votedFor:       -1,
		state:          Follower,
		heartBeatTimer: time.NewTimer(getHeartBeatTimeout()),
		electronTimer:  time.NewTimer(getElectronTimeout()),

		//2B
		logs:       make([]Entry, 1),
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		dead: 0,

		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		// initialize from state persisted before a crash
	}

	rf.readPersist(persister.ReadRaftState())

	// 需要开共len(peer) - 1 个线程replicator，分别管理对应 peer 的复制状态
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i)
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
