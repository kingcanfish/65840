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
	"context"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// StatusType 定义 节点的三种状态
type StatusType = int

const (
	StatusFollower = iota
	StatusCandidate
	StatusLeader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有节点上的持久状态, 处理请求时需要先更新这些持久状态 然后再响应请求
	currentTerm int // 该节点已知的当前任期。节点启动时初始化为 0，然后单调递增。
	voteFor     int // 投票给谁（candidateId）；如果没有就是 none (-1?)
	leaderID    int // 当前的 leader

	Logs []LogEntry // 日志数组

	// 易丢失状态
	status           StatusType
	commitIndex      int // 最后提交的 entry 的 index,初始化为 -1? 然后单调递增
	lastAppliedIndex int // 最后 apply 到状态机的 index 初始化为 -1 ?, 单调递增

	// todo 后续完善
	// leader 节点上的易丢失状态
	lastHeartBeatStamp time.Time // 上一次收到心跳的时间戳

}

type LogEntry struct {
	Term int    // 日志所在领导人任期
	Log  []byte // 日志条目
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.status == StatusLeader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选者的任期
	CandidateID  int // 候选者的ID
	LastLogIndex int // 候选者最后一条日志的索引
	LastLogTerm  int // 最后一条日志的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 自己的任期
	VoteGranted bool // 是否同意投票给候选者
}

type AppendEntriesArgs struct {
	Term         int // leader的任期编号
	LeaderID     int // follower 重定向用到
	PrevLogIdx   int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int // currentTerm
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	voteFor := rf.voteFor

	if args.Term < currentTerm {
		reply.VoteGranted = false
		return
	}

	// 如果一个节点发现自己的 currentTerm 小于其他节点的，要立即更新自己的
	if args.Term > currentTerm {

		rf.currentTerm = args.Term
	}

	if (voteFor == -1 || voteFor == args.CandidateID) && rf.receiveLogIsUptoDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm

	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply = &AppendEntriesReply{
		Term:    0,
		Success: false,
	}

	rf.mu.Lock()

	defer rf.mu.Unlock()

	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = StatusFollower
	}
	rf.lastHeartBeatStamp = time.Now()
	return
}

// receiveLogIsUptoDate 判断受到的日志是不是比自己的新
// 如果 term 不同，那 term 新的那个 log 胜出；
// 如果 term 相同，那 index 更大（即更长）的那个 log 胜出
func (rf *Raft) receiveLogIsUptoDate(index, term int) bool {
	selfCurTerm, selfCurIdx := rf.lastLogInfo()
	if term > selfCurTerm {
		return true
	}
	if term == selfCurTerm && index >= selfCurIdx {
		return true
	}
	return false
}

func (rf *Raft) lastLogInfo() (term, index int) {
	index = len(rf.Logs) - 1
	term = rf.Logs[index].Term
	return
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker 看上去就是follower的loop
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		timeOut := rf.RandomElectionTimeout()

		rf.mu.Lock()
		if rf.status == StatusFollower {

			// 选举超时
			if time.Now().After(rf.lastHeartBeatStamp.Add(timeOut)) {
				rf.beCandidateLocked()
			}
		}

		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// beCandidateLocked
func (rf *Raft) beCandidateLocked() {
	rf.currentTerm++
	rf.status = StatusCandidate
	rf.lastHeartBeatStamp = time.Now()
	serveTerm := rf.currentTerm

	go rf.candidateLoop(serveTerm)

}

func (rf *Raft) candidateLoop(serveTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != serveTerm || rf.status != StatusCandidate {
		return
	}
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)
	respChan := make(chan *RequestVoteReply, len(rf.peers))
	rf.doSendVoteRequestLocked(respChan)

	go rf.processVoteReply(ctx, serveTerm, respChan)

	// 超时计时器
	go func(serveTerm int) {
		time.Sleep(rf.RandomElectionTimeout())
		rf.mu.Lock()

		if rf.status == StatusCandidate && rf.currentTerm == serveTerm {
			rf.beCandidateLocked()
		}
		defer rf.mu.Unlock()
		cancel()
		return

	}(serveTerm)

}

func (rf *Raft) leaderLoop(serveTerm int) {
	for {
		rf.mu.Lock()
		if rf.status != StatusLeader || serveTerm != rf.currentTerm {
			rf.mu.Unlock()
			return
		}

		rf.doSendAppendEntriesLocked()

		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) doSendAppendEntriesLocked() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIdx:   0,
			PrevLogTerm:  0,
			Entries:      nil,
			LeaderCommit: 0,
		}

		go func(server int) {
			reply := &AppendEntriesReply{}
			if ok := rf.sendAppendEntries(server, args, reply); ok {
				// todo handle the appendEntries reply
			}
		}(i)

	}
}

func (rf *Raft) doSendVoteRequestLocked(respChan chan *RequestVoteReply) {
	term, idx := rf.lastLogInfo()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: idx,
		LastLogTerm:  term,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			if success := rf.sendRequestVote(peer, args, reply); success {
				respChan <- reply
			}
		}(i)
	}
}

func (rf *Raft) RandomElectionTimeout() time.Duration {
	return (time.Duration(150 + (rand.Int63() % 150))) * time.Millisecond
}

// beLeaderLocked Need Locked改变节点状态为leader 调用前应该上锁
func (rf *Raft) beLeaderLocked() {
	rf.lastHeartBeatStamp = time.Now()
	rf.status = StatusLeader
	go rf.leaderLoop(rf.currentTerm)
}

func (rf *Raft) processVoteReply(ctx context.Context, serveTerm int, respChan chan *RequestVoteReply) {
	n := len(rf.peers)
	voteGranted := 1
	for {
		select {
		case <-ctx.Done():
			return
		case reply := <-respChan:
			if reply.VoteGranted == true {
				voteGranted++
			}
		}

		// 赢得选举转变成 leader
		if voteGranted > n/2 {
			rf.mu.Lock()
			if rf.status != StatusCandidate || rf.currentTerm != serveTerm {
				rf.mu.Unlock()
				return
			}
			rf.beLeaderLocked()
			rf.mu.Unlock()
			return
		}

	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.lastAppliedIndex = -1
	rf.commitIndex = -1
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.status = StatusFollower
	rf.Logs = []LogEntry{
		{
			Term: -1,
			Log:  nil,
		},
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
