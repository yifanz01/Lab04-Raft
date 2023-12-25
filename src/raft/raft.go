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
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type Role string

const (
	Leader    Role = "Leader"
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

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
	role    Role // should be initialized be follower
	term    int
	voteFor int        // record who you vote
	logs    []LogEntry // store cmd

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine(initialize to 0)

	// volatile state on leaders(need to be reinitialize after election)
	nextIndex  []int // todo: 搞清这两个函数值是干嘛的
	matchIndex []int //更新：nextIndex代表leader将要发给follower的下一个log的index，在leader被elect后初始化为leader的下一个log；如果AppendEntries RPC失败是因为follower没有与
	// leader 匹配的log，leader就会递减该follower的nextIndex，并重试
	// matchIndex 代表leader和她的follower match的最后一个log的index，在leader被elect后 每个follower的matchIndex被初始化为0

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	heartBeatInterval time.Duration //心跳间隔，in the paper, it is 10ms
	electionTimer     *time.Timer   //选举间隔，if electionTimer is timeout, then start a new election
	heartbeatsTimer   *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.term
	if rf.role == Leader {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
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
	CandidateId  int
	Term         int
	LastLogIndex int // Last log index of candidate
	LastLogTerm  int // term of last log of candidate
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term        int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 1. 如果candidate的term比自己还小，不投票；2.如果candidate的log比自己的旧，也不投票；3.如果已经投过票了，不投票
	// if Candidate's term is bigger, or equal to follower's term and follower has not voted yet
	rf.mu.Lock()
	defer rf.mu.Unlock()
	voting := true
	if len(rf.logs) > 0 {
		if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm || (rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs)-1 > args.LastLogIndex) {
			voting = false
		}
	}
	if args.Term < rf.term {
		voting = false
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}
	//if args.Term > rf.term || (args.Term == rf.term && rf.voteFor == -1) {
	//	rf.role = Follower
	//	rf.term = args.Term
	//	rf.voteFor = args.CandidateId
	//	rf.electionTimer = time.NewTimer(rf.randomTimeout())
	//	reply.VoteGranted = true
	//	reply.Term = rf.term
	//	rf.mu.Unlock()
	//} else {
	//	reply.VoteGranted = false
	//	reply.Term = rf.term
	//	rf.mu.Unlock()
	//}
	if args.Term > rf.term {
		rf.role = Follower
		rf.term = args.Term
		rf.voteFor = -1

		if voting {
			rf.voteFor = args.CandidateId
		}
		rf.electionTimer = time.NewTimer(rf.randomTimeout())
		reply.Term = args.Term
		reply.VoteGranted = (rf.voteFor == args.CandidateId)
		return
	}

	if args.Term == rf.term {
		if rf.voteFor == -1 && voting {
			rf.voteFor = args.CandidateId
		}
		reply.Term = rf.term
		reply.VoteGranted = (rf.voteFor == args.CandidateId)
		return
	}

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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// if current node has not been killed
		// Your code here (2A)
		// Check if a leader election should be started.
		if rf.role == Leader {
			rf.HeartBeat()
			time.Sleep(rf.heartBeatInterval)
		} else {
			time.Sleep(rf.randomTimeout())
			select {
			case <-rf.electionTimer.C:
				rf.ElectLeader()
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)

		//select {
		//case <-rf.electionTimer.C:
		//	//DPrintf("Current node %v", len(rf.peers))
		//	rf.ElectLeader()
		//case <-rf.heartbeatsTimer.C:
		//	rf.HeartBeat()
		//}
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
	rf.role = Follower
	rf.term = 0
	rf.voteFor = -1
	rf.heartBeatInterval = 50 * time.Millisecond // 50ms
	rf.electionTimer = time.NewTimer(rf.randomTimeout())
	rf.heartbeatsTimer = time.NewTimer(rf.heartBeatInterval) // every 10 ms

	rf.logs = make([]LogEntry, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) ElectLeader() {
	rf.mu.Lock()
	if !(rf.role == Candidate || rf.role == Follower) {
		rf.mu.Unlock()
		return
	}

	rf.role = Candidate
	rf.electionTimer = time.NewTimer(rf.randomTimeout())
	rf.term++
	rf.voteFor = rf.me
	votedReceived := 1
	args := RequestVoteArgs{
		CandidateId:  rf.me,
		Term:         rf.term,
		LastLogIndex: len(rf.logs) - 1,
	}
	if len(rf.logs) > 0 {
		args.LastLogTerm = rf.logs[args.LastLogIndex].Term
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		if rf.role != Candidate {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		// send vote requests to every peers
		go func(i int) {
			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			if reply.VoteGranted {
				votedReceived++
				// become the new leader and start the heartbeat
				if votedReceived > len(rf.peers)/2 && rf.role == Candidate {
					rf.role = Leader
					rf.voteFor = -1
					rf.electionTimer = time.NewTimer(rf.randomTimeout())
					rf.mu.Unlock()
					for j := 0; j < len(rf.peers); j++ {
						if j == rf.me {
							continue
						}
						rf.nextIndex[j] = len(rf.logs)
						rf.matchIndex[j] = -1
					}
					// DPrintf("Node %v become the leader", rf.me)
					// now the new leader should send heartbeat messages to all of the other servers to
					// establish its authority and prevent new elections
					go rf.HeartBeat()
					return
				}
				rf.mu.Unlock()
			} else if reply.Term > rf.term {
				rf.role = Follower
				rf.term = reply.Term
				rf.voteFor = -1
				rf.electionTimer = time.NewTimer(rf.randomTimeout())

				rf.mu.Unlock()
				return
			} else {
				rf.mu.Unlock()
			}
		}(i)
	}

}

func (rf *Raft) HeartBeat() {
	for !rf.killed() {
		// once the peer is dead or it is not the leader, then it will stop sending heartbeat to others
		// only Leader will send heartbeat to followers
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		rf.heartbeatsTimer.Reset(rf.heartBeatInterval)
		args := AppendEntriesArgs{
			Term:     rf.term,
			LeaderId: rf.me,
		}
		rf.mu.Unlock()

		heartBeatNoResp := 0
		wg := sync.WaitGroup{}
		// sending heartbeat to it's follower
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				// todo:
				rf.mu.Lock()
				rf.electionTimer = time.NewTimer(rf.randomTimeout())
				rf.mu.Unlock()
				continue
			}

			//args.PrevLogIndex = rf.nextIndex[i] - 1
			//if args.PrevLogIndex >= 0 {
			//	args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			//}
			//
			//if rf.nextIndex[i] < len(rf.logs) {
			//	args.Entries = rf.logs[rf.nextIndex[i]:]
			//}
			//args.LeaderCommit = rf.commitIndex

			rf.mu.Lock()
			if rf.role != Leader {
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
			wg.Add(1)

			go func(i int) {
				defer wg.Done()
				reply := AppendEntriesReply{}
				ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
				if !ok {
					// receive no response from peers[i]
					rf.mu.Lock()
					heartBeatNoResp++
					rf.mu.Unlock()
					return
				}

				rf.mu.Lock()
				if reply.Term > rf.term {
					rf.term = reply.Term
					rf.role = Follower
					rf.voteFor = -1
				}
				rf.mu.Unlock()

			}(i)
		}
		wg.Wait()
		rf.mu.Lock()
		if heartBeatNoResp > len(rf.peers)/2 && rf.role == Leader {
			rf.role = Follower
			rf.voteFor = -1
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store(empty if heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	// if the Leader's term is greater than mine
	if args.Term >= rf.term {
		rf.electionTimer = time.NewTimer(rf.randomTimeout())
		// DPrintf("Node: %v, election time: %v", rf.me, rf.electionTimer)
		rf.role = Follower
		rf.term = args.Term
	} else if rf.term > args.Term {
		// todo：
		// 目前个人推测，当一个节点收到落后于自己的term只可能发生在网络分区的情况下
		// 这个时候的leader其实没有什么意义
		reply.Success = false
		reply.Term = rf.term

	}
}
