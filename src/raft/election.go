package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) checkElectionTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == StateFollower || rf.state == StateCandidate {
		if time.Now().After(rf.lastConnected.Add(rf.electionTimeout)) {
			return true
		}
	}
	return false
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidatedId int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.unLockToFollower(args.Term)
	}
	if rf.votedFor == -1 && rf.currentTerm == args.Term {
		lastLogIndex := rf.log.len()-1
		lastLogTerm := rf.log.at(lastLogIndex).Term
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm==lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.votedFor = args.CandidatedId
			rf.persist()
			reply.VoteGranted = true
			rf.lastConnected = time.Now()
		}
	}
	reply.Term = rf.currentTerm
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term == rf.currentTerm && reply.VoteGranted {
		rf.votedGranted++
		if rf.votedGranted >= (1+len(rf.peers))/2 {
			rf.unLockToLeader()
		}
	}
	if reply.Term > rf.currentTerm {
		rf.unLockToFollower(reply.Term)
		rf.lastConnected = time.Now()
	}
	return ok
}

func (rf *Raft) unLockToLeader() {
	// rf.logging("become leader ")
	rf.state = StateLeader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = rf.log.len()
	}

}


func (rf *Raft) unLockToFollower(term int) {
	// rf.logging("become leader ")
	rf.state = StateFollower
	rf.currentTerm = term
	rf.persist()
}


func (rf *Raft) unLockToCandidate() {
	rf.votedGranted = 1
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.state = StateCandidate
	rf.persist()
	rf.resetElectionTimeout()
}

func (rf *Raft) resetElectionTimeout() {
	rf.lastConnected = time.Now()
	rf.electionTimeout = time.Duration((electionTimeoutStart + rand.Int63()%(electionTimeoutEnd-electionTimeoutStart))) * time.Millisecond
}

func (rf *Raft) broadcastVote() {
	reqArgs := RequestVoteArgs{}
	rf.mu.Lock()
	rf.unLockToCandidate()
	reqArgs.Term = rf.currentTerm
	reqArgs.CandidatedId = rf.me
	reqArgs.LastLogIndex = rf.log.len()-1
	reqArgs.LastLogTerm = rf.log.at(reqArgs.LastLogIndex).Term
	rf.mu.Unlock()
	for idx := range rf.peers {
		if idx != rf.me {
			reply := &RequestVoteReply{}
			args := reqArgs
			go rf.sendRequestVote(idx, &args, reply)

		}
	}
}

func (rf *Raft) timeoutTicker() {
	for !rf.killed() {
		if rf.checkElectionTimeout() {
			rf.broadcastVote()
		}
		ms := checkTimeoutStart + rand.Int63()%(checkTimeoutEnd-checkTimeoutStart)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
