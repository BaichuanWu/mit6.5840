package raft

import (
	"sort"
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}


func (rf *Raft) unLockCheckUpdateLeaderCommitIndex() {
	sortIndex := make([]int, len(rf.peers))
	copy(sortIndex, rf.matchIndex)
	sort.Ints(sortIndex)
	commitIndex :=sortIndex[(len(sortIndex)-1)/2] 
	// 5.4.2
	if rf.log[commitIndex].Term == rf.currentTerm {
		rf.unLockUpdateCommitIndex(commitIndex)
	}


}

func (rf *Raft) unLockUpdateCommitIndex(index int) {
	for ; rf.commitIndex < index; {
		rf.commitIndex++ 
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.commitIndex].Command, CommandIndex: rf.commitIndex}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		reply.Success = true
	}
	if reply.Success {
		if len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
			if len(args.Entries) != 0 {
				rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
			}
			// rf.logging(fmt.Sprintf("req:%v", args))
			commitIndex :=args.LeaderCommit 
			if len(rf.log)- 1 <= commitIndex {
				commitIndex = len(rf.log) - 1
			}
			rf.unLockUpdateCommitIndex(commitIndex)
		} else {
			reply.Success = false
		}
	}
	reply.Term = rf.currentTerm
	rf.lastConnected = time.Now()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.state = StateFollower
			rf.currentTerm = reply.Term
		} else if reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			rf.unLockCheckUpdateLeaderCommitIndex()
		} else if rf.nextIndex[server] > 1 {
			rf.nextIndex[server]--
		}
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for idx := range rf.peers {
		if idx != rf.me {
			reply := &AppendEntriesReply{}
			args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, LeaderCommit: rf.commitIndex}
			nextIdx := rf.nextIndex[idx]
			args.PrevLogIndex = nextIdx - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			if len(rf.log) > nextIdx {
				args.Entries = rf.log[nextIdx:]
			}

			// rf.logging(fmt.Sprintf("send to %d nextId:%d req:%v",idx, nextIdx, args))
			go rf.sendAppendEntries(idx, args, reply)
		} else {
			rf.nextIndex[idx] = len(rf.log)
			rf.matchIndex[idx] = len(rf.log) - 1
		}
	}
}
