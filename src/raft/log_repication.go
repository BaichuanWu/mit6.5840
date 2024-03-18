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
	// optumize in page 7-8
	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) unLockCheckUpdateLeaderCommitIndex() {
	sortIndex := make([]int, len(rf.peers))
	copy(sortIndex, rf.matchIndex)
	sort.Ints(sortIndex)
	commitIndex := sortIndex[(len(sortIndex)-1)/2]
	// 5.4.2
	if rf.log.at(commitIndex).Term == rf.currentTerm {
		rf.unLockUpdateCommitIndex(commitIndex)
	}

}

func (rf *Raft) unLockUpdateCommitIndex(index int) {
	for rf.commitIndex < index {
		rf.commitIndex++
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log.at(rf.commitIndex).Command, CommandIndex: rf.commitIndex}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		rf.unLockToFollower(args.Term)
		reply.Success = true
	}
	reply.XLen = rf.log.len()
	if reply.Success {
		if reply.XLen-1 >= args.PrevLogIndex && rf.log.at(args.PrevLogIndex).Term == args.PrevLogTerm {
			if len(args.Entries) != 0 {
				rf.log.setLog(append(rf.log.slice(1, args.PrevLogIndex+1), args.Entries...))
			}
			// rf.logging(fmt.Sprintf("req:%v", args))
			commitIndex := args.LeaderCommit
			if reply.XLen-1 < commitIndex {
				commitIndex = reply.XLen - 1
			}
			rf.unLockUpdateCommitIndex(commitIndex)
		} else {
			if reply.XLen-1 >= args.PrevLogIndex {
				reply.XTerm = rf.log.at(args.PrevLogIndex).Term
				lastIndex := args.PrevLogIndex - 1
				for ; lastIndex > 0 && rf.log.at(lastIndex).Term == reply.XTerm; lastIndex-- {
				}
				reply.XIndex = lastIndex + 1
			}
			reply.Success = false
		}
	}
	reply.Term = rf.currentTerm
	rf.lastConnected = time.Now()
}

func (rf *Raft) getLastTermLog(term int) int {
	nextIndex := sort.Search(rf.log.len(), func(i int) bool { return rf.log.at(i).Term > term })
	if nextIndex == 0 || rf.log.len() < nextIndex || rf.log.at(nextIndex-1).Term != term {
		return -1
	} else {
		return nextIndex - 1
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.unLockToFollower(reply.Term)
		} else if reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			rf.unLockCheckUpdateLeaderCommitIndex()
		} else {
			if reply.XTerm == 0 {
				rf.nextIndex[server] = reply.XLen
			} else {
				if lastIndex := rf.getLastTermLog(reply.XTerm); lastIndex == -1 {
					rf.nextIndex[server] = reply.XIndex
				} else {
					rf.nextIndex[server] = lastIndex
				}
			}
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
			args.PrevLogTerm = rf.log.at(args.PrevLogIndex).Term
			if rf.log.len() > nextIdx {
				args.Entries = rf.log.slice(nextIdx, rf.log.len())
			}

			// rf.logging(fmt.Sprintf("send to %d nextId:%d req:%v",idx, nextIdx, args))
			go rf.sendAppendEntries(idx, args, reply)
		} else {
			rf.nextIndex[idx] = rf.log.len()
			rf.matchIndex[idx] = rf.log.len() - 1
		}
	}
}
