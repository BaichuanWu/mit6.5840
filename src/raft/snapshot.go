package raft

import (
	"fmt"

	"6.5840/labgob"
)

type SnapshotLog struct {
	LastTerm  int
	LastIndex int
	Bytes     []byte
	State     map[string]interface{}
	Log       []LogEntry
}

var placeholder = LogEntry{}

func (lg *SnapshotLog) info() string {
	return fmt.Sprintf("lastTerm:%d lastIndex:%d Log:%v", lg.LastTerm, lg.LastIndex, lg.Log)

}

func (lg *SnapshotLog) encode(e *labgob.LabEncoder) {
	e.Encode(lg.LastTerm)
	e.Encode(lg.LastIndex)
	e.Encode(lg.Log)
}

func logFromPersist(d *labgob.LabDecoder) *SnapshotLog {
	log := new(SnapshotLog)
	d.Decode(&log.LastTerm)
	d.Decode(&log.LastIndex)
	d.Decode(&log.Log)
	return log
}

func (lg *SnapshotLog) at(index int) LogEntry {
	if index == 0 {
		return placeholder
	}
	logIndex := index - lg.LastIndex - 1
	if logIndex < 0 {
		return LogEntry{Term: lg.LastTerm}
	}
	return lg.Log[logIndex]
}

func (lg *SnapshotLog) len() int {
	return lg.LastIndex + len(lg.Log) + 1
}

func (lg *SnapshotLog) slice(start, end int) []LogEntry {
	logStart := start - lg.LastIndex - 1
	logEnd := end - lg.LastIndex - 1
	if start == 0 {
		logStart = 0
	}
	if end == 0 {
		logEnd = len(lg.Log)
	}
	return lg.Log[logStart:logEnd]
}
func (lg *SnapshotLog) setLog(entries []LogEntry) {
	lg.Log = entries
}

func (lg *SnapshotLog) append(entries ...LogEntry) {
	lg.Log = append(lg.Log, entries...)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()

	rf.log.LastTerm = rf.log.at(index).Term
	rf.log.Bytes = snapshot
	rf.log.setLog(rf.log.slice(index+1, 0))
	rf.log.LastIndex = index
	// rf.applyMsg = append(rf.applyMsg, ApplyMsg{SnapshotValid: true, Snapshot: snapshot, SnapshotTerm: rf.log.LastTerm, SnapshotIndex: rf.log.LastIndex})
	rf.persist()
	rf.mu.Unlock()

	// for snapshotApply before commitApply on test
	// select {
	// case msg:= <-rf.applyCh:
	// 	rf.applyMsg = append([]ApplyMsg{msg},rf.applyMsg...)
	// default:

	// }
	// insertIdx :=sort.Search(len(rf.applyMsg), func(i int) bool {return rf.applyMsg[i].CommandIndex>0})
	// msg:= ApplyMsg{SnapshotValid: true, Snapshot: snapshot, SnapshotTerm: rf.log.LastTerm, SnapshotIndex: rf.log.LastIndex}
	// rf.applyMsg = append(rf.applyMsg, msg)
	// copy(rf.applyMsg[insertIdx+1:], rf.applyMsg[insertIdx:])
	// rf.applyMsg[insertIdx] = msg
	// DPrintf("after add snap %v", rf.applyInfo())
}

// restore previously persisted state.
func (rf *Raft) ReadSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.log.Bytes = data
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.unLockToFollower(args.Term)
	}
	rf.log.LastTerm = args.LastIncludedTerm
	rf.log.LastIndex = args.LastIncludedIndex
	rf.log.Bytes = args.Data
	rf.log.Log = make([]LogEntry, 0)
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
	}
	rf.applyMsg = append(rf.applyMsg, ApplyMsg{SnapshotValid: true, Snapshot: rf.log.Bytes, SnapshotTerm: rf.log.LastTerm, SnapshotIndex: rf.log.LastIndex})
	// DPrintf("apply install msg %v", rf.applyMsg[len(rf.applyMsg)-1])
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	// DPrintf("send snap shot %d -> %d  lastTerm %d lastIndex %d", rf.me, server, args.LastIncludedTerm, args.LastIncludedIndex)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.unLockToFollower(reply.Term)
		} else {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
			rf.matchIndex[server] = args.LastIncludedIndex
		}
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) syncSnapshot(server int) {
	reply := &InstallSnapshotReply{}
	args := &InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIndex: rf.log.LastIndex, LastIncludedTerm: rf.log.LastTerm, Data: rf.log.Bytes}
	go rf.sendInstallSnapshot(server, args, reply)
}
