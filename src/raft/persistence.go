package raft

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
)

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.log.encode(e)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.log.Bytes)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil {
		fmt.Println("fail to read Persist")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logFromPersist(d)
		rf.ReadSnapshot(rf.persister.ReadSnapshot())
		DPrintf("%s set log lastIndex %d lastTerm %d len %d", rf.info(), rf.log.LastIndex, rf.log.LastTerm, rf.log.len())
	}
}
