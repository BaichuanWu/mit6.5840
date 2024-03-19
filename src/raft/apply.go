package raft

import (
	"fmt"
	"time"
)

func (rf *Raft) applyInfo() []string {
	rs := make([]string, 0)
	for _, m := range rf.applyMsg {
		rs = append(rs, fmt.Sprintf("commandId %d  snapshotId %d", m.CommandIndex, m.SnapshotIndex))
	}
	return rs

}

func (rf *Raft) applyTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		if len(rf.applyMsg) != 0 {
			// DPrintf("before apply  %v ", rf.applyInfo())
			msg := rf.applyMsg[0]
			rf.applyMsg = rf.applyMsg[1:]
			rf.mu.Unlock()
			rf.applyCh <- msg
			// DPrintf("apply cmd %d shot %d  surplus %v", msg.CommandIndex, msg.SnapshotIndex, rf.applyInfo())
		} else {
			rf.mu.Unlock()
			time.Sleep(applyInterval * time.Millisecond)
		}
	}
}
