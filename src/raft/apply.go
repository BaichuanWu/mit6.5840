package raft



func (rf *Raft) applyTicker() {
	for !rf.killed() {
		var lastApplied, lastSnapshotApplied int
		rf.mu.Lock()
		rf.applyCond.Wait()
		msgs := make([]ApplyMsg, 0)
		if rf.log.LastIndex > rf.lastSnapshotApplied {
			lastSnapshotApplied = rf.log.LastIndex
			msgs = append(msgs, ApplyMsg{SnapshotValid: true, Snapshot: rf.log.Bytes, SnapshotTerm: rf.log.LastTerm, SnapshotIndex: rf.log.LastIndex})
		} else if rf.lastApplied < rf.commitIndex {
			if rf.lastApplied > rf.log.LastIndex {
				lastApplied = rf.lastApplied
			} else {
				lastApplied = rf.log.LastIndex
			}
			var commitIndex int
			// 存在早发的appendEntry 后发送到follower 导致 Log截断 但commitId不会更新
			if rf.commitIndex > rf.log.len() - 1 { 
				commitIndex = rf.log.len() - 1
			} else {
				commitIndex = rf.commitIndex
			}
			for lastApplied < commitIndex {
				lastApplied++
				msgs = append(msgs, ApplyMsg{CommandValid: true, Command: rf.log.at(lastApplied).Command, CommandIndex: lastApplied})
			}
		}
		rf.mu.Unlock()
		for _, m := range msgs {
			rf.applyCh <- m
		}
		rf.mu.Lock()
		if lastSnapshotApplied > rf.lastSnapshotApplied {
			if rf.commitIndex < rf.log.LastIndex {
				rf.commitIndex = rf.log.LastIndex
				rf.lastApplied = rf.log.LastIndex
			}
			rf.lastSnapshotApplied = lastSnapshotApplied
		} else if lastApplied > rf.lastApplied {
			rf.lastApplied = lastApplied
		}
		rf.mu.Unlock()
	}
}
