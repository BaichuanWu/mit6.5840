package raft

type SnapshotLog struct {
	LastTerm  int
	LastIndex int
	State     map[string]interface{}
	Log       []LogEntry
}

func (lg *SnapshotLog) at(index int) LogEntry {
	return lg.Log[index-lg.LastIndex]
}

func (lg *SnapshotLog) len() int {
	return lg.LastIndex + len(lg.Log)
}

func (lg *SnapshotLog) slice(start, end int) []LogEntry {
	return lg.Log[start-lg.LastIndex : end-lg.LastIndex]
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

}
