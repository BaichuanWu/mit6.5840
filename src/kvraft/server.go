package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false
const WaitCommitTime = 300 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Name     string
	Key      string
	Value    string
	ReqId    int
	ClientId int
}
type OpReply struct {
	Err   Err
	value string
	op    Op
}

type KVServer struct {
	mu         sync.Mutex
	me         int
	rf         *raft.Raft
	applyCh    chan raft.ApplyMsg
	dead       int32 // set by Kill()
	db         map[string]string
	lastApply  map[int]int
	commitChan map[int]chan OpReply

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *KVServer) startAndWaitCommit(op Op) OpReply {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader || kv.killed() {
		return OpReply{Err: ErrWrongLeader}
	} else {
		opReply := kv.waitCommitChan(index)
		if opReply.op.ReqId == op.ReqId && opReply.op.ClientId == op.ClientId {
			return opReply
		} else {
			return OpReply{Err: ErrNotCommit}
		}
	}
}

func (kv *KVServer) waitCommitChan(index int) OpReply {
	var op OpReply
	kv.mu.Lock()
	commitChan, ok := kv.commitChan[index]
	if !ok {
		commitChan = make(chan OpReply)
		kv.commitChan[index] = commitChan
	}
	kv.mu.Unlock()
	select {
	case op = <-commitChan:
	case <-time.After(WaitCommitTime):
		op = OpReply{Err: ErrNotCommit}
	}
	go kv.deleteCommitChan(index)
	return op
}

func (kv *KVServer) deleteCommitChan(index int) {
	kv.mu.Lock()
	delete(kv.commitChan, index)
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	opReply := kv.startAndWaitCommit(Op{Name: "Get", Key: args.Key, ClientId: args.ClientId, ReqId: args.ReqId})
	reply.Err = opReply.Err
	reply.Value = opReply.value
	reply.ServerId = kv.me
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	lastReqId :=kv.lastApply[args.ClientId] 
	kv.mu.Unlock()
	if lastReqId < args.ReqId {
		OpReply := kv.startAndWaitCommit(Op{Name: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, ReqId: args.ReqId})
		reply.Err = OpReply.Err
	} else {
		reply.Err = OK
	}
	reply.ServerId = kv.me
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.commitChan = make(map[int]chan OpReply)
	kv.lastApply = make(map[int]int)

	go kv.apply()
	// You may need initialization code here.

	return kv
}

func (kv *KVServer) apply() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			value := ""
			op := msg.Command.(Op)
			kv.mu.Lock()
			if op.Name == "Get" {
				value = kv.db[op.Key]
			} else if kv.lastApply[op.ClientId] < op.ReqId {
				if op.Name == "Put" {
					kv.db[op.Key] = op.Value
				} else if op.Name == "Append" {
					kv.db[op.Key] += op.Value
				}
				kv.lastApply[op.ClientId] = op.ReqId
			} 
			kv.mu.Unlock()
			_, isLeader:=kv.rf.GetState();
			DPrintf("server %d leader %v reqId %d apply msg %v value %s", kv.me,isLeader, op.ReqId, msg.Command, value)
			kv.mu.Lock()
			commitChan, ok := kv.commitChan[msg.CommandIndex]
			kv.mu.Unlock()
			if ok && isLeader {
				commitChan <- OpReply{Err: OK, value: value, op: op}
			}
		}
	}
}
