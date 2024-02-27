package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type LastReply struct {
	reqId int
	value string
}

type KVServer struct {
	mu              sync.Mutex
	db              map[string]string
	lastReply map[int]LastReply
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if v, ok := kv.db[args.Key]; ok {
		reply.Value = v
	} else {
		reply.Value = ""
	}
	defer kv.mu.Unlock()
	// Your code here.
}

func (kv *KVServer) getDulReplyValue(clientId, reqId int) (string, bool) {
	reply, ok:=kv.lastReply[clientId]
	if ok && reply.reqId == reqId {
		return reply.value, true
	} else {
		return "", false
	}
}

func (kv *KVServer) updateReq(clientId, reqId int, value string) {
	kv.lastReply[clientId] = LastReply{reqId, value}
}
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if _, isDul:= kv.getDulReplyValue(args.ClientId, args.ReqId);!isDul {
		kv.db[args.Key] = args.Value
		kv.updateReq(args.ClientId, args.ReqId, "")
	}
	defer kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	v, ok := kv.db[args.Key]
	if prevV, isDul:= kv.getDulReplyValue(args.ClientId, args.ReqId);!isDul {
		if ok {
			reply.Value = v
		} else {
			reply.Value = ""
		}
		kv.updateReq(args.ClientId, args.ReqId, v)
		kv.db[args.Key] = reply.Value + args.Value
	} else {
		reply.Value = prevV
	}
	defer kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.db = make(map[string]string)
	kv.lastReply = make(map[int]LastReply)
	// You may need initialization code here.
	return kv
}
