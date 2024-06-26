package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int
	reqId    int
	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = int(nrand())
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) setNextLeader () {
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
}

func (ck *Clerk) Call(svcMeth string, args ClientArgs, reply ClientReply) {
	ck.reqId += 1
	args.set(ck.clientId, ck.reqId)
	for {
			r := reply.create()
			if ck.servers[ck.leaderId].Call(svcMeth, args, r) && !r.next() {
				DPrintf("Client %d to server %d success: req:%v reply %v", ck.clientId, ck.leaderId , args, r)
				reply.clone(r)
				return
			} else {
				// DPrintf("Client fail %d: req:%v reply %v", ck.clientId, args, r)
				ck.setNextLeader()
			}
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}
	reply := GetReply{}
	ck.Call("KVServer.Get", &args, &reply)
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op}
	reply := PutAppendReply{}
	ck.Call("KVServer.PutAppend", &args, &reply)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
