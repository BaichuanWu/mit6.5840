package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNotCommit   = "ErrNotCommit"
)

type Err string

type ClientArgs interface {
	set(clientId, reqId int)
}
type ClientReply interface {
	next() bool
	create() ClientReply
	clone(ClientReply) ClientReply
}
type BaseArgs struct {
	ReqId    int
	ClientId int
}

func (a *BaseArgs) set(clientId, reqId int) {
	a.ReqId = reqId
	a.ClientId = clientId
}


// Put or Append
type PutAppendArgs struct {
	BaseArgs
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type BaseReply struct {
	ServerId int
	Err Err
}

func (r *BaseReply) next() bool {
	return r.Err == ErrWrongLeader || r.Err == ErrNotCommit
}

func (r *BaseReply) create() ClientReply {
	return &BaseReply{}
}
func (r *BaseReply) clone(reply ClientReply) ClientReply {
	r.Err = reply.(*BaseReply).Err
	return r
}

type PutAppendReply = BaseReply

type GetArgs struct {
	BaseArgs
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	BaseReply
	Value string
}

func (r *GetReply) create() ClientReply {
	return &GetReply{}
}

func (r *GetReply) clone(reply ClientReply) ClientReply {
	r.Err = reply.(*GetReply).Err
	r.Value = reply.(*GetReply).Value
	return r
}
