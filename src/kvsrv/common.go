package kvsrv

type ClientArgs interface {
	set(clientId, reqId int)
}
type BaseArgs struct {
	ReqId int
	ClientId int
}
func (a *BaseArgs) set (clientId, reqId int){
	a.ReqId = reqId
	a.ClientId = clientId
}

// Put or Append
type PutAppendArgs struct {
	BaseArgs
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	BaseArgs
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
