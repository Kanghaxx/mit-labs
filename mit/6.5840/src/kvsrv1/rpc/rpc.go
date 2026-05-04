package rpc

type Err string

const (
	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader       = "ErrWrongLeader"
	ErrWrongGroup        = "ErrWrongGroup"
	ErrWrongGroupRetried = "ErrWrongGroupRetried"
)

type Tversion uint64

type PutArgs struct {
	Key     string
	Value   string
	Version Tversion
}

type GenericReply interface {
	GetErr() Err
}

type PutReply struct {
	Err Err
}

func (reply *PutReply) GetErr() Err {
	return reply.Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}

func (reply *GetReply) GetErr() Err {
	return reply.Err
}
