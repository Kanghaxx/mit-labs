package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Value struct {
	value   string
	version uint64
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	m map[string]Value
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		m: make(map[string]Value),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.m[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = value.value
	reply.Version = rpc.Tversion(value.version)
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	reply.Err = rpc.OK
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.m[args.Key]
	if !ok {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		kv.m[args.Key] = Value{
			value:   args.Value,
			version: 1,
		}
		return
	}

	if value.version != uint64(args.Version) {
		reply.Err = rpc.ErrVersion
		return
	}
	kv.m[args.Key] = Value{
		value:   args.Value,
		version: value.version + 1,
	}

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
