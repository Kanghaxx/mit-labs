package kvraft

import (
	"fmt"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	models "6.5840/models1"
	tester "6.5840/tester1"
)

type Value struct {
	value   string
	version uint64
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu   sync.Mutex
	data map[string]Value
}

func (kv *KVServer) DoPut(args rpc.PutArgs) rpc.PutReply {
	var reply rpc.PutReply = rpc.PutReply{}
	reply.Err = rpc.OK
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.data[args.Key]
	if !ok {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return reply
		}
		kv.data[args.Key] = Value{
			value:   args.Value,
			version: 1,
		}
		return reply
	}

	if value.version != uint64(args.Version) {
		reply.Err = rpc.ErrVersion
		return reply
	}
	kv.data[args.Key] = Value{
		value:   args.Value,
		version: value.version + 1,
	}
	return reply
}

func (kv *KVServer) DoGet(args rpc.GetArgs) rpc.GetReply {
	var reply rpc.GetReply = rpc.GetReply{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.data[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return reply
	}
	reply.Value = value.value
	reply.Version = rpc.Tversion(value.version)
	reply.Err = rpc.OK
	return reply
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case rpc.GetArgs:
		return kv.DoGet(args)
	case rpc.PutArgs:
		return kv.DoPut(args)
	default:
		panic(fmt.Sprintf("Unknown command type %T", args))
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	args_val := *args
	err, res := kv.rsm.Submit(args_val)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep := res.(rpc.GetReply)
	reply.Err = rep.Err
	reply.Value = rep.Value
	reply.Version = rep.Version
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	args_val := *args
	err, res := kv.rsm.Submit(args_val)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep := res.(rpc.PutReply)
	reply.Err = rep.Err
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
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rsm.OpID{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(models.KvInput{})
	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	kv.data = make(map[string]Value)

	return []tester.IService{kv, kv.rsm.Raft()}
}
