package kvraft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"maps"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type Value struct {
	Value   string
	Version uint64
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	mu   sync.Mutex
	data map[string]Value
}

type Snapshot struct {
	Data map[string]Value
}

func (kv *KVServer) applyPut(args rpc.PutArgs) rpc.PutReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//log.Printf("KV node%d: KV before Put apply: %v", kv.me, kv.data)
	//defer log.Printf("KV node%d: KV after Put apply: %v", kv.me, kv.data)
	value, exists := kv.data[args.Key]
	if !exists {
		if args.Version != 0 {
			return rpc.PutReply{Err: rpc.ErrNoKey}
		}
		kv.data[args.Key] = Value{
			Value:   args.Value,
			Version: 1,
		}
		return rpc.PutReply{Err: rpc.OK}
	}

	if value.Version != uint64(args.Version) {
		return rpc.PutReply{Err: rpc.ErrVersion}
	}
	kv.data[args.Key] = Value{
		Value:   args.Value,
		Version: value.Version + 1,
	}
	return rpc.PutReply{Err: rpc.OK}
}

func (kv *KVServer) applyGet(args rpc.GetArgs) rpc.GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exists := kv.data[args.Key]
	if !exists {
		return rpc.GetReply{Err: rpc.ErrNoKey}
	}
	return rpc.GetReply{
		Value:   value.Value,
		Version: rpc.Tversion(value.Version),
		Err:     rpc.OK,
	}
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case rpc.GetArgs:
		return kv.applyGet(args)
	case rpc.PutArgs:
		res := kv.applyPut(args)
		return res
	default:
		panic(fmt.Sprintf("Unknown command type %T", args))
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	snapshot := Snapshot{
		Data: make(map[string]Value),
	}
	maps.Copy(snapshot.Data, kv.data)
	encoder.Encode(snapshot)
	return buffer.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//log.Printf("KV node%d: KV before snapshot restore: %v", kv.me, kv.data)
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	var snapshot Snapshot
	err := decoder.Decode(&snapshot)
	if err != nil {
		panic(fmt.Sprintf("ERROR while decoding state: %v", err))
	}
	kv.data = snapshot.Data
	//log.Printf("KV node%d:KV after snapshot restore: %v", kv.me, kv.data)
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	//labgob.Register(rsm.OpID{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(Value{})

	kv := &KVServer{me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// You may need initialization code here.
	if kv.data == nil {
		kv.data = make(map[string]Value)
	}

	//log.Printf("KV node%d started.", kv.me)
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
