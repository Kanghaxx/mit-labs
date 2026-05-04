package shardgrp

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"maps"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

const Debug = false
const DebugServer = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
func DPrintfS(format string, a ...interface{}) {
	if DebugServer {
		log.Printf(format, a...)
	}
}

type Value struct {
	Value   string
	Version uint64
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// Your code here
	mu           sync.Mutex
	shards       map[shardcfg.Tshid]map[string]Value
	activeShards map[shardcfg.Tshid]bool
	Num          shardcfg.Tnum
}

type Snapshot struct {
	Shards       map[shardcfg.Tshid]map[string]Value
	ActiveShards map[shardcfg.Tshid]bool
}

func (kv *KVServer) shardExists(shard shardcfg.Tshid) bool {
	_, ok := kv.activeShards[shard]
	return ok
}

func (kv *KVServer) isShardActive(shard shardcfg.Tshid) bool {
	value, ok := kv.activeShards[shard]
	if !ok {
		return false
	}
	return value
}

func (kv *KVServer) applyPut(args rpc.PutArgs) rpc.PutReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//log.Printf("KV node%d: KV before Put apply: %v", kv.me, kv.data)
	//defer log.Printf("KV node%d: KV after Put apply: %v", kv.me, kv.data)

	sid := shardcfg.Key2Shard(args.Key)
	// Reply ErrWrongGroup if server is not responsible for the key or if the shard is frozen
	if !kv.isShardActive(sid) {
		return rpc.PutReply{Err: rpc.ErrWrongGroup}
	}
	value, exists := kv.shards[sid][args.Key]
	if !exists {
		if args.Version != 0 {
			return rpc.PutReply{Err: rpc.ErrNoKey}
		}
		DPrintfS("KV node%d: applyPut installing new key %s versionReq=%d -> set version=1 value=%s", kv.me, args.Key, args.Version, args.Value)
		kv.shards[sid][args.Key] = Value{
			Value:   args.Value,
			Version: 1,
		}
		return rpc.PutReply{Err: rpc.OK}
	}

	if value.Version != uint64(args.Version) {
		DPrintfS("KV node%d: applyPut ErrVersion for key %s. stored.version=%d req.version=%d", kv.me, args.Key, value.Version, args.Version)
		return rpc.PutReply{Err: rpc.ErrVersion}
	}
	DPrintfS("KV node%d: applyPut updating key %s from version=%d to version=%d with value=%s", kv.me, args.Key, value.Version, value.Version+1, args.Value)
	kv.shards[sid][args.Key] = Value{
		Value:   args.Value,
		Version: value.Version + 1,
	}

	return rpc.PutReply{Err: rpc.OK}
}

func (kv *KVServer) applyGet(args rpc.GetArgs) rpc.GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sid := shardcfg.Key2Shard(args.Key)
	// Reply ErrWrongGroup if server is not responsible for the key
	if !kv.isShardActive(sid) {
		//if !kv.shardExists(sid) { // if continue to serve Gets on frozen shards, looks like it causes "history not linearizable" errors more frequently
		return rpc.GetReply{Err: rpc.ErrWrongGroup}
	}
	value, exists := kv.shards[sid][args.Key]
	if !exists {
		return rpc.GetReply{Err: rpc.ErrNoKey}
	}
	return rpc.GetReply{
		Value:   value.Value,
		Version: rpc.Tversion(value.Version),
		Err:     rpc.OK,
	}
}

func (kv *KVServer) applyFreeze(args shardrpc.FreezeShardArgs) shardrpc.FreezeShardReply {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.shardExists(args.Shard) { // might be deleted
		DPrintfS("Server[%d] in Group[%d]: Freeze: shard[%d] doesn't exist", kv.me, kv.gid, args.Shard)
		return shardrpc.FreezeShardReply{
			Err:   rpc.OK,
			State: nil,
			Num:   kv.Num,
		}
	}

	if kv.Num > args.Num { // there will be multiple calls with a single Num: Num is passed in new config to ChangeConfigTo, which could move multiple shards
		DPrintfS("Server[%d] in Group[%d]: Freeze: Num mismatch for shard[%d]", kv.me, kv.gid, args.Shard)
		return shardrpc.FreezeShardReply{
			Err:   rpc.OK, // looks like it's safe to return OK
			State: nil,
			Num:   kv.Num,
		}
	}
	kv.Num = args.Num

	kv.activeShards[args.Shard] = false // freeze shard. Set to false, so Puts will be rejected. Gets will still be served until the shard is completely deleted.

	// encode only the frozen shard's map and return it in reply, so the client could send it in subsequent InstallShard RPC
	shardState := make(map[string]Value)
	maps.Copy(shardState, kv.shards[args.Shard])
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(shardState)
	DPrintfS("Server[%d] in Group[%d]: Freeze: shard[%d] frozen. kv.shards[%d]=%v", kv.me, kv.gid, args.Shard, args.Shard, kv.shards[args.Shard])
	return shardrpc.FreezeShardReply{
		Err:   rpc.OK,
		State: buffer.Bytes(),
		Num:   kv.Num, // probably the cliend could compare Num if it receives OK response
	}
}

func (kv *KVServer) applyInstall(args shardrpc.InstallShardArgs) shardrpc.InstallShardReply {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// If shard data is not empty, the call MUST be ignored.
	// Case:
	// Controller1:
	// 1. Freeze shard1 in g1
	// 2. Install shard1 to g2
	// 3. Delete shard1 from g1 - removes shard1 map from data.
	// Controller1 crashes without updating current config in kvsrv.
	// Controller2: gets the current config. Retries the whole process.
	// 1. Freeze shard1 in g1 - sets owned[s1] = false AND RETURNS EMPTY MAP
	// 2. Install shard1 to g2 - SETS TO EMPTY MAP
	//    Fix: install MUST check if shard is not empty. If so, it ignores the call.
	// 3. Delete shard1 from g1 - already removed.
	//
	// ! Problem: what if Num increased?
	// A new config is sent, but the provious transfer operation isn't conpleted yet?
	//
	if _, ok := kv.shards[args.Shard]; ok {
		DPrintfS("Server[%d] in Group[%d]: Install: shard[%d] already exists", kv.me, kv.gid, args.Shard)
		return shardrpc.InstallShardReply{Err: rpc.OK} // OK or ErrWrongGroup?
	}

	if kv.Num > args.Num {
		DPrintfS("Server[%d] in Group[%d]: Install: Num mismatch for shard[%d]", kv.me, kv.gid, args.Shard)
		return shardrpc.InstallShardReply{Err: rpc.OK}
	}
	kv.Num = args.Num

	buffer := bytes.NewBuffer(args.State)
	decoder := gob.NewDecoder(buffer)
	//var shardState InstallShardState // make internal map?
	var shardState map[string]Value
	err := decoder.Decode(&shardState)
	if err != nil {
		panic(fmt.Sprintf("ERROR while decoding shard state: %v", err))
	}

	kv.shards[args.Shard] = shardState // install shard data
	kv.activeShards[args.Shard] = true
	DPrintfS("Server[%d] in Group[%d]: Install: shard[%d] installed. kv.shards[%d]=%v", kv.me, kv.gid, args.Shard, args.Shard, kv.shards[args.Shard])

	return shardrpc.InstallShardReply{Err: rpc.OK}
}

func (kv *KVServer) applyDelete(args shardrpc.DeleteShardArgs) shardrpc.DeleteShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.Num > args.Num {
		DPrintfS("Server[%d] in Group[%d]: Delete: Num mismatch for shard[%d]", kv.me, kv.gid, args.Shard)
		return shardrpc.DeleteShardReply{Err: rpc.OK}
	}
	kv.Num = args.Num

	delete(kv.shards, args.Shard) // looks like safe if key isn't present in the map
	delete(kv.activeShards, args.Shard)

	DPrintfS("Server[%d] in Group[%d]: Delete: shard[%d] deleted", kv.me, kv.gid, args.Shard)
	return shardrpc.DeleteShardReply{Err: rpc.OK}
}

func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case rpc.GetArgs:
		return kv.applyGet(args)
	case rpc.PutArgs:
		res := kv.applyPut(args)
		return res
	case shardrpc.FreezeShardArgs:
		return kv.applyFreeze(args)
	case shardrpc.InstallShardArgs:
		return kv.applyInstall(args)
	case shardrpc.DeleteShardArgs:
		return kv.applyDelete(args)
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
		Shards:       kv.shards,
		ActiveShards: kv.activeShards,
	}
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
	kv.shards = snapshot.Shards
	kv.activeShards = snapshot.ActiveShards
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

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	args_val := *args
	err, res := kv.rsm.Submit(args_val)
	if err != rpc.OK && res == nil {
		reply.Err = err
		return
	}
	rep := res.(shardrpc.FreezeShardReply)
	reply.Err = rep.Err
	reply.Num = rep.Num
	reply.State = rep.State
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	args_val := *args
	err, res := kv.rsm.Submit(args_val)
	if err != rpc.OK && res == nil {
		reply.Err = err
		return
	}
	rep := res.(shardrpc.InstallShardReply)
	reply.Err = rep.Err
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	args_val := *args
	err, res := kv.rsm.Submit(args_val)
	if err != rpc.OK && res == nil {
		reply.Err = err
		return
	}
	rep := res.(shardrpc.DeleteShardReply)
	reply.Err = rep.Err
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(Value{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here
	if kv.shards == nil {
		kv.shards = make(map[shardcfg.Tshid]map[string]Value)
		kv.activeShards = make(map[shardcfg.Tshid]bool)
	}

	if gid == shardcfg.Gid1 { // initial group ows all shards. Only then shards are moved out by Controller to other groups
		for i := range shardcfg.NShards {
			kv.shards[shardcfg.Tshid(i)] = make(map[string]Value)
			kv.activeShards[shardcfg.Tshid(i)] = true
		}
	}

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
