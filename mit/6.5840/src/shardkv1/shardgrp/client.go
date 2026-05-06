package shardgrp

// this is a low level clerk that controls a single shard group (replica set) and provides interface for:
// 1) get and put keys into a shard group
// 2) move shards (replicas) between groups: freeze&get, install, delete shard
// set of these clerks must be controlled from outside to achieve an abstraction from cluster details to make the sharded cluster look like a single DB.
// this is done from shardkv/Clerk, which provides get-put interface to a whole cluster for a client and abstracts-out details like shard groups and shards.

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

var clerkIdGen int // for logging
var muClerk sync.Mutex

type Clerk struct {
	mu sync.Mutex
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
	id        int
	requestId int // for debug logging

}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	// copy servers slice to avoid aliases
	srvCopy := make([]string, len(servers))
	copy(srvCopy, servers)
	ck := &Clerk{Clnt: clnt, servers: srvCopy}
	muClerk.Lock()
	defer muClerk.Unlock()
	ck.id = clerkIdGen // for debug logging
	clerkIdGen++

	// AI slop
	// initialize leader index
	ck.leader = 0
	ck.requestId = 0
	// ensure client's allowed server list includes these servers
	if ck.Clnt != nil {
		// ConnectTo will enable connections to the given servers
		//ck.Clnt.ConnectTo(srvCopy)
	}
	return ck
}

func (ck *Clerk) Leader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	ck.mu.Lock()
	server := ck.leader
	ck.mu.Unlock()
	attempt := 0
	for {
		reply := &rpc.GetReply{}
		ck.mu.Lock()
		requestId := ck.requestId // for debug logging only
		ck.requestId++
		ck.mu.Unlock()

		DPrintf("Client%d: calling Get, requestId=%d attempt=%d ...", ck.id, requestId, attempt)
		result := ck.Clnt.Call(ck.servers[server], "KVServer.Get", &rpc.GetArgs{Key: key}, reply)
		if result && (reply.Err != rpc.ErrWrongLeader) {
			DPrintf("Client%d: Get completed, requestId=%d attempt=%d. reply.Value=%v, reply.Version=%v, reply.Err=%v", ck.id, requestId, attempt, reply.Value, reply.Version, reply.Err)
			ck.mu.Lock()
			ck.leader = server
			ck.mu.Unlock()
			return reply.Value, reply.Version, reply.Err
		}

		if result { // debug logging
			DPrintf("Client%d: Get ErrWrongLeader, requestId=%d attempt=%d", ck.id, requestId, attempt)
		} else {
			DPrintf("Client%d: Get timeout, requestId=%d attempt=%d", ck.id, requestId, attempt)
		}

		// limiting attempts to some N is buggy: there could be more than N servers in the group and attempts could run out too early
		attempt++
		if (attempt > len(ck.servers)*3) && (!result) {
			return "", 0, rpc.ErrWrongGroup
		}

		server = (server + 1) % len(ck.servers)
		time.Sleep(10 * time.Millisecond)
	}
}

// TODO move to a generic method that executes a func and updates leader or does retries
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	attempt := 0
	ck.mu.Lock()
	server := ck.leader
	ck.mu.Unlock()
	for {
		reply := &rpc.PutReply{}
		ck.mu.Lock()
		requestId := ck.requestId // for debug logging only
		ck.requestId++
		ck.mu.Unlock()

		DPrintf("Client%d: calling Put, requestId=%d attempt=%d ...", ck.id, requestId, attempt)
		result := ck.Clnt.Call(ck.servers[server], "KVServer.Put", &rpc.PutArgs{Key: key, Value: value, Version: version}, reply)
		if result && (reply.Err != rpc.ErrWrongLeader) {
			ck.mu.Lock()
			ck.leader = server
			ck.mu.Unlock()

			if attempt > 0 {
				switch reply.Err {
				case rpc.ErrVersion:
					DPrintf("Client%d: Put completed, requestId=%d attempt=%d. reply.Err=%v, returning ErrMaybe", ck.id, requestId, attempt, reply.Err)
					return rpc.ErrMaybe
				case rpc.ErrWrongGroup:
					DPrintf("Client%d: Put completed, requestId=%d attempt=%d. reply.Err=%v, returning ErrWrongGroupRetried", ck.id, requestId, attempt, reply.Err)
					return rpc.ErrWrongGroupRetried // any subsequent ErrVersion will be treated as ErrMaybe by the calling clerk
				}
			}
			DPrintf("Client%d: Put completed, requestId=%d attempt=%d. reply.Err=%v", ck.id, requestId, attempt, reply.Err)
			return reply.Err
		}

		attempt++
		if (attempt > len(ck.servers)*3) && !result {
			return rpc.ErrWrongGroupRetried
		}

		server = (server + 1) % len(ck.servers)
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	ck.mu.Lock()
	server := ck.leader
	ck.mu.Unlock()
	for {
		DPrintf("Client%d: calling FreezeShard", ck.id)

		reply := &shardrpc.FreezeShardReply{}

		//DPrintfImp("Client%d: calling Get, requestId=%d attempt=%d ...", ck.id, requestId, attempt)
		result := ck.Clnt.Call(ck.servers[server], "KVServer.FreezeShard", &shardrpc.FreezeShardArgs{Shard: s, Num: num}, reply)
		if result && (reply.Err != rpc.ErrWrongLeader) {
			//DPrintfImp("Client%d: Get completed, requestId=%d attempt=%d. reply.Value=%v, reply.Version=%v, reply.Err=%v", ck.id, requestId, attempt, reply.Value, reply.Version, reply.Err)
			ck.mu.Lock()
			ck.leader = server
			ck.mu.Unlock()
			if reply.Num > num { // trying to signal the controller to exit
				DPrintf("Client%d: FreezeShard fenced out due to Num mismatch", ck.id)
				return nil, rpc.ErrWrongGroup // ErrMaybe or ErrWrongGroup or custom error?
			}
			return reply.State, reply.Err
		}
		server = (server + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	ck.mu.Lock()
	server := ck.leader
	ck.mu.Unlock()
	for {
		DPrintf("Client%d: calling InstallShard", ck.id)

		reply := &shardrpc.InstallShardReply{}

		//DPrintfImp("Client%d: calling Get, requestId=%d attempt=%d ...", ck.id, requestId, attempt)
		result := ck.Clnt.Call(ck.servers[server], "KVServer.InstallShard", &shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}, reply)
		if result && (reply.Err != rpc.ErrWrongLeader) {
			//DPrintfImp("Client%d: Get completed, requestId=%d attempt=%d. reply.Value=%v, reply.Version=%v, reply.Err=%v", ck.id, requestId, attempt, reply.Value, reply.Version, reply.Err)
			ck.mu.Lock()
			ck.leader = server
			ck.mu.Unlock()
			// TODO reply doesn't contain Num. Add it there of Num in the Freeze reply is enough?
			// if reply.Num > num { // trying to signal the controller to exit
			// 	return rpc.ErrWrongGroup // ErrMaybe or ErrWrongGroup or custom error?
			// }
			return reply.Err
		}
		server = (server + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	ck.mu.Lock()
	server := ck.leader
	ck.mu.Unlock()
	for {
		DPrintf("Client%d: calling DeleteShard", ck.id)

		reply := &shardrpc.DeleteShardReply{}

		//DPrintfImp("Client%d: calling Get, requestId=%d attempt=%d ...", ck.id, requestId, attempt)
		result := ck.Clnt.Call(ck.servers[server], "KVServer.DeleteShard", &shardrpc.DeleteShardArgs{Shard: s, Num: num}, reply)
		if result && (reply.Err != rpc.ErrWrongLeader) {
			//DPrintfImp("Client%d: Get completed, requestId=%d attempt=%d. reply.Value=%v, reply.Version=%v, reply.Err=%v", ck.id, requestId, attempt, reply.Value, reply.Version, reply.Err)
			ck.mu.Lock()
			ck.leader = server
			ck.mu.Unlock()
			return reply.Err
		}
		server = (server + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
