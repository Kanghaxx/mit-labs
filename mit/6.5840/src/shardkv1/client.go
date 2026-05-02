package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

// this clerk provides get-put interface to a whole cluster for a client and abstracts-out details like shard groups and shards.
// it manages a set of low level shard group clerks, each of which is designated to control a single shard group
// to 1) send get and put to a shard group, and 2) control data partition movements between groups.
// to determive which group is responsible for which shard (partition), this clerk utilized controller client to query this info from the kvsrv instance (DB-like service).
// most probably this clerk doesn't control shards (paritions) which is done from controller. This clerk is only for get-put calls from a client.

import (
	"sync"
	"time"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	tester "6.5840/tester1"
)

type Clerk struct {
	mu               sync.Mutex
	clnt             *tester.Clnt
	controller       *shardctrler.ShardCtrler
	shardGroupClerks map[tester.Tgid]*shardgrp.Clerk // TODO map is empty after restart. Probably need to query config and fill the map. And then maintain it after config refresh.
	// You will have to modify this struct.

	// TODO probaly cache queried configs per group (or cache latest config). and if wrongGroup error received, refresh cache
	config *shardcfg.ShardConfig
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:       clnt,
		controller: sck,
	}
	ck.shardGroupClerks = make(map[tester.Tgid]*shardgrp.Clerk)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	rck, ok := ck.shardGroupClerks[gid]
	return rck, ok
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	//log.Printf("Get")
	shardNumber := shardcfg.Key2Shard(key)
	for {
		ck.config = ck.controller.Query()
		groupId, servers, _ := ck.config.GidServers(shardNumber)
		shardClerk := shardgrp.MakeClerk(ck.clnt, servers)

		value, version, error := shardClerk.Get(key)
		if error == rpc.ErrWrongGroup {
			//log.Printf("Get: received ErrWrongGroup")
			time.Sleep(20 * time.Millisecond)
			continue
		}
		ck.mu.Lock()
		ck.shardGroupClerks[groupId] = shardClerk
		ck.mu.Unlock()
		return value, version, error
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	//log.Printf("Put")
	shardNumber := shardcfg.Key2Shard(key)
	for {
		//log.Printf("Put: groupId=%v servers=%v", groupId, servers)
		ck.config = ck.controller.Query()
		groupId, servers, _ := ck.config.GidServers(shardNumber)
		shardClerk := shardgrp.MakeClerk(ck.clnt, servers)

		error := shardClerk.Put(key, value, version)
		if error == rpc.ErrWrongGroup {
			//log.Printf("Put: received ErrWrongGroup")ck.config = nil
			time.Sleep(20 * time.Millisecond)
			continue
		}

		ck.mu.Lock()
		ck.shardGroupClerks[groupId] = shardClerk
		ck.mu.Unlock()
		return error
	}
}
