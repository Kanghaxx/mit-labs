package kvraft

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	mu      sync.Mutex
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leader int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	var reply *rpc.GetReply

	ck.mu.Lock()
	server := ck.leader
	ck.mu.Unlock()
	for {
		reply = &rpc.GetReply{}
		result := ck.clnt.Call(ck.servers[server], "KVServer.Get", &rpc.GetArgs{Key: key}, reply)
		if result && (reply.Err != rpc.ErrWrongLeader) {
			break
		}
		server = (server + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
	ck.mu.Lock()
	ck.leader = server
	ck.mu.Unlock()
	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	var reply *rpc.PutReply
	attempt := 1
	ck.mu.Lock()
	server := ck.leader
	ck.mu.Unlock()
	for {
		reply = &rpc.PutReply{}
		result := ck.clnt.Call(ck.servers[server], "KVServer.Put", &rpc.PutArgs{Key: key, Value: value, Version: version}, reply)
		if result && (reply.Err != rpc.ErrWrongLeader) {
			break
		}
		if !result {
			attempt++
		}
		server = (server + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
	ck.mu.Lock()
	ck.leader = server
	ck.mu.Unlock()
	if attempt > 1 && reply.Err == rpc.ErrVersion {
		return rpc.ErrMaybe
	}
	return reply.Err
}
