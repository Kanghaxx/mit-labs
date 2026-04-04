package kvraft

import (
	"log"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

var clerkIdGen int // for logging
var muClerk sync.Mutex

const DebugImp = false

type Clerk struct {
	mu      sync.Mutex
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can add to this struct.
	id        int
	requestId int // for debug logging
}

func DPrintfImp(format string, a ...interface{}) {
	if DebugImp {
		log.Printf(format, a...)
	}
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	muClerk.Lock()
	defer muClerk.Unlock()
	ck.id = clerkIdGen // for debug logging
	clerkIdGen++
	return ck
}

func (ck *Clerk) Leader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leader
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
	ck.mu.Lock()
	server := ck.leader
	ck.mu.Unlock()
	attempt := 0 // for debug logging only
	for {
		reply := &rpc.GetReply{}
		ck.mu.Lock()
		requestId := ck.requestId // for debug logging only
		ck.requestId++
		ck.mu.Unlock()

		DPrintfImp("Client%d: calling Get, requestId=%d attempt=%d ...", ck.id, requestId, attempt)
		result := ck.clnt.Call(ck.servers[server], "KVServer.Get", &rpc.GetArgs{Key: key}, reply)
		if result && (reply.Err != rpc.ErrWrongLeader) {
			DPrintfImp("Client%d: Get completed, requestId=%d attempt=%d. reply.Value=%v, reply.Version=%v, reply.Err=%v", ck.id, requestId, attempt, reply.Value, reply.Version, reply.Err)
			ck.mu.Lock()
			ck.leader = server
			ck.mu.Unlock()
			return reply.Value, reply.Version, reply.Err
		}

		if result { // debug logging
			DPrintfImp("Client%d: Get ErrWrongLeader, requestId=%d attempt=%d", ck.id, requestId, attempt)
		} else {
			DPrintfImp("Client%d: Get timeout, requestId=%d attempt=%d", ck.id, requestId, attempt)
		}
		attempt++
		server = (server + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
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

		DPrintfImp("Client%d: calling Put, requestId=%d attempt=%d ...", ck.id, requestId, attempt)
		result := ck.clnt.Call(ck.servers[server], "KVServer.Put", &rpc.PutArgs{Key: key, Value: value, Version: version}, reply)
		if result && (reply.Err != rpc.ErrWrongLeader) {
			ck.mu.Lock()
			ck.leader = server
			ck.mu.Unlock()
			if attempt > 0 && reply.Err == rpc.ErrVersion {
				DPrintfImp("Client%d: Put completed, requestId=%d attempt=%d. reply.Err=%v, returning ErrMaybe", ck.id, requestId, attempt, reply.Err)
				return rpc.ErrMaybe
			}
			DPrintfImp("Client%d: Put completed, requestId=%d attempt=%d. reply.Err=%v", ck.id, requestId, attempt, reply.Err)
			return reply.Err
		}
		attempt++
		server = (server + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
