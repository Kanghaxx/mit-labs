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
	attempt := 0
	ck.mu.Lock()
	server := ck.leader
	ck.mu.Unlock()
	for {
		reply = &rpc.PutReply{}
		// TODO rewrite
		// looks like ErrWrongLeader could lead to command actually committed. So ErrWrongLeader may be treated as an attempt and if ErrVersion is returned after that, return ErrMaybe
		// AI:
		// In the context of MIT 6.5840 (Spring 2026) Lab 4B, your reasoning is correct. Because a command can be committed even if the server returns ErrWrongLeader, that first contact must be treated as a potential execution attempt. [1]
		// 1. Identify the Ambiguity of ErrWrongLeader
		// When a server returns ErrWrongLeader, it often means it lost its leadership after it might have already replicated your request to a majority. If that request was committed and applied to the state machine, the version number of the key has already changed. [1, 2]
		// 2. Handle the ErrVersion Conflict
		// If your Clerk then retries the same Put (with the same version check) on the new leader, the new leader will see that the version has already advanced (because of your previous "failed" attempt). The new leader will then return ErrVersion. [2, 3]
		// 3. Determine the Final Response
		// At this point, the Clerk cannot distinguish between two scenarios:

		// * Success: Your first attempt (ErrWrongLeader) actually succeeded in committing the value.
		// * External Interference: Another client updated the key between your two attempts, causing your second attempt to fail the version check. [2]

		// Because of this uncertainty, the [Lab 2 rules](https://pdos.csail.mit.edu/6.824/labs/lab-kvsrv1.html) (which Lab 4B explicitly instructs you to follow) require that you return ErrMaybe. [2, 3]
		// Summary of Correct Clerk Logic

		// 1. First RPC: Returns ErrWrongLeader.
		// * Action: Mark this request as "retransmitted" or "retried." Move to the next server.
		// 2. Second RPC (Retry): Returns ErrVersion. [2]
		// * Action: Since this is a retry, you cannot know if the first attempt caused the version mismatch.
		// 	* Result: Return ErrMaybe to the application.

		// Answer
		// Yes, that is correct. Because ErrWrongLeader can occur after a command is committed, it must be treated as a potentially successful attempt. If a subsequent retry returns ErrVersion, the Clerk must return ErrMaybe to reflect the uncertainty of whether the original request was the one that updated the version. [2, 4]
		// Would you like to review the specific duplicate detection implementation at the server level to see how it handles these "zombie" committed requests?

		// [1] [https://pdos.csail.mit.edu](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft1.html)
		// [2] [https://pdos.csail.mit.edu](https://pdos.csail.mit.edu/6.824/labs/lab-kvsrv1.html)
		// [3] [https://pdos.csail.mit.edu](https://pdos.csail.mit.edu/6.824/labs/lab-kvsrv1.html)
		// [4] [https://pdos.csail.mit.edu](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft1.html)

		result := ck.clnt.Call(ck.servers[server], "KVServer.Put", &rpc.PutArgs{Key: key, Value: value, Version: version}, reply)
		if result && (reply.Err != rpc.ErrWrongLeader) {
			break
		}
		attempt++
		server = (server + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
	ck.mu.Lock()
	ck.leader = server
	ck.mu.Unlock()
	if attempt > 0 && reply.Err == rpc.ErrVersion {
		return rpc.ErrMaybe
	}
	return reply.Err
}
