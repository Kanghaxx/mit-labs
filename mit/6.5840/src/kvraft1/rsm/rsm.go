package rsm

import (
	"container/list"
	"log"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const Debug = false

var useRaftStateMachine bool // to plug in another raft besided raft1

type OpID struct {
	ServerId         int
	SequentialNumber int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID      OpID
	Command any
}

type SubmitRequest struct {
	ReplyCh      chan SubmitResponse
	Operation    Op
	CommandIndex int
	CommandTerm  int
}

type SubmitResponse struct {
	Result any
	Error  rpc.Err
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	lastId         int
	pendingSubmits *list.List
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	raft.DPrintf("RSM node%d: starting", me)
	rsm := &RSM{
		me:             me,
		maxraftstate:   maxraftstate,
		applyCh:        make(chan raftapi.ApplyMsg),
		sm:             sm,
		pendingSubmits: list.New(),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
		go rsm.startReader()
	}
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
func (rsm *RSM) startReader() {
	//log.Printf("RSM node%d: starting reader", rsm.me)
	for applyMsg := range rsm.applyCh {
		// AI slop:
		// Handle snapshot messages first
		// if applyMsg.SnapshotValid {
		// 	log.Printf("RSM node%d: READER: received snapshot. index=%d term=%d", rsm.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)
		// 	// Restore snapshot into state machine. Do this outside of the pendingSubmits lock
		// 	rsm.sm.Restore(applyMsg.Snapshot)
		// 	// We don't try to reconcile pending submits here; the existing reader logic
		// 	// will handle pending submits as new ApplyMsg entries arrive.
		// 	continue
		// }
		// // Ignore non-command messages
		// if !applyMsg.CommandValid {
		// 	log.Printf("RSM node%d: READER: received non-command ApplyMsg, skipping", rsm.me)
		// 	continue
		// }

		// // Be defensive: Command may be nil or of unexpected type when tests
		// // simulate network partitions/crashes. Avoid panics from type assertions.
		// applyOp, ok := applyMsg.Command.(Op)
		// if !ok {
		// 	log.Printf("RSM node%d: READER: unexpected command type %T (nil?), skipping", rsm.me, applyMsg.Command)
		// 	continue
		// }

		// Ignore non-command messages
		if !applyMsg.CommandValid {
			log.Printf("RSM node%d: READER: received non-command ApplyMsg, skipping", rsm.me)
			continue
		}

		applyOp := applyMsg.Command.(Op)

		// call DoOp() here because followers don't have pending Submit goroutines
		result := rsm.sm.DoOp(applyOp.Command)
		DPrintf("RSM node%d: READER: DoOp executed. applyOp.ID=%v applyOp.index=%d", rsm.me, applyOp.ID, applyMsg.CommandIndex)

		// dequeue and compare op with dequeued items
		rsm.mu.Lock()
		if rsm.pendingSubmits.Len() > 0 {
			// Peek first and dequeue only if queued.index == applied.index:
			// applied.index == queued.index: dequeue, check IDs equal and call DoOp()
			// applied.index < queued.index: the peer just became a leader and queued 1 submit to the top of the log,
			// 	 but applyCh still applies older committed records from previous leaders.
			//   In this case: skip and only call DoOp()
			// applied.index > queued.index: probably impossible. Log grows monolonically only.
			pendingSubmit := rsm.pendingSubmits.Front() // peek
			pendingSubmitData := pendingSubmit.Value.(*SubmitRequest)
			if applyMsg.CommandIndex == pendingSubmitData.CommandIndex {
				if applyOp.ID == pendingSubmitData.Operation.ID {
					// received the same command that was added at that index: dequeue it and reply OK
					rsm.pendingSubmits.Remove(pendingSubmit) // dequeue if applying the same command
					pendingSubmitData.ReplyCh <- SubmitResponse{Result: result, Error: rpc.OK}
					// log.Printf("RSM node%d: READER: sending OK response. applyOp.command=%+v applyOp.command.type=%T applyOp.ID=%v applyOp.index=%d; queueed.command=%+v queued.id=%v queued.index=%d; DoOP.result=%+v DoOP.result.type=%T",
					// 	rsm.me,
					// 	applyOp.Command, applyOp.Command, applyOp.ID, applyMsg.CommandIndex,
					// 	pendingSubmitData.Operation.Command, pendingSubmitData.Operation.ID, pendingSubmitData.CommandIndex,
					// 	result, result)
				} else {
					// received a different command at the same index: cancel all pending submits having term < applyMsg.term
					for pendingSubmit != nil && pendingSubmitData.CommandTerm < applyMsg.CommandTerm {
						pendingSubmitData.ReplyCh <- SubmitResponse{Result: nil, Error: rpc.ErrWrongLeader}
						DPrintf("RSM node%d: READER: sending ErrWrongLeader response. applyOp.ID=%v applyOp.index=%d; queued.id=%v queued.index=%d",
							rsm.me, applyOp.ID, applyMsg.CommandIndex, pendingSubmitData.Operation.ID, pendingSubmitData.CommandIndex)
						rsm.pendingSubmits.Remove(pendingSubmit)
						pendingSubmit = rsm.pendingSubmits.Front() // peek
						if pendingSubmit != nil {
							pendingSubmitData = pendingSubmit.Value.(*SubmitRequest)
						}
					}
				}
			} else if applyMsg.CommandIndex < pendingSubmitData.CommandIndex {
				DPrintf("RSM node%d: READER: skipping applyOp.index < queued.index. applyOp.ID=%v applyOp.index=%d; queued.id=%v queued.index=%d",
					rsm.me, applyOp.ID, applyMsg.CommandIndex, pendingSubmitData.Operation.ID, pendingSubmitData.CommandIndex)
			} else {
				//panic(fmt.Sprintf("RSM node%d: READER: unexpected applyOp.index > queued.index. applyOp.ID=%v applyOp.index=%d; queued.id=%v queued.index=%d",
				//	rsm.me, applyOp.ID, applyMsg.CommandIndex, pendingSubmitData.Operation.ID, pendingSubmitData.CommandIndex))

				// TODO
				// AI:
				//  The comment says "probably impossible. Log grows monotonically only", but it's actually possible in a partitioned network. The issue is:

				// A node is leader and submits commands (indices 1,2,3...)
				// The node gets partitioned and becomes a follower
				// The new leader has already advanced the log beyond where this node was
				// When this node reconnects, Raft applies committed entries it had missed, and those entries can have indices higher than what was at the top of the pendingSubmits queue
				// The fix is to handle this case: when we receive an applied command with an index higher than the oldest pending submit, we should skip the pending submit (since it may have been superseded or the leader changed) and continue. Let me update the code:

				// Summary of Fixes
				// 1. Client Put Logic (client.go)
				// Problem: The retry tracking was incorrect. It only counted failed RPC calls, not all retry attempts to different servers.

				// Solution: Changed attempt to be incremented for every server we try (whether the RPC fails or returns ErrWrongLeader), starting from 0. This ensures we correctly detect resends and return ErrMaybe when appropriate.

				// 2. RSM Reader Logic (rsm.go)
				// Problem: The code panicked when an applied operation's index was higher than the oldest pending submit's index. The comment said this was "probably impossible" but it IS possible in partitioned networks.

				// Solution: Instead of panicking, the code now handles this gracefully by:

				// Recognizing this happens when leadership changes and new log entries are committed
				// Dequeuing all pending submits with indices lower than the current apply index
				// Returning ErrWrongLeader for those submits since they may never be committed
				// This allows the RSM to continue operating correctly during network partitions
				// These fixes ensure that the KVRaft implementation correctly handles network partitions while maintaining consistency between clients and servers.

				// applyMsg.CommandIndex > pendingSubmitData.CommandIndex
				// This can happen in a partitioned network: we queued a submit at some index,
				// but the leader changed and log entries beyond that were committed.
				// We don't know if our submit will ever be committed, so treat it as lost.
				for pendingSubmit != nil && applyMsg.CommandIndex > pendingSubmitData.CommandIndex {
					pendingSubmitData.ReplyCh <- SubmitResponse{Result: nil, Error: rpc.ErrWrongLeader}
					DPrintf("RSM node%d: READER: sending ErrWrongLeader due to gap. applyOp.ID=%v applyOp.index=%d; queued.id=%v queued.index=%d",
						rsm.me, applyOp.ID, applyMsg.CommandIndex, pendingSubmitData.Operation.ID, pendingSubmitData.CommandIndex)
					rsm.pendingSubmits.Remove(pendingSubmit)
					pendingSubmit = rsm.pendingSubmits.Front()
					if pendingSubmit != nil {
						pendingSubmitData = pendingSubmit.Value.(*SubmitRequest)
					}
				}
			}
		}
		rsm.mu.Unlock()

	}
	rsm.Kill()
	DPrintf("RSM node%d: reader exit", rsm.me)
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	DPrintf("RSM node%d: Submit call. req=%v", rsm.me, req)

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()
	// ID
	// autoinc int: bad idea because it could be same on different peers.
	// struct {me, autoinc int}: what if server reloads?
	// guid? OK but inefficient (AI)
	rsm.lastId++
	op := Op{
		ID:      OpID{rsm.me, rsm.lastId},
		Command: req,
	}
	DPrintf("RSM node%d: calling Start for ID=%v...", rsm.me, op.ID)
	index, term, isLeader := rsm.rf.Start(op)
	// log.Printf("RSM node%d: Start executed for op.ID=%v op.command=%+v. index=%d, term=%d, isLeader=%v", rsm.me, op.ID, op.Command, index, term, isLeader)
	if !isLeader {
		DPrintf("RSM node%d: not a leader, returning error. index=%d, term=%d, isLeader=%v", rsm.me, index, term, isLeader)
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}
	r := &SubmitRequest{
		ReplyCh:      make(chan SubmitResponse, 1),
		Operation:    op,
		CommandIndex: index,
		CommandTerm:  term,
	}
	pendingSubmit := rsm.pendingSubmits.PushBack(r)
	rsm.mu.Unlock()

	DPrintf("RSM node%d: waiting to complete for index=%d op.ID=%v", rsm.me, index, op.ID)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case response := <-r.ReplyCh:
			DPrintf("RSM node%d: Submit received result for op.ID=%v: error=%v result=%v", rsm.me, op.ID, response.Error, response.Result)
			return response.Error, response.Result
		// fallback: check current peer state if the cluster is inactive and reader-goroutine can't remove pending submits due to no new applied commands in applyCh
		// probably not needed: if there are no new entries applied, we couldn't know if the added entries are going to be committed or deleted from raft's log.
		//   so it's OK to wait until the cluster recovers
		case <-ticker.C:
			pendingSubmitData := pendingSubmit.Value.(*SubmitRequest)
			newTerm, _ := rsm.rf.GetState()
			if pendingSubmitData.CommandTerm < newTerm {
				rsm.mu.Lock()
				if rsm.pendingSubmits.Len() > 0 {
					DPrintf("RSM node%d: Submit canceled due timeout for op.ID=%v term=%d due to new term=%d", rsm.me, op.ID, pendingSubmitData.CommandTerm, newTerm)
					rsm.pendingSubmits.Remove(pendingSubmit) // seems to be safe: if already removed from reader the call does nothing
					rsm.mu.Unlock()
					return rpc.ErrWrongLeader, nil
				}
				rsm.mu.Unlock()
			}
		}
	}
}

func (rsm *RSM) Kill() {
	DPrintf("RSM node%d: Kill started", rsm.me)
	rsm.mu.Lock()
	for rsm.pendingSubmits.Len() > 0 {
		element := rsm.pendingSubmits.Front()
		val := rsm.pendingSubmits.Remove(element)
		pendingSubmitRequest := val.(*SubmitRequest)
		pendingSubmitRequest.ReplyCh <- SubmitResponse{Result: nil, Error: rpc.ErrWrongLeader}
		//close(pendingSubmitRequest.ReplyCh) // ?
	}
	rsm.mu.Unlock()
	DPrintf("RSM node%d: Kill completed", rsm.me)
}
