package rsm

import (
	"container/list"
	"fmt"
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
	lastId int
	queue  *list.List
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
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		queue:        list.New(),
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
	for applyOp := range rsm.applyCh {
		op := applyOp.Command.(Op)

		// dequeue and compare op with dequeued items
		var pendingSubmitRequest *SubmitRequest
		rsm.mu.Lock()
		if rsm.queue.Len() > 0 {
			// Peek first and dequeue only if queued.index == applied.index:
			// applied.index == queued.index: dequeue, check IDs equal and call DoOp()
			// applied.index < queued.index: the peer just became a leader and queued 1 submit to the top of the log,
			// 	 but applyCh still applies older committed records from previous leaders.
			//   In this case: skip and only call DoOp()
			// applied.index > queued.index: probably impossible. Log grows monolonically only.
			element := rsm.queue.Front()
			val := element.Value // peek
			pendingSubmitRequest = val.(*SubmitRequest)
			// TODO compare terms of queued and applied ops and if they differ, cancel all pending ops with that term (or <=)
			if applyOp.CommandIndex == pendingSubmitRequest.CommandIndex {
				rsm.queue.Remove(element) // dequeue if applying the same command
			} else if applyOp.CommandIndex < pendingSubmitRequest.CommandIndex {
				DPrintf("RSM node%d: READER: skipping applyOp.index < queued.index. applyOp.ID=%v applyOp.index=%d; queued.id=%v queued.index=%d",
					rsm.me, op.ID, applyOp.CommandIndex, pendingSubmitRequest.Operation.ID, pendingSubmitRequest.CommandIndex)
			} else {
				panic(fmt.Sprintf("RSM node%d: READER: detected applyOp.index > queued.index. applyOp.ID=%v applyOp.index=%d; queued.id=%v queued.index=%d",
					rsm.me, op.ID, applyOp.CommandIndex, pendingSubmitRequest.Operation.ID, pendingSubmitRequest.CommandIndex))
			}
		}
		rsm.mu.Unlock()

		// call DoOp() here because followers don't have pending Submit goroutines
		result := rsm.sm.DoOp(op.Command)
		DPrintf("RSM node%d: READER: DoOp executed. applyOp.ID=%v applyOp.index=%d", rsm.me, op.ID, applyOp.CommandIndex)

		if pendingSubmitRequest != nil && applyOp.CommandIndex == pendingSubmitRequest.CommandIndex {
			// compare applied with queued
			if op.ID == pendingSubmitRequest.Operation.ID {
				pendingSubmitRequest.ReplyCh <- SubmitResponse{Result: result, Error: rpc.OK}
				DPrintf("RSM node%d: READER: sending OK response. applyOp.ID=%v applyOp.index=%d; queued.id=%v queued.index=%d",
					rsm.me, op.ID, applyOp.CommandIndex, pendingSubmitRequest.Operation.ID, pendingSubmitRequest.CommandIndex)
			} else {
				pendingSubmitRequest.ReplyCh <- SubmitResponse{Result: nil, Error: rpc.ErrWrongLeader}
				DPrintf("RSM node%d: READER: sending ErrWrongLeader response. applyOp.ID=%v applyOp.index=%d; queued.id=%v queued.index=%d",
					rsm.me, op.ID, applyOp.CommandIndex, pendingSubmitRequest.Operation.ID, pendingSubmitRequest.CommandIndex)
			}
		}
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
	DPrintf("RSM node%d: Start executed. index=%d, term=%d, isLeader=%v", rsm.me, index, term, isLeader)
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
	element := rsm.queue.PushBack(r) // maybe min heap? so result could be added outside lock (probably not)
	rsm.mu.Unlock()

	val := element.Value
	pendingSubmitRequest := val.(*SubmitRequest)

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	DPrintf("RSM node%d: waiting to complete for index=%d op.ID=%v", rsm.me, index, op.ID)
	for {
		select {
		case response := <-r.ReplyCh:
			DPrintf("RSM node%d: Submit received result for op.ID=%v: error=%v result=%v", rsm.me, op.ID, response.Error, response.Result)
			return response.Error, response.Result
		case <-ticker.C: // probably better to cancel submits from reader goroutine: if command with newer term detected. But need to add term into command
			newTerm, _ := rsm.rf.GetState()
			if pendingSubmitRequest.CommandTerm < newTerm {
				rsm.mu.Lock()
				if rsm.queue.Len() > 0 {
					DPrintf("RSM node%d: Submit canceled for op.ID=%v term=%d due to new term=%d", rsm.me, op.ID, pendingSubmitRequest.CommandTerm, newTerm)
					rsm.queue.Remove(element) // probably race condition with reader goroutine
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
	for rsm.queue.Len() > 0 {
		element := rsm.queue.Front()
		val := rsm.queue.Remove(element)
		pendingSubmitRequest := val.(*SubmitRequest)
		pendingSubmitRequest.ReplyCh <- SubmitResponse{Result: nil, Error: rpc.ErrWrongLeader}
		//close(pendingSubmitRequest.ReplyCh) // ?
	}
	rsm.mu.Unlock()
	DPrintf("RSM node%d: Kill completed", rsm.me)
}
