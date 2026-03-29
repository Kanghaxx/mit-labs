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
					DPrintf("RSM node%d: READER: sending OK response. applyOp.ID=%v applyOp.index=%d; queued.id=%v queued.index=%d",
						rsm.me, applyOp.ID, applyMsg.CommandIndex, pendingSubmitData.Operation.ID, pendingSubmitData.CommandIndex)
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
				panic(fmt.Sprintf("RSM node%d: READER: unexpected applyOp.index > queued.index. applyOp.ID=%v applyOp.index=%d; queued.id=%v queued.index=%d",
					rsm.me, applyOp.ID, applyMsg.CommandIndex, pendingSubmitData.Operation.ID, pendingSubmitData.CommandIndex))
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
