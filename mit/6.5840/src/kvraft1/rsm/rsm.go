package rsm

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const Debug = false
const DebugImp = false

const (
	raftStateShrinkRate = 0.8
)

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

type SnapshotRSMData struct {
	Snapshot          []byte
	LastIncludedIndex int
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
	persister            *tester.Persister
	maxraftstatelimitted int // fraction of maxraftstate
	lastId               int
	lastAppliedIndex     int // 1-indexed
	pendingSubmits       *list.List
	appliedCommands      *list.List // for debug logging
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
		me:                   me,
		maxraftstate:         maxraftstate,
		applyCh:              make(chan raftapi.ApplyMsg),
		sm:                   sm,
		persister:            persister,
		maxraftstatelimitted: int(float64(maxraftstate) * raftStateShrinkRate),
		pendingSubmits:       list.New(),
		appliedCommands:      list.New(),
	}

	if !tester.UseRaftStateMachine {
		rsm.restoreSnapshot(rsm.persister.ReadSnapshot())
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

func DPrintfImp(format string, a ...interface{}) {
	if DebugImp {
		log.Printf(format, a...)
	}
}

func (rsm *RSM) raftStateSizeTooLarge() bool {
	if rsm.maxraftstate == -1 {
		return false
	}
	return rsm.rf.PersistBytes() > rsm.maxraftstatelimitted
	//return rsm.rf.PersistBytes() >= rsm.maxraftstate
}

func (rsm *RSM) restoreSnapshot(data []byte) {
	// on reload:
	// read snapshot from persister
	// deserialize snapshot to retrieve snapshot butes and index
	// pass snapshot to KV and set lastAppliedIndex = index
	// ! drawback: probably tests will fail when trying to read snapshot. Probably not
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	var snapshot SnapshotRSMData
	err := decoder.Decode(&snapshot)
	if err != nil {
		panic(fmt.Sprintf("ERROR while decoding state: %v", err))
	}
	rsm.sm.Restore(snapshot.Snapshot)
	rsm.lastAppliedIndex = snapshot.LastIncludedIndex
}

func (rsm *RSM) startReader() {
	//log.Printf("RSM node%d: starting reader", rsm.me)

	for applyMsg := range rsm.applyCh {

		if applyMsg.SnapshotValid {
			//log.Printf("RSM node%d: READER: received snapshot. index=%d term=%d", rsm.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)

			// lock is not needed: State Machine and lastAppliedIndex are updated only from the seconds case of this select
			if applyMsg.SnapshotIndex > rsm.lastAppliedIndex {
				rsm.restoreSnapshot(applyMsg.Snapshot)        // deserializes snapshot, restores State Machine state and updates lastAppliedIndex
				rsm.lastAppliedIndex = applyMsg.SnapshotIndex // TODO is it needed? could it diverge from index in snapshot?

				rsm.mu.Lock()
				// clear all pending submits
				if rsm.pendingSubmits.Len() > 0 {
					pendingSubmit := rsm.pendingSubmits.Front() // peek
					pendingSubmitData := pendingSubmit.Value.(*SubmitRequest)
					for pendingSubmit != nil {
						pendingSubmitData.ReplyCh <- SubmitResponse{Result: nil, Error: rpc.ErrWrongLeader}
						//DPrintfImp("RSM node%d: READER: sending ErrWrongLeader response. applyOp.ID=%v applyOp.index=%d; queued.id=%v queued.index=%d",
						//	rsm.me, applyOp.ID, applyMsg.CommandIndex, pendingSubmitData.Operation.ID, pendingSubmitData.CommandIndex)
						rsm.pendingSubmits.Remove(pendingSubmit)
						pendingSubmit = rsm.pendingSubmits.Front() // peek
						if pendingSubmit != nil {
							pendingSubmitData = pendingSubmit.Value.(*SubmitRequest)
						}
					}
				}
				rsm.mu.Unlock()
			}
		} else if applyMsg.CommandValid {

			applyOp := applyMsg.Command.(Op)

			// call DoOp() here because followers don't have pending Submit goroutines
			result := rsm.sm.DoOp(applyOp.Command)
			DPrintf("RSM node%d: READER: DoOp executed. applyOp.ID=%v applyOp.index=%d", rsm.me, applyOp.ID, applyMsg.CommandIndex)

			// Dequeue pending Submit call if any and compare it with applyOp
			rsm.mu.Lock()

			//rsm.appliedCommands.PushBack(applyMsg) // for debug

			if rsm.lastAppliedIndex > applyMsg.CommandIndex {
				panic(fmt.Sprintf("RSM node%d: READER: unexpected lastAppliedIndex > applyOp.index. lastAppliedIndex=%v; applyOp.ID=%v applyOp.index=%d applyOp.term=%d",
					rsm.me, rsm.lastAppliedIndex,
					applyOp.ID, applyMsg.CommandIndex, applyMsg.CommandTerm))
			}
			rsm.lastAppliedIndex = applyMsg.CommandIndex

			if rsm.pendingSubmits.Len() > 0 {
				// Peek first and dequeue only if queued.index == applied.index:
				// applied.index == queued.index: dequeue, check IDs equal and call DoOp()
				// applied.index < queued.index: the peer just became a leader and queued 1 submit to the top of the log,
				// 	 but applyCh still applies older committed records from previous leaders.
				//   In this case: skip and only call DoOp()
				// applied.index > queued.index: impossible. Log grows monolonically only.
				pendingSubmit := rsm.pendingSubmits.Front() // peek
				pendingSubmitData := pendingSubmit.Value.(*SubmitRequest)
				if applyMsg.CommandIndex == pendingSubmitData.CommandIndex {
					if applyOp.ID == pendingSubmitData.Operation.ID {
						// received the same command that was added at that index: dequeue it and reply OK
						rsm.pendingSubmits.Remove(pendingSubmit) // dequeue if applying the same command
						pendingSubmitData.ReplyCh <- SubmitResponse{Result: result, Error: rpc.OK}
						DPrintfImp("RSM node%d: READER: sending OK response. applyOp.command=%+v applyOp.command.type=%T applyOp.ID=%v applyOp.index=%d; queueed.command=%+v queued.id=%v queued.index=%d; DoOP.result=%+v DoOP.result.type=%T",
							rsm.me,
							applyOp.Command, applyOp.Command, applyOp.ID, applyMsg.CommandIndex,
							pendingSubmitData.Operation.Command, pendingSubmitData.Operation.ID, pendingSubmitData.CommandIndex,
							result, result)
					} else {
						// Received a different command at the same index: cancel all pending submits.
						// It's probably safe: if a different command appears on the submitted index, then the node isn'a a leader anymore and all its subsequent log entries will be discarded as being garbage

						pendingSubmitData.ReplyCh <- SubmitResponse{Result: nil, Error: rpc.ErrWrongLeader} // TODO remove?
						DPrintfImp("RSM node%d: READER: sending ErrWrongLeader response. applyOp.ID=%v applyOp.index=%d; queued.id=%v queued.index=%d",
							rsm.me, applyOp.ID, applyMsg.CommandIndex, pendingSubmitData.Operation.ID, pendingSubmitData.CommandIndex)
						for pendingSubmit != nil {
							pendingSubmitData.ReplyCh <- SubmitResponse{Result: nil, Error: rpc.ErrWrongLeader}
							DPrintfImp("RSM node%d: READER: sending ErrWrongLeader response. applyOp.ID=%v applyOp.index=%d; queued.id=%v queued.index=%d",
								rsm.me, applyOp.ID, applyMsg.CommandIndex, pendingSubmitData.Operation.ID, pendingSubmitData.CommandIndex)
							rsm.pendingSubmits.Remove(pendingSubmit)
							pendingSubmit = rsm.pendingSubmits.Front() // peek
							if pendingSubmit != nil {
								pendingSubmitData = pendingSubmit.Value.(*SubmitRequest)
							}
						}
					}
				} else if applyMsg.CommandIndex < pendingSubmitData.CommandIndex {
					DPrintfImp("RSM node%d: READER: skipping applyOp.index < queued.index. applyOp.ID=%v applyOp.index=%d; queued.id=%v queued.index=%d",
						rsm.me, applyOp.ID, applyMsg.CommandIndex, pendingSubmitData.Operation.ID, pendingSubmitData.CommandIndex)
				} else {
					// applyMsg.CommandIndex > pendingSubmitData.CommandIndex:
					// This must not be possible. it's guaranteed that Raft eventually will pass some command at queued.index. It must do that in order to keep all State Machines in sync.
					// So if there is a pending Submit containing index i (received from Start call), eventually a command at i will arrive at applyCh. But it could be a different command though.
					// Only if the node is partitioned it will not see new commands committed and applied as long as partition remains, which could last forever (this is OK).
					// Tests: passed 300 of 300 runs
					//rsm.debugLog()
					panic(fmt.Sprintf("RSM node%d: READER: unexpected applyOp.index > queued.index. applyOp.ID=%v applyOp.index=%d applyOp.term=%d; queued.ID=%v queued.index=%d queued.term=%d",
						rsm.me,
						applyOp.ID, applyMsg.CommandIndex, applyMsg.CommandTerm,
						pendingSubmitData.Operation.ID, pendingSubmitData.CommandIndex, pendingSubmitData.CommandTerm))
				}
			}
			rsm.mu.Unlock()
		}

		// Create snapshot
		if rsm.raftStateSizeTooLarge() {
			// Do we need to store lastAppliedIndex with snapshot?
			// Raft requires an index passed along with snapshot bytes.
			// RSM needs to restore persisted snapshot on reload: pass it to KV.
			// And if index is not persisted by RSM, on reload there is no index, just snapshot bytes.

			// when raft state size exceeded:
			// ask for snapshot from KV
			// pass snapshot and lastAppliedIndex to Raft
			rsm.mu.Lock()
			lastAppliedIndex := rsm.lastAppliedIndex
			snapshot := rsm.sm.Snapshot()
			snapshotData := SnapshotRSMData{
				Snapshot:          snapshot,
				LastIncludedIndex: lastAppliedIndex,
			}
			rsm.mu.Unlock()

			buffer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(buffer)
			encoder.Encode(snapshotData)

			rsm.rf.Snapshot(lastAppliedIndex, buffer.Bytes())
		}
	}
	rsm.Kill()
}

func (rsm *RSM) debugLog() {
	// log applied commands on this peer
	s := "["
	for elt := rsm.appliedCommands.Front(); elt != nil; elt = elt.Next() {
		val := elt.Value.(raftapi.ApplyMsg)
		s += fmt.Sprintf("{commandIndex=%d commandTerm=%v command=%v} ", val.CommandIndex, val.CommandTerm, val.Command)
	}
	s += "]"
	log.Printf("RSM node%d applied commands: %v", rsm.me, s)

	// log pending Submis on this peer
	s = "["
	for elt := rsm.pendingSubmits.Front(); elt != nil; elt = elt.Next() {
		val := elt.Value.(*SubmitRequest)
		s += fmt.Sprintf("{commandIndex=%d commandTerm=%v command=%v ID=%v} ", val.CommandIndex, val.CommandTerm, val.Operation.Command, val.Operation.ID)
	}
	s += "]"
	log.Printf("RSM node%d pending submits: %v", rsm.me, s)

	// log Raft log
	rsm.rf.PrintLog()
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
	// Generating an ID for this operation to compare applied and submitted operations when it will be applied
	// Looks like ID as a struct of {me seqId} is better choice than GUID (inefficient) and just a seqID (could overlap with other nodes)
	// TODO move to a method
	rsm.lastId++
	op := Op{
		ID:      OpID{rsm.me, rsm.lastId},
		Command: req,
	}

	DPrintf("RSM node%d: calling Start for ID=%v...", rsm.me, op.ID)
	index, term, isLeader := rsm.rf.Start(op)
	DPrintfImp("RSM node%d: Start executed for op.ID=%v op.command=%+v. index=%d, term=%d, isLeader=%v", rsm.me, op.ID, op.Command, index, term, isLeader)

	//log.Printf("RSM node%d: Start executed for op.ID=%v op.command=%+v. index=%d, term=%d, isLeader=%v", rsm.me, op.ID, op.Command, index, term, isLeader)
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
	// Between Start() and enqueuing the command, it could already have been applied (very unlikely but so).
	// This is safe: the reader will apply the command to state machine, but then it has to wait on the mutex until the submit enqueued and releases the lock.
	// And only then in will dequeue the submit and send a response received from State Machine here to be returned to the client.
	pendingSubmit := rsm.pendingSubmits.PushBack(r)
	pendingSubmitData := pendingSubmit.Value.(*SubmitRequest)
	rsm.mu.Unlock()

	// Now wait for the reply from the reader arrived in replyCh or timeout ellapsed
	DPrintf("RSM node%d: waiting to complete for index=%d op.ID=%v", rsm.me, index, op.ID)
	timeout := time.NewTicker(200 * time.Millisecond)
	defer timeout.Stop()
	for {
		select {
		case response := <-r.ReplyCh:
			DPrintfImp("RSM node%d: Submit received result for op.ID=%v queued.CommandIndex=%d: error=%v result=%v", rsm.me, op.ID, pendingSubmitData.CommandIndex, response.Error, response.Result)
			return response.Error, response.Result
		// Fallback: check current peer state if the cluster is inactive and reader-goroutine can't remove pending submits due to no new applied commands in applyCh.
		// Without this fallback sometimes tests will stuck:
		// 		=== RUN   TestUnreliable4B
		// 		Test: many clients (4B many clients) (unreliable network)...
		// 		...and waits forever
		case <-timeout.C:
			newTerm, _ := rsm.rf.GetState()
			// Cancel the Submit only if the Raft term increased: corresponding leader has lost leadership.
			// If the correspoding leader hasn't lost leadership, the command eventually will be applied and the Submit will receive a reply.
			// In either case (network partition, leader crash etc), the leader will lose leadership, and the command may never be committed and applied,
			//    and if there are no more commands, the Submit could stuck forever (and it does sometimes).
			if pendingSubmitData.CommandTerm < newTerm {
				rsm.mu.Lock()
				if rsm.pendingSubmits.Len() > 0 {
					DPrintfImp("RSM node%d: Submit canceled due timeout for op.ID=%v term=%d due to new term=%d", rsm.me, op.ID, pendingSubmitData.CommandTerm, newTerm)
					rsm.pendingSubmits.Remove(pendingSubmit) // Removing here seems to be safe: if already removed from reader the call does nothing
					rsm.mu.Unlock()
					return rpc.ErrWrongLeader, nil
				}
				rsm.mu.Unlock()
			}
		}
	}
}

// Is called from reader after applyCh is closed from outside
func (rsm *RSM) Kill() {
	DPrintf("RSM node%d: Kill started", rsm.me)
	rsm.mu.Lock()
	for rsm.pendingSubmits.Len() > 0 {
		element := rsm.pendingSubmits.Front()
		val := rsm.pendingSubmits.Remove(element)
		pendingSubmitRequest := val.(*SubmitRequest)
		pendingSubmitRequest.ReplyCh <- SubmitResponse{Result: nil, Error: rpc.ErrWrongLeader}
	}
	rsm.mu.Unlock()
	//rsm.rf.Kill() // TODO how to kill raft? And is it needed? rf.Kill() is not in inteface. Add it there?
	DPrintf("RSM node%d: Kill completed", rsm.me)
}
