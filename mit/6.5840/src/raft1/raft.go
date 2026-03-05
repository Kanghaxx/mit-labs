package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type PeerState int32

const (
	Follower PeerState = iota
	Candidate
	Leader
)

const (
	votedForNone = -1
	noneTerm     = -1
)

const (
	appendEntriesPeriodicityMs = 10
	applyEntriesPeriodicityMs  = 2
	electionCheckPeriodicityMs = 2
	electionTimeoutMs          = 80
	electionTimeoutRandMs      = 50
	// appendEntriesPeriodicityMs = 50
	// applyEntriesPeriodicityMs  = 2
	// electionCheckPeriodicityMs = 10
	// electionTimeoutMs          = 150
	// electionTimeoutRandMs      = 80
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type appendEtriesResult struct {
	ok           bool
	id           int
	nextIndex    int
	lastLogIndex int
	reply        *AppendEntriesReply
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 3A
	peerState               PeerState
	currentTerm             int
	votedFor                int
	electionTimeoutMs       int
	majority                int
	lastHeartbeatReceivedAt time.Time
	// 3B
	log                   []LogEntry
	commitIndex           int
	lastApplied           int
	nextIndex             []int
	matchIndex            []int
	applyCh               chan raftapi.ApplyMsg
	appendEntriesResultCh chan appendEtriesResult
	// 3D
	snapshot                    []byte
	lastIncludedIndexInSnapshot int
	lastIncludedTermInSnapshot  int
}

type RaftState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
	// 3D: snapshot metadata persisted with raft state so that the raw snapshot bytes can be stored separately by Persister
	// looks like tests are decoding raw snapshot from persister, so no user type allowed there
	LastIncludedIndex int
	LastIncludedTerm  int
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// 3A
	rf.peerState = Follower
	rf.votedFor = votedForNone
	rf.currentTerm = 0
	rf.majority = len(rf.peers)/2 + 1 // for 5 peers, majority = 3
	rf.resetElectionTimeout()
	rf.resetHeartbeatTimeout()
	// 3B
	rf.log = make([]LogEntry, 0, 10)           // first index in paper is 1. But in lab 3 starting index is 0
	rf.commitIndex = -1                        // index of highest log entry known to be committed. initialized to 0, increases monotonically
	rf.lastApplied = -1                        // index of highest log entry applied to state machine. initialized to 0, increases monotonically
	rf.nextIndex = make([]int, len(rf.peers))  // for each server, index of the next log entry to send to that server
	rf.matchIndex = make([]int, len(rf.peers)) // for each server, index of highest log entry known to be replicated on server
	// 3C: initialize from state persisted before a crash
	rf.lastIncludedIndexInSnapshot = -1
	rf.lastIncludedTermInSnapshot = noneTerm
	rf.readPersist(persister.ReadRaftState())
	rf.applyCh = applyCh
	rf.appendEntriesResultCh = make(chan appendEtriesResult)
	// 3D:
	rf.readSnapshot(persister.ReadSnapshot())
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getAbsLogLen() // initialized to leader last log index + 1
		rf.matchIndex[i] = -1               // initialized to 0, increases monotonically
	}

	go rf.startElection()
	go rf.startLogReplicationAndHeartbeats()
	go rf.startLogApply()
	go rf.startInstallSnapshot()

	DPrintf("Raft instance%d started", rf.me)
	DPrintf("Raft instance%d init state: lastIncludedIndexInSnapshot=%d lastIncludedTermInSnapshot=%d", rf.me, rf.lastIncludedIndexInSnapshot, rf.lastIncludedTermInSnapshot)
	//rf.printLog()

	return rf
}

func (rf *Raft) getAbsLogLen() int {
	// Example:
	// len(abs) =          5  (= local len + lastIncludedIndex + 1)
	// len(local) =        2
	// log(trim) =       0,1
	// log(abs) =  0,1,2,3,4
	// snapshot =  0,1,2
	//                 ^
	//         lastIncludedIndexInSnapshot
	logLen := len(rf.log)
	if rf.lastIncludedIndexInSnapshot >= 0 {
		logLen += rf.lastIncludedIndexInSnapshot + 1
	}
	return logLen
}

func (rf *Raft) absToLocal(absIndex int) int {
	// Warning: absToLocal could return index <= -1 when there is only a snapshot and the abs index is inside it
	// All callers must take this into account
	// Example:
	// log (abs) =    0 1 2
	// log (local) = nil
	// snapshot =     0 1 2
	// Call absToLocal(2) returns 2 - 2 - 1 = -1
	// Call absToLocal(1) returns 1 - 2 - 1 = -2
	//
	// Example:
	// absIndex =          4
	// localIndex =        1   (= abs index - lastIncludedIndex - 1)
	// log(trim) =       0,1
	// log(abs) =  0,1,2,3,4
	// snapshot =  0,1,2
	//                 ^
	//         lastIncludedIndexInSnapshot
	// Example:
	// absIndex =    1
	// localIdx =    0
	// log(trim) =   0
	// log(abs) =  0,1
	// snapshot =  0,1
	//             ^
	//    lastIncludedIndexInSnapshot
	local := absIndex
	if rf.lastIncludedIndexInSnapshot >= 0 {
		local = local - rf.lastIncludedIndexInSnapshot - 1
	}
	//DPrintf("Raft instance%d absToLocal(): absIndex=%d lastIncludedIndexInSnapshot=%d result=%d", rf.me, absIndex, rf.lastIncludedIndexInSnapshot, local)
	return local
}

func (rf *Raft) startInstallSnapshot() {
	resultCh := make(chan struct {
		ok                bool
		id                int
		lastIncludedIndex int
		reply             *InstallSnapshotReply
	})

	go func() {
		for rf.killed() == false {
			for serverid := range rf.peers {
				if serverid == rf.me {
					continue
				}

				rf.mu.Lock()
				var args *InstallSnapshotArgs
				lastIncludedIndex := rf.lastIncludedIndexInSnapshot
				sendSnapshot := (rf.lastIncludedIndexInSnapshot >= 0) && (rf.peerState == Leader) && (rf.nextIndex[serverid] <= rf.lastIncludedIndexInSnapshot) // send snapshot if nextIndex fo peer falls down inside snapshot
				if sendSnapshot {
					snapshot := make([]byte, len(rf.snapshot))
					copy(snapshot, rf.snapshot)
					args = &InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LastIncludedIndex: lastIncludedIndex,
						LastIncludedTerm:  rf.lastIncludedTermInSnapshot,
						Data:              snapshot,
					}
				}
				rf.mu.Unlock()

				if sendSnapshot {
					go func(server int, lastIncludedIndex int, args *InstallSnapshotArgs) {
						reply := &InstallSnapshotReply{}
						ok := rf.sendInstallSnapshot(server, args, reply)
						resultCh <- struct {
							ok                bool
							id                int
							lastIncludedIndex int
							reply             *InstallSnapshotReply
						}{ok, server, lastIncludedIndex, reply}
					}(serverid, lastIncludedIndex, args)
				}

			}
			time.Sleep(time.Duration(appendEntriesPeriodicityMs) * time.Millisecond)
		}
	}()

	for result := range resultCh {
		if !result.ok {
			continue
		}
		rf.mu.Lock()
		if rf.increaseTerm(result.reply.Term) {
			DPrintf("Raft instance%d (Leader) detected higher term %d on InstallSnapshot response from %d. Converting to follower", rf.me, result.reply.Term, result.id)
		} else {
			// 3D
			rf.increaseFollowerWatermarksOnLeader(result.id, result.lastIncludedIndex)
			DPrintf("Raft instance%d (Leader) snapshot sent to peer [%d]. lastIncludedIndexInSnapshot=%d lastIncludedTermInSnapshot=%d nextIndex[peer] = %d", rf.me, result.id, rf.lastIncludedIndexInSnapshot, rf.lastIncludedTermInSnapshot, rf.nextIndex[result.id])
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) handleInstallSnapshot(leaderTerm int, lastIncludedIndex int, lastIncludedTerm int, data []byte) {
	// If the incoming RPC has older term, ignore it.
	if leaderTerm < rf.currentTerm {
		return
	}
	// Treat InstallSnapshot like a heartbeat from the leader: reset heartbeat timeout.
	rf.resetHeartbeatTimeout()
	// If the RPC term is newer, update term and convert to follower.
	if rf.increaseTerm(leaderTerm) {
		DPrintf("Raft instance%d detected higher term %d on InstallSnapshot RPC. Converting to follower", rf.me, leaderTerm)
	}
	if lastIncludedIndex <= rf.lastIncludedIndexInSnapshot {
		DPrintf("Raft instance%d ignored snapshot due to request.lastIncludedIndex=%d and rf.lastIncludedIndexInSnapshot=%d", rf.me, lastIncludedIndex, rf.lastIncludedIndexInSnapshot)
		return
	}
	// 6. If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	lastIncludedIndexLocal := rf.absToLocal(lastIncludedIndex)
	if (lastIncludedIndexLocal >= 0) && (lastIncludedIndexLocal < len(rf.log)) {
		if rf.log[lastIncludedIndexLocal].Term == lastIncludedTerm {
			if lastIncludedIndexLocal == len(rf.log)-1 {
				rf.log = rf.log[:0]
			} else {
				rf.log = rf.log[lastIncludedIndexLocal+1:]
			}
			rf.lastIncludedIndexInSnapshot = lastIncludedIndex
			rf.lastIncludedTermInSnapshot = lastIncludedTerm
			rf.snapshot = data
			rf.persist()

			DPrintf("Raft instance%d agreed with snapshot lastIncludedIndex=%d lastIncludedTerm=%d", rf.me, lastIncludedIndex, lastIncludedTerm)
			//rf.printLog()
			// If this snapshot advances what the follower has applied, apply it
			if rf.lastApplied < rf.lastIncludedIndexInSnapshot {
				rf.applySnapshot()
			}
			return
		}
	}
	// Otherwise: 7. Discard the entire log
	rf.log = rf.log[:0]
	rf.lastIncludedIndexInSnapshot = lastIncludedIndex
	rf.lastIncludedTermInSnapshot = lastIncludedTerm
	rf.snapshot = data
	rf.persist()

	DPrintf("Raft instance%d discarded its state due to newer snapshot lastIncludedIndex=%d lastIncludedTerm=%d", rf.me, lastIncludedIndex, lastIncludedTerm)
	// 8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	rf.applySnapshot()
}

func (rf *Raft) applySnapshot() {
	if rf.lastApplied > rf.lastIncludedIndexInSnapshot {
		panic(fmt.Sprintf("Raft instance%d lastApplied mismatch. rf.lastApplied=%d lastIncludedIndex=%d", rf.me, rf.lastApplied, rf.lastIncludedIndexInSnapshot))
	}

	DPrintf("Raft instance%d applying snapshot. rf.lastApplied=%d, rf.lastIncludedIndexInSnapshot=%d lastIncludedTermInSnapshot=%d", rf.me, rf.lastApplied, rf.lastIncludedIndexInSnapshot, rf.lastIncludedTermInSnapshot)
	rf.lastApplied = rf.lastIncludedIndexInSnapshot
	if rf.commitIndex < rf.lastApplied {
		rf.commitIndex = rf.lastApplied
	}
	applyMsg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.lastIncludedTermInSnapshot,
		SnapshotIndex: rf.lastIncludedIndexInSnapshot + 1, // pretend 1-indexed
	}
	rf.mu.Unlock()
	// Do not lock while applying: client may call back into Raft, which could cause deadlocks or whatever
	rf.applyCh <- applyMsg // TODO refactor
	rf.mu.Lock()
}

func (rf *Raft) startLogApply() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.lastApplied < rf.lastIncludedIndexInSnapshot { // restore after reboot
			rf.applySnapshot() // TODO probably do 1 time on reboot
		}
		rf.mu.Unlock()

		applyMsg := raftapi.ApplyMsg{CommandValid: true}

		rf.mu.Lock()
		// "If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)"
		applyLog := rf.commitIndex > rf.lastApplied
		if applyLog {
			rf.lastApplied++
			// absToLocal() could return negative index if lastApplied is inside snapshot, but here lastApplied cannot be inside previous snapshot
			lastAppliedLocal := rf.absToLocal(rf.lastApplied)
			if lastAppliedLocal < 0 {
				DPrintf("Raft instance%d unexpected lastAppliedLocal=%d. rf.lastApplied=%d, rf.lastIncludedIndexInSnapshot=%d", rf.me, lastAppliedLocal, rf.lastApplied, rf.lastIncludedIndexInSnapshot)
				//rf.printLog()
			}
			applyMsg.Command = rf.log[lastAppliedLocal].Command
			applyMsg.CommandIndex = rf.lastApplied + 1 // pretend 1-indexed. If not, tests fail with "one(100) failed to reach agreement" or "got index 0 but expected 1": 1 is hardcoded in tests
		}
		rf.mu.Unlock()

		if applyLog {
			// channels are sync: if apply inside lock, all other threads that acquire this lock will wait for the subscriber (client) to get the message out.
			// instead apply outside lock so only apply thread is blocked until client gets the message out of the channel.
			rf.applyCh <- applyMsg

			//rf.mu.Lock()
			//DPrintf("Raft instance%d has applied command at log index=%d. lastIncludedIndexInSnapshot=%d lastIncludedTermInSnapshot=%d", rf.me, rf.lastApplied, rf.lastIncludedIndexInSnapshot, rf.lastIncludedTermInSnapshot)
			//rf.printLog()
			//rf.mu.Unlock()
		}
		time.Sleep(time.Duration(applyEntriesPeriodicityMs) * time.Millisecond)
	}
}

func (rf *Raft) startLogReplicationAndHeartbeats() {
	go func() {
		for rf.killed() == false {
			rf.sendAppendEntriesToPeers()
			time.Sleep(time.Duration(appendEntriesPeriodicityMs) * time.Millisecond)
		}
	}()

	// handle AppendEntries responses
	for result := range rf.appendEntriesResultCh {
		if !result.ok {
			continue
		}
		rf.mu.Lock()
		if rf.increaseTerm(result.reply.Term) {
			DPrintf("Raft instance%d (Leader) detected higher term %d on AppendEntries response from %d. Converting to follower", rf.me, result.reply.Term, result.id)
		} else {
			if result.reply.Success {
				// if follower successfuly stored entries, increase follower watermarks
				if rf.nextIndex[result.id] != result.lastLogIndex+1 {

					DPrintf("Raft instance%d (Leader) increasing nextIndex[%d] from %d to %d. lastIncludedIndexInSnapshot=%d lastIncludedTermInSnapshot=%d", rf.me, result.id, rf.nextIndex[result.id], result.lastLogIndex+1, rf.lastIncludedIndexInSnapshot, rf.lastIncludedTermInSnapshot)
				}
				rf.increaseFollowerWatermarksOnLeader(result.id, result.lastLogIndex)
				ok, commitIndex := rf.increaseCommitIndexOnLeader() // increase commitIndex if quorum achieved
				if ok {
					DPrintf("Raft instance%d (Leader) increased commitIndex from %d to %d. lastIncludedIndexInSnapshot=%d lastIncludedTermInSnapshot=%d", rf.me, rf.commitIndex, commitIndex, rf.lastIncludedIndexInSnapshot, rf.lastIncludedTermInSnapshot)
					//rf.printLog()
				}
			} else {
				// if not success, decrease nextIndex: find index where leader and follower agree on their logs
				rf.decreaseFollowerWatermarksOnLeader(result.id, result.nextIndex, result.reply.XTerm, result.reply.XIndex, result.reply.XLen)
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntriesToPeers() int {
	sentCount := 0
	for serverid := range rf.peers {
		if serverid == rf.me {
			continue
		}
		if rf.killed() == true {
			return 0
		}
		rf.mu.Lock()
		isLeader := rf.peerState == Leader
		var args *AppendEntriesArgs
		nextIndex := -1
		lastLogIndex := rf.getAbsLogLen() - 1
		if isLeader {
			var newEntries []LogEntryApiModel = nil
			nextIndex, newEntries = rf.getNewEntries(serverid)
			// "If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex"
			prevLogTerm := noneTerm
			prevLogIndex := nextIndex - 1 // prev index and term for the very first entry = -1
			prevLogIndexLocal := rf.absToLocal(prevLogIndex)
			//DPrintf("Raft instance%d (Leader) prepares appendEntries: prevLogIndex=%d, prevLogIndexLocal=%d newEntries=%v", rf.me, prevLogIndex, prevLogIndexLocal, newEntries)
			//rf.printLog()
			if prevLogIndexLocal >= 0 {
				prevLogTerm = rf.log[prevLogIndexLocal].Term
			} else {
				// 3D: prevLogIndex could be inside snapshot. Therefore send snapshot's metadata.
				// If there is no snapshot either, that means leader sends very first entries and -1 will be sent.
				prevLogIndex = rf.lastIncludedIndexInSnapshot
				prevLogTerm = rf.lastIncludedTermInSnapshot
			}
			args = &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      newEntries, // nil = heartbeat
				LeaderCommit: rf.commitIndex,
			}
			if newEntries != nil {
				sentCount += len(newEntries)
			}
		}
		rf.mu.Unlock()

		if isLeader {
			//DPrintf("Raft isntance %d (Leader) sends AppendEntries request for %d entries, PrevLogIndex=%d LeaderCommit=%d count=%d", rf.me, len(newEntries), prevLogIndex, leaderCommit, len(newEntries))
			// Send AppendEntries requests in parallel to tolerate crashed servers
			go func(server int, a *AppendEntriesArgs, nextIndex int, lastLogIndex int) {
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, a, reply)
				rf.appendEntriesResultCh <- appendEtriesResult{ok, server, nextIndex, lastLogIndex, reply}
			}(serverid, args, nextIndex, lastLogIndex)
		}
	}
	return sentCount
}

func (rf *Raft) getNewEntries(serverid int) (nextIndex int, entries []LogEntryApiModel) {
	// 3D
	// Example:
	// log (abs):              0 1 2 3 4
	// s1 (leader) log: [snapshot] 0 1 2
	// s2 (follower) log:      0
	//
	// s1 nextIndex[s2] will decrease to 1 (abs), but there is no entry on that index in leader's local log
	// nextIndex needs to be converted to local index:
	// nextIndexLocal = nextIndex - lastIncludedIndex - 1
	// nextIndexLocal for 1(abs) = 1 - 1 - 1 = -1: negative nextIndexLocal means there is no such log record anymore and leader must send snapshot instead.

	lastLogIndexLocal := len(rf.log) - 1
	nextIndexLocal := rf.absToLocal(rf.nextIndex[serverid])
	var newEntries []LogEntryApiModel = nil
	// 3D: nextIndexLocal could be negative (could be inside local snapshot), in this case snapshot must be sent and installed on follower first to increase nextIndex[i]
	if (nextIndexLocal >= 0) && (nextIndexLocal <= lastLogIndexLocal) {
		// copy entries to send
		//DPrintf("Raft isntance %d (Leader) has lastLogIndex=%d and nextIndex=%d. Copying entries from log", rf.me, lastLogIndexLocal, nextIndex)
		// TODO new leader can try to send all log after failover. Mb add upper limit
		newEntriesSlice := rf.log[nextIndexLocal : lastLogIndexLocal+1]
		newEntries = make([]LogEntryApiModel, len(newEntriesSlice))
		for i, val := range newEntriesSlice {
			newEntries[i] = LogEntryApiModel{Term: val.Term, Command: val.Command}
		}
	}
	return rf.nextIndex[serverid], newEntries // 3D: if nextIndex was negative due to entries missing because of snapshotting, newEntries will be nil = heartbeat
}

func (rf *Raft) increaseFollowerWatermarksOnLeader(serverid int, lastLogIndex int) {
	newIndex := lastLogIndex + 1
	if newIndex > rf.nextIndex[serverid] {
		rf.nextIndex[serverid] = lastLogIndex + 1
		rf.matchIndex[serverid] = lastLogIndex
	}
}

func (rf *Raft) decreaseFollowerWatermarksOnLeader(serverid int, oldNextIndex int, xterm int, xindex int, xlen int) {

	//  TODO 3D: fencing. nextIndex could already have been increased by installing snapshot
	//if (rf.nextIndex[serverid] == oldNextIndex) && (rf.nextIndex[serverid] > 0) { // somehow 3C tests become worse
	// Mb check rf.nextIndex[serverid] > oldNextIndex

	if rf.nextIndex[serverid] > 0 {
		from := rf.nextIndex[serverid]
		rf.nextIndex[serverid] = rf.nextIndex[serverid] - 1

		// TODO 3D

		// newIndex := old
		// // 3C
		// // XTerm:  term in the conflicting entry (if any)
		// // XIndex: index of first entry with that term (if any)
		// // XLen:   log length
		// if (xlen >= 0) && (rf.nextIndex[serverid] > xlen) {
		// 	// Case 3: follower's log is too short:
		// 	//     nextIndex = XLen
		// 	newIndex = xlen
		// } else {
		// 	if (xterm >= 0) && (xindex >= 0) {
		// 		lastLogIndex := len(rf.log) - 1
		// 		prevLogIndex := rf.nextIndex[serverid] - 1
		// 		i := min(lastLogIndex, prevLogIndex)
		// 		// Find xterm to the left. No sense to find to the right: follower responded false if term mismatch on prevLogIndex
		// 		for ; (i >= 0) && (rf.log[i].Term > xterm); i-- {
		// 		}
		// 		// now i contains index of last entry with XTerm on leader or entry with lesser term if there is no XTerm on leader
		// 		if rf.log[i].Term == xterm {
		// 			// Case 2: leader has XTerm:
		// 			//     nextIndex = (index of leader's last entry for XTerm) + 1
		// 			rf.nextIndex[serverid] = i + 1
		// 			newIndex = i + 1
		// 		} else {
		// 			// Case 1: leader doesn't have XTerm:
		// 			//     nextIndex = XIndex
		// 			newIndex = xindex
		// 		}
		// 	}
		// }
		// if newIndex < rf.nextIndex[serverid] {
		// 	rf.nextIndex[serverid] = newIndex
		// }

		DPrintf("Raft instance%d (Leader) decreased nextIndex[%d] from %d to %d. lastIncludedIndexInSnapshot=%d lastIncludedTermInSnapshot=%d", rf.me, serverid, from, rf.nextIndex[serverid], rf.lastIncludedIndexInSnapshot, rf.lastIncludedTermInSnapshot)
	}
}

// AppendEntries RPC Handler
func (rf *Raft) handleAppendEntries(leaderTerm int, leaderCommit int, prevLogIndex int, prevLogTerm int, newEntries []LogEntryApiModel) (ok bool, xterm int, xindex int, xlen int) {
	// "1. Reply false if term < currentTerm (§5.1)"
	xterm = -1
	xindex = -1
	xlen = len(rf.log) // TODO 3D
	if leaderTerm < rf.currentTerm {
		return false, xterm, xindex, xlen // ignore zombie leaders: Reply false if term < currentTerm (§5.1)
	}
	rf.resetHeartbeatTimeout() // reset only after term check: ignore heartbeats from zombie leaders
	if rf.increaseTerm(leaderTerm) {
		DPrintf("Raft instance%d detected higher term %d while handling heartbeat. Converting to follower", rf.me, leaderTerm)
	}
	// "2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)"
	if !rf.isLogConsistentWithLeader(prevLogIndex, prevLogTerm) {

		// 3C:
		// XTerm:  term in the conflicting entry (if any)
		// XIndex: index of first entry with that term (if any)
		// XLen:   log length

		// TODO 3D
		// if (prevLogIndex >= 0) && (prevLogIndex < len(rf.log)) {
		// 	xterm = rf.log[prevLogIndex].Term
		// 	for i := prevLogIndex; (i >= 0) && (rf.log[i].Term == xterm); i-- {
		// 		xindex = i
		// 	}
		// }
		return false, xterm, xindex, xlen
	}

	if len(newEntries) > 0 { // newEntries is empty for heatbeats
		// "3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)"
		newEntriesIndex := rf.deleteConflictingEntriesOnFollower(prevLogIndex, newEntries)
		// "4. Append any new entries not already in the log"
		rf.appentNewEntries(newEntriesIndex, newEntries)
		rf.persist()
		//rf.printLog()
	}

	//DPrintf("Raft instance%d (Follower) AppendEntries recevied. leaderCommit=%d", rf.me, leaderCommit)

	oldCommitIndex := rf.commitIndex
	if rf.increaseCommitIndexOnFollower(leaderCommit, prevLogIndex, len(newEntries)) {
		DPrintf("Raft instance%d (Follower) commitIndex=%d leaderCommit=%d. Increasing commitIndex from %d to %d. lastIncludedIndexInSnapshot=%d lastIncludedTermInSnapshot=%d", rf.me, rf.commitIndex, leaderCommit, oldCommitIndex, rf.commitIndex, rf.lastIncludedIndexInSnapshot, rf.lastIncludedTermInSnapshot)
		//rf.printLog()
	}

	return true, xterm, xindex, xlen
}

// Consistency Check on follower
func (rf *Raft) isLogConsistentWithLeader(prevLogIndex int, prevLogTerm int) bool {
	// if prevLogIndex == -1 then the very first entry at index 0 is being sent, or log is empty. -1 passes Consistency Check
	if prevLogIndex < 0 {
		return true
	}

	// 3D
	// lastIncludedIndex is guaranteed to be committed: only committed and applied entries are included in snapshot.
	// This means that every index <= lastIncludedIndex is committed in cluster.
	// Therefore when leader sends prevLogIndex <= lastIncludedIndex it means that prevLogIndex is committed too, because each leader contains all committed entries.
	// So this follower's shapshot surely contains prevLogIndex and we need to apply entries after lastIncludedIndex.
	if prevLogIndex <= rf.lastIncludedIndexInSnapshot {
		return true
	}

	lastLogIndexLocal := len(rf.log) - 1
	prevLogIndexLocal := rf.absToLocal(prevLogIndex) // absToLocal() could return negative idx if it's inside snapshot but above we return true if so
	if (prevLogIndexLocal <= lastLogIndexLocal) && (rf.log[prevLogIndexLocal].Term == prevLogTerm) {
		return true
	}

	return false
}

func (rf *Raft) deleteConflictingEntriesOnFollower(prevLogIndex int, newEntries []LogEntryApiModel) int {
	// "If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)"

	// 3D
	// Case: prevLogIndex could be outside snapshot:
	// entries(local) =    0 1 2     (entries are 0-indexed slice)
	// entries(abs) =      3 4 5
	// prevLogIndex =    2           (prevLogIndex is global and points where entries globally start in the log)
	// log (abs) =   0 1 2 3 4 5
	// log (local) =     0 1
	// snapshot =    0 1 <-- lastIncludedIndex
	// Where log comparison starts:
	// logIndex = absToLocal(prevLogIndex) + 1 = (prevLogIndex - lastIncludedIndex - 1) + 1 = 2 - 1 - 1 + 1 = 1 (OK)
	// newEntriesIndex = 0 (OK)
	//
	// Case: prevLogIndex == lastIncludedIndex:
	// entries(local) =    0 1 2     (entries are 0-indexed slice)
	// entries(abs) =      3 4 5
	// prevLogIndex =    2           (prevLogIndex is global and points where entries globally start in the log)
	// log (abs) =   0 1 2 3 4 5
	// log (local) =       0
	// snapshot =    0 1 2 <-- lastIncludedIndex
	// logIndex = 0 (OK)
	// newEntriesIndex = lastIncludedIndexInSnapshot - prevLogIndex = 0 - 0 = 0 (OK)
	//
	// Case: prevLogIndex could be inside snapshot and some entries too:
	// entries(local) =  0 1 2 3   (entries are 0-indexed slice)
	// entries(abs) =    2 3 4 5
	// prevLogIndex =  1           (prevLogIndex is global and points where entries globally start in the log)
	// log (abs) =   0 1 2 3 4 5
	// log (local) =       0
	// snapshot =    0 1 2 <-- lastIncludedIndex
	// logIndex = 0 (OK)
	// newEntriesIndex = lastIncludedIndex - prevLogIndex = 2 - 1 = 1 (OK)
	//
	// Case: prevLogIndex and entries could be completely inside snapshot:
	// entries(local) =   0 1         (entries are 0-indexed slice)
	// entries(abs) =     1 2
	// prevLogIndex =   0             (prevLogIndex is global and points where entries globally start in the log)
	// log (abs) =      0 1 2 3 4 5
	// log (local) =          0 1 2
	// snapshot =       0 1 2 <-- lastIncludedIndex
	// logIndex = 0 (OK)
	// newEntriesIndex = lastIncludedIndex - prevLogIndex = 2 - 0 = 2 (OK)
	//
	// Case: prevLogIndex == 0, lastIncludedIndex == 0
	// entries(local) =   0 1         (entries are 0-indexed slice)
	// entries(abs) =     1 2
	// prevLogIndex =   0             (prevLogIndex is global and points where entries globally start in the log)
	// log (abs) =      0 1 2 3 4 5
	// log (local) =      0 1 2 3 4
	// snapshot =       0 <-- lastIncludedIndex
	// logIndex = 0 (OK)
	// newEntriesIndex = lastIncludedIndex - prevLogIndex = 0 - 0 = 0 (OK)
	//
	// Case: prevLogIndex == -1, some entries are inside snapshot
	// entries(local) = 0 1 2 3 4     (entries are 0-indexed slice)
	// entries(abs) =   0 1 2 3 4
	// prevLogIndex = -1              (prevLogIndex is global and points where entries globally start in the log)
	// log (abs) =      0 1 2 3 4 5
	// log (local) =          0 1 2
	// snapshot =       0 1 2 <-- lastIncludedIndex
	// logIndex = 0 (OK)
	// newEntriesIndex = lastIncludedIndex - prevLogIndex = 2 - - 1 = 3 (OK)
	//
	// Case: prevLogIndex == -1, all entries are inside snapshot
	// entries(local) = 0 1 2         (entries are 0-indexed slice)
	// entries(abs) =   0 1 2
	// prevLogIndex = -1              (prevLogIndex is global and points where entries globally start in the log)
	// log (abs) =      0 1 2 3 4 5
	// log (local) =          0 1 2
	// snapshot =       0 1 2 <-- lastIncludedIndex
	// logIndex = 0 (OK)
	// newEntriesIndex = lastIncludedIndex - prevLogIndex = 2 - - 1 = 3 (OK)
	//
	// Case: prevLogIndex == -1, lastIncludedIndex == 0
	// entries(local) = 0 1 2         (entries are 0-indexed slice)
	// entries(abs) =   0 1 2
	// prevLogIndex = -1              (prevLogIndex is global and points where entries globally start in the log)
	// log (abs) =      0 1 2 3 4 5
	// log (local) =      0 1 2 3 4
	// snapshot =       0 <-- lastIncludedIndex
	// logIndex = 0 (OK)
	// newEntriesIndex = lastIncludedIndex - prevLogIndex = 0 - - 1 = 1 (OK)
	//
	// Case: prevLogIndex == -1, lastIncludedIndex == -1 (no snapshot)
	// entries(local) = 0 1 2         (entries are 0-indexed slice)
	// entries(abs) =   0 1 2
	// prevLogIndex = -1              (prevLogIndex is global and points where entries globally start in the log)
	// log (abs) =      0 1 2 3 4 5
	// log (local) =    0 1
	// snapshot =     -1 <-- lastIncludedIndex
	// logIndex = 0 (OK)
	// newEntriesIndex = lastIncludedIndex - prevLogIndex = -1 - - 1 = 0 (OK)

	// If prevLogIndex is outside snapshot, convert it to local log index (substract includedIndex) and merge as usual.
	// If prevLogIndex is inside snapshot, skip some entries that might be in snapshot and start merging with the local log at index 0.
	newEntriesIndex := 0
	logIndex := rf.absToLocal(prevLogIndex) + 1
	if prevLogIndex <= rf.lastIncludedIndexInSnapshot { // lastLogIndex is inside local snapshot
		newEntriesIndex = rf.lastIncludedIndexInSnapshot - prevLogIndex // skip new entries that already present in the snapshot
		logIndex = 0
	}

	lastLogIndexLocal := len(rf.log) - 1
	for newEntriesIndex < len(newEntries) {
		if logIndex > lastLogIndexLocal { // no more items in the log
			break
		}
		if newEntries[newEntriesIndex].Term != rf.log[logIndex].Term {
			rf.log = rf.log[:logIndex] // found mismatch, delete all log entries staring here
			break
		}
		logIndex++
		newEntriesIndex++
	}
	return newEntriesIndex
}

func (rf *Raft) appentNewEntries(newEntriesIndex int, newEntries []LogEntryApiModel) {
	// "Append any new entries not already in the log"
	appendedCount := 0
	for newEntriesIndex < len(newEntries) {
		model := newEntries[newEntriesIndex]

		DPrintf("Raft instance%d (Follower) appending entry %v to local log with entry term=%d. lastIncludedIndexInSnapshot=%d lastIncludedTermInSnapshot=%d", rf.me, model.Command, model.Term, rf.lastIncludedIndexInSnapshot, rf.lastIncludedTermInSnapshot)
		rf.log = append(rf.log, LogEntry{Command: model.Command, Term: model.Term})
		newEntriesIndex++
		appendedCount++
	}
	if appendedCount > 0 {
		//rf.printLog()
	}
}

func (rf *Raft) increaseCommitIndexOnLeader() (ok bool, newCommitIndex int) {
	if rf.peerState != Leader {
		return false, -1
	}
	// "If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:""
	// "set commitIndex = N (§5.3, §5.4)""
	matchIndexSorted := make([]int, len(rf.matchIndex))
	copy(matchIndexSorted, rf.matchIndex)
	sort.Ints(matchIndexSorted)
	commitIndex := matchIndexSorted[len(matchIndexSorted)-rf.majority] // leader's matchIndex is now taken into account

	// 3D: current commitIndex could be inside snapshot, in this case it always equals to lastIncludedIndex: snapshot contains only committed records.
	if (commitIndex >= 0) && (commitIndex > rf.commitIndex) && (rf.log[rf.absToLocal(commitIndex)].Term == rf.currentTerm) {
		commitIndexLocal := rf.absToLocal(commitIndex)
		// absToLocal() could return negative if index is inside snapshot,
		// probably here on leader it cannot actually, because new commitIndex is always greater than current commitIndex, so it's always outside snapshot,
		// but check anyway
		if (commitIndexLocal < 0) || (commitIndexLocal >= len(rf.log)) {
			return false, -1
		}
		if rf.log[commitIndexLocal].Term == rf.currentTerm {
			DPrintf("Raft instance%d (Leader) increasing commitIndex from %d to %d", rf.me, rf.commitIndex, commitIndex)
			rf.commitIndex = commitIndex
			return true, commitIndex
		}
	}
	return false, -1
}

func (rf *Raft) increaseCommitIndexOnFollower(leaderCommit int, prevLogIndex int, newEntriesCount int) bool {
	if rf.peerState == Leader {
		return false
	}
	// "If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)"
	if leaderCommit > rf.commitIndex {
		// Propagate commitIndex on heartbeat received from leader.
		// "The leader keeps track of the highest index it knows
		// to be committed, and it includes that index in future
		// AppendEntries RPCs (including heartbeats) so that the
		// other servers eventually find out."
		if newEntriesCount > 0 {
			lastNewEntryIndex := prevLogIndex + newEntriesCount
			rf.commitIndex = min(leaderCommit, lastNewEntryIndex)
		} else {
			// Heartbeat
			// follower log could contain crap after prevLogIndex on which it passed consistency check.
			// so we must not increase commit index above prevLogIndex on heartbeat and must wait until real entries are passed and the log is synchronized.
			rf.commitIndex = min(leaderCommit, prevLogIndex, rf.getAbsLogLen()-1)
		}
		return true
	}
	return false
}

func (rf *Raft) startElection() {
	for rf.killed() == false {
		// 3A: Check if a leader election should be started.
		rf.mu.Lock()
		needElection := rf.needElection()
		rf.mu.Unlock()
		if needElection {
			// Start election
			//DPrintf("Raft instance%d election timeout elapsed, starting election", rf.me)
			rf.mu.Lock()
			currentTerm, electionTimeoutMs := rf.transitionToCandidate()
			lastLogIndex := rf.getAbsLogLen() - 1
			lastLogIndexLocal := len(rf.log) - 1
			lastLogTerm := noneTerm
			if lastLogIndexLocal >= 0 {
				lastLogTerm = rf.log[lastLogIndexLocal].Term
			} else {
				if rf.lastIncludedIndexInSnapshot >= 0 {
					lastLogTerm = rf.lastIncludedTermInSnapshot
				}
			}
			rf.mu.Unlock()

			success, higherTermSeen, higherTerm, votes := rf.collectQuorumVotes(currentTerm, electionTimeoutMs, lastLogIndex, lastLogTerm)
			if success {
				//DPrintf("Raft instance%d received majority votes (%d), becoming leader", rf.me, votes)
				rf.mu.Lock()
				if rf.transitionToLeader(currentTerm) {
					DPrintf("Raft instance%d is now LEADER for term %d, votes=%d", rf.me, rf.currentTerm, votes)

				} else {
					//DPrintf("Raft instance%d transition to leader fail: already fallen back to follower", rf.me)
				}
				rf.mu.Unlock()
				rf.sendAppendEntriesToPeers() // Send initial heartbeats. Locking and leader check inside
			} else {
				if higherTermSeen {
					rf.mu.Lock()
					if rf.increaseTerm(higherTerm) {
						//DPrintf("Raft instance%d detected higher term %d while election. Converting to follower", rf.me, higherTerm)
					}
					rf.mu.Unlock()
				} else {
					//DPrintf("Raft instance%d received only %d votes, staying as candidate", rf.me, votes)
				}
			}
		}

		time.Sleep(time.Duration(electionCheckPeriodicityMs) * time.Millisecond)
	}
}

func (rf *Raft) collectQuorumVotes(currentTerm int, electionTimeoutMs int, lastLogIndex int, lastLogTerm int) (ok bool, higherTermSeen bool, higherTerm int, votes int) {
	resultCh := make(chan struct {
		ok    bool
		id    int
		reply *RequestVoteReply
	})
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func() {
			//start := time.Now()
			// term is passed as parameter so the peer votes with an immutable term atomically read when the peer was a candidate. if the term is obsolete it will be fenced.
			// cause: if term is read from the state non atomically (e.g. here), it could be already have been incremented by RPCs and the candidate already fallen back to follower.
			//        so a peer as a candidate decided to collect votes, but when it does, it collects them as a follower with a newer term. which isn't correct.
			ok, reply := rf.requestVote(i, currentTerm, rf.me, lastLogIndex, lastLogTerm)
			//DPrintf("Raft instance%d received response for RequestVote from instance %d: ok=%v voteGranted=%v. elapsed=%d", rf.me, i, ok, reply.VoteGranted, time.Since(start).Milliseconds())
			resultCh <- struct {
				ok    bool
				id    int
				reply *RequestVoteReply
			}{ok, i, reply}
		}()
	}

	votes = 1 // count me as +1 vote
	waitForCount := len(rf.peers) - 1
	waited := 0
	for {
		select {
		case <-time.After(time.Duration(electionTimeoutMs) * time.Millisecond):
			DPrintf("Raft instance%d hasn't election timeout of %d ms elapsed. Breaking election", rf.me, electionTimeoutMs)
			return false, false, noneTerm, votes
		case result := <-resultCh:
			if result.ok {
				// Candidate: if reply.Term > currentTerm: transition to follower and break election:
				// "If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)"
				if result.reply.Term > currentTerm {
					return false, true, result.reply.Term, votes
				}
				if result.reply.VoteGranted {
					votes++
					if votes >= rf.majority {
						return true, false, noneTerm, votes
					}
				}
			}
			waited++
			if waited >= waitForCount {
				DPrintf("Raft instance%d waited for all voters, but got no succesful quorum with (%d) votes", rf.me, votes)
				return false, false, noneTerm, votes
			}
		}
	}
}

func (rf *Raft) transitionToCandidate() (newTerm int, newElectionTimeoutMs int) {
	rf.transitionPeerState(Candidate) // Follower and Candidate can become a candidate. Leader cannot and will panic: something went really wrong.
	electionTimeoutMs := rf.resetElectionTimeout()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	return rf.currentTerm, electionTimeoutMs
}

func (rf *Raft) transitionToLeader(termWhenPeerWasCandidate int) (ok bool) {
	if (rf.peerState == Candidate) && (rf.currentTerm == termWhenPeerWasCandidate) { // could have been changed to Follower by RPCs
		rf.transitionPeerState(Leader)
		// initialize nextIndex and matchIndex for all peers when becoming leader
		lastLogIndex := rf.getAbsLogLen() - 1
		for i := range rf.peers {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = -1
		}
		// leader counts itself as having all its log entries
		rf.matchIndex[rf.me] = lastLogIndex
		return true
	}
	return false
}

func (rf *Raft) transitionPeerState(newState PeerState) {
	switch rf.peerState {
	case Follower:
		if newState == Follower {
			return
		}
		if newState == Leader {
			panic(fmt.Sprintf("Invalid peer transition from %d to %d", rf.peerState, newState))
		}
	case Candidate:
		if newState == Candidate {
			return
		}
	case Leader:
		if newState == Leader {
			return
		}
		if newState == Candidate {
			panic(fmt.Sprintf("Invalid peer transition from %d to %d", rf.peerState, newState))
		}
	default:
		panic(fmt.Sprintf("invalid peer state %d", newState))
	}
	rf.peerState = newState
}

func (rf *Raft) needElection() bool {
	return (rf.peerState != Leader) && rf.electionTimeoutElapsed()
}

func (rf *Raft) resetElectionTimeout() int {
	newTimeoutMs := electionTimeoutMs + ((int(rand.Int31())) % electionTimeoutRandMs) + ((int(rand.Int31())) % electionTimeoutRandMs) + ((int(rand.Int31())) % electionTimeoutRandMs)
	rf.electionTimeoutMs = newTimeoutMs
	return newTimeoutMs
}

func (rf *Raft) resetHeartbeatTimeout() {
	rf.lastHeartbeatReceivedAt = time.Now()
}

func (rf *Raft) electionTimeoutElapsed() bool {
	elapsed := time.Since(rf.lastHeartbeatReceivedAt)
	return elapsed.Milliseconds() >= int64(rf.electionTimeoutMs)
}

func (rf *Raft) voteForCandidate(candidateId, candidateTerm int, candidateLastLogIndex int, candidateLastLogTerm int) bool {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if rf.increaseTerm(candidateTerm) {
		DPrintf("Raft instance%d detected higher term %d in RequestVote request. Converting to follower", rf.me, candidateTerm)
	}

	// Reply false if args.term < currentTerm (§5.1)
	if candidateTerm < rf.currentTerm {
		return false
	}

	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	// Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer is more up-to-date.
	//
	// candidate.term >= -1 (-1 means empty log, but first term in the log is 1: initially was 0, but transition to candidate increases to 1)
	// candidate.index >= -1 (-1 means empty log, first index in the log is 0)
	// peer.term >= -1
	// peer.index >= -1 (empty log)
	// grant vote:
	// candidate.term > peer.term: candidate has newer log. grant vote.
	// candidate.term == peer.term: check log index. if candidate.index >= peer.index: candidate has newer or same log. grant vote.
	// otherwise: false.
	lastLogIndex := rf.getAbsLogLen() - 1
	lastLogIndexLocal := len(rf.log) - 1
	// TODO move to a method - here and on leader
	lastLogTerm := -1
	if lastLogIndexLocal >= 0 {
		lastLogTerm = rf.log[lastLogIndexLocal].Term
	} else {
		if rf.lastIncludedIndexInSnapshot >= 0 {
			lastLogTerm = rf.lastIncludedTermInSnapshot
		}
	}

	candidateLogGreaterOrEquals := (candidateLastLogTerm > lastLogTerm) || ((candidateLastLogTerm == lastLogTerm) && (candidateLastLogIndex >= lastLogIndex))
	if ((rf.votedFor == votedForNone) || (rf.votedFor == candidateId)) && candidateLogGreaterOrEquals {
		rf.votedFor = candidateId
		DPrintf("Raft instance%d voted for candidate %d", rf.me, candidateId)
		// Granging vote to candidate = heartbeat:
		// "If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate"
		rf.resetHeartbeatTimeout()
		rf.persist()
		return true
	} else {
		return false
	}
}

func (rf *Raft) increaseTerm(newTerm int) bool {
	if newTerm > rf.currentTerm {
		rf.currentTerm = newTerm
		rf.votedFor = votedForNone
		rf.transitionPeerState(Follower)
		rf.persist()
		return true
	}
	return false
}

func (rf *Raft) requestVote(serverid int, term int, candidateId int, lastLogIndex int, lastLogTerm int) (bool, *RequestVoteReply) {
	args := &RequestVoteArgs{
		Term:        term,
		CandidateId: candidateId,
		// 3B
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(serverid, args, reply)
	return ok, reply
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if args.CandidateId == rf.me {
		panic(fmt.Sprintf("received RequestVote from self id=%d", rf.me))
	}
	rf.mu.Lock()
	reply.VoteGranted = rf.voteForCandidate(args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	// TODO return new term from handler
	reply.Term = rf.currentTerm // set reply term after handing because term could have been increased there
	DPrintf("Raft instance%d processed RequestVote from id=%d. currentTerm=%d votedFor=%d. Result: voteGranted=%v", rf.me, args.CandidateId, rf.currentTerm, rf.votedFor, reply.VoteGranted)
	rf.mu.Unlock()
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("Raft instance%d received AppendEntries from id=%d", rf.me, args.LeaderId)
	if args.LeaderId == rf.me {
		panic(fmt.Sprintf("received AppendEntries from self id=%d", rf.me))
	}
	rf.mu.Lock()
	reply.Success, reply.XTerm, reply.XIndex, reply.XLen = rf.handleAppendEntries(args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, args.Entries) // 3B: log == nil: heartbeat
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	rf.handleInstallSnapshot(args.Term, args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type LogEntryApiModel struct { // LogEntry requires lowercase fields, API model requires uppercase fields
	Command interface{}
	Term    int
}

// field names must start with capital letters!
type AppendEntriesArgs struct {
	// 3A
	Term     int
	LeaderId int
	// 3B
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntryApiModel
	LeaderCommit int
}

// field names must start with capital letters!
type AppendEntriesReply struct {
	Term    int
	Success bool
	// 3C
	XTerm  int
	XIndex int
	XLen   int
}

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.peerState == Leader
	if isLeader {
		rf.log = append(rf.log, LogEntry{command, term})
		index = rf.getAbsLogLen() // pretend 1-indexed. If not, tests fail with "one(100) failed to reach agreement" or "got index 0 but expected 1": 1 is hardcoded in tests
		// update leader's own watermarks so leader counts itself for replication/commit
		lastLogIndex := index - 1
		rf.matchIndex[rf.me] = lastLogIndex
		rf.nextIndex[rf.me] = lastLogIndex + 1
		rf.persist() // 3C
		DPrintf("Raft instance%d (Leader) appended log entry cmd=%v to local log at index=%d (1-indexed)", rf.me, command, index)
		//rf.printLog()
	}
	rf.mu.Unlock()

	if isLeader {
		go rf.sendAppendEntriesToPeers() // don't wait for the AppendEntries work loop timeout to elapse
	}
	return index, term, isLeader
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// 3D
	rf.mu.Lock()
	index -= 1 // ! TODO index could be 1-based!
	DPrintf("Raft instance%d Snapshot call with index=%d (1-indexed)", rf.me, index)
	//rf.printLog()
	if index > rf.lastIncludedIndexInSnapshot {
		localIndex := rf.absToLocal(index) // hope index is global
		// 3D: if absToLocal(i) returns index < 0 means that i is inside snapshot, but here it's impossible
		if localIndex < 0 || localIndex >= len(rf.log) {
			panic(fmt.Sprintf("Snapshot: snapshot localIndex out of bounds (%d)", localIndex))
		}
		rf.lastIncludedTermInSnapshot = rf.log[localIndex].Term
		rf.lastIncludedIndexInSnapshot = index
		rf.snapshot = snapshot

		rf.log = rf.log[localIndex+1:] // shrink log

		DPrintf("Raft instance%d applied snapshot. lastIncludedIndexInSnapshot=%d (0-indexed), lastIncludedTermInSnapshot=%d", rf.me, rf.lastIncludedIndexInSnapshot, rf.lastIncludedTermInSnapshot)
		//rf.printLog()

		rf.persist()
	}

	rf.mu.Unlock()
}

func (rf *Raft) printLog() {
	s := "["
	for _, v := range rf.log {
		s += fmt.Sprintf(" %v(%v) ", v.Command, v.Term)
	}
	s += "]"
	DPrintf("Raft instance%d log: %v", rf.me, s)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// 3A
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.peerState == Leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {

	// "Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	// currentTerm latest term server has seen (initialized to 0
	//		on first boot, increases monotonically)
	//	votedFor candidateId that received vote in current
	//		term (or null if none)
	//	log[] log entries; each entry contains command
	//		for state machine, and term when entry
	//		was received by leader (first index is 1)"

	// Your code here (3C).
	// Example:
	// buffer := new(bytes.Buffer)
	// e := labgob.NewEncoder(buffer)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := buffer.Bytes()
	// rf.persister.Save(raftstate, nil)

	stateBuffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(stateBuffer)
	raftState := RaftState{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		Log:               rf.log, // copy?
		LastIncludedIndex: rf.lastIncludedIndexInSnapshot,
		LastIncludedTerm:  rf.lastIncludedTermInSnapshot,
	}
	encoder.Encode(raftState)
	// Save raw snapshot bytes (if any) as the persister snapshot parameter so
	// the KV server (tester) can decode the snapshot it created earlier.
	if rf.snapshot == nil {
		rf.persister.Save(stateBuffer.Bytes(), nil)
	} else {
		rf.persister.Save(stateBuffer.Bytes(), rf.snapshot)
	}

	//compareRaftStates(raftState, rf.persister)
}

func compareRaftStates(before RaftState, persister *tester.Persister) {
	// test
	loadedBytes := persister.ReadRaftState()
	buffer := bytes.NewBuffer(loadedBytes)
	decoder := gob.NewDecoder(buffer)
	var loadedState RaftState
	err := decoder.Decode(&loadedState)
	//loadedState.Log = append(loadedState.Log, LogEntry{99, 1})
	if err != nil {
		DPrintf("ERROR while decoding state: %v", err)
	}
	if before.CurrentTerm != loadedState.CurrentTerm {
		panic(fmt.Sprintf("ERROR: state mismtach after save. Before CurrentTerm=%v; After CurrentTerm=%v", before.CurrentTerm, loadedState.CurrentTerm))
	}
	if before.VotedFor != loadedState.VotedFor {
		panic(fmt.Sprintf("ERROR: state mismtach after save. Before VotedFor=%v; After VotedFor=%v", before.VotedFor, loadedState.VotedFor))
	}
	if !slices.Equal(before.Log, loadedState.Log) {
		panic(fmt.Sprintf("ERROR: state mismtach after save. Before Log=%v; After Log=%v", before.Log, loadedState.Log))
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// 3C
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	var loadedState RaftState
	err := decoder.Decode(&loadedState)
	if err != nil {
		DPrintf("ERROR while decoding state: %v", err)
	}
	rf.currentTerm = loadedState.CurrentTerm
	rf.votedFor = loadedState.VotedFor
	rf.log = loadedState.Log
	// restore snapshot metadata if present in persisted raft state
	rf.lastIncludedIndexInSnapshot = loadedState.LastIncludedIndex
	rf.lastIncludedTermInSnapshot = loadedState.LastIncludedTerm
}

func (rf *Raft) readSnapshot(data []byte) {
	// 3D: the persister snapshot should contain the raw snapshot bytes
	// produced by the service (KV server). Do not try to decode raft's
	// metadata here — metadata is persisted as part of the raft state blob.
	if data == nil || len(data) < 1 {
		return
	}
	rf.snapshot = make([]byte, len(data))
	copy(rf.snapshot, data)
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
