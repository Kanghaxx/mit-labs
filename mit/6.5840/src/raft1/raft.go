package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	heartbeatsPeriodicityMs    = 150
	electionCheckPeriodicityMs = 50
	electionTimeoutMs          = 500
)

type LogEntry struct {
	command interface{}
	term    int
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
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan raftapi.ApplyMsg
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

	// Your initialization code here (3A, 3B, 3C).
	// 3A
	rf.peerState = Follower
	rf.votedFor = votedForNone
	rf.currentTerm = 0
	rf.majority = len(rf.peers)/2 + 1
	rf.resetElectionTimeout()
	rf.resetHeartbeatTimeout()
	// 3B
	rf.log = make([]LogEntry, 0, 10)           // first index in paper is 1. But in lab 3 starting index is 0
	rf.commitIndex = -1                        // index of highest log entry known to be committed. initialized to 0, increases monotonically
	rf.lastApplied = -1                        // index of highest log entry applied to state machine. initialized to 0, increases monotonically
	rf.nextIndex = make([]int, len(rf.peers))  // for each server, index of the next log entry to send to that server
	rf.matchIndex = make([]int, len(rf.peers)) // for each server, index of highest log entry known to be replicated on server
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log) // initialized to leader last log index + 1
		rf.matchIndex[i] = -1         // initialized to 0, increases monotonically
	}
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// TODO check threading. Excessive goroutines
	go rf.startElection()
	go rf.startHeartbeats()
	go rf.startLogCommit()
	go rf.startLogApply()

	DPrintf("raft instance %d started", rf.me)

	return rf
}

func (rf *Raft) startLogApply() {
	for rf.killed() == false {
		rf.mu.Lock()
		// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].command,
				CommandIndex: rf.lastApplied + 1, // pretend 1-indexed. If not, tests fail with "one(100) failed to reach agreement" or "got index 0 but expected 1": 1 is hardcoded in tests
			}
			DPrintf("Raft instance %d has applied command at log index=%d", rf.me, rf.lastApplied)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (rf *Raft) startLogCommit() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(serverid int) {
			for rf.killed() == false {
				currentTerm, isLeader := rf.GetState() // locking inside
				if isLeader {
					rf.mu.Lock()
					leaderCommit := rf.commitIndex
					nextIndex := rf.nextIndex[serverid]
					prevLogIndex := nextIndex - 1 // prev index and term for the very first entry = -1
					prevLogTerm := noneTerm
					if prevLogIndex >= 0 {
						prevLogTerm = rf.log[prevLogIndex].term
					}
					// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
					var newEntries []LogEntryApiModel = nil
					lastLogIndex := len(rf.log) - 1
					if lastLogIndex >= nextIndex {
						// copy entries to send
						DPrintf("Raft isntance %d (Leader) has lastLogIndex=%d and nextIndex=%d. Copying entries from log", rf.me, lastLogIndex, nextIndex)
						newEntriesSlice := rf.log[nextIndex : lastLogIndex+1] // ! TODO new leader can try to send all log after failover. Mb add upper limit
						newEntries = make([]LogEntryApiModel, len(newEntriesSlice))
						for i, val := range newEntriesSlice {
							newEntries[i] = LogEntryApiModel{Term: val.term, Command: val.command}
						}
					}
					rf.mu.Unlock()
					if newEntries != nil {
						if len(newEntries) == 0 {
							panic(fmt.Sprintf("Raft instance %d (Leader) tried to send zero new entries", rf.me))
						}
						args := &AppendEntriesArgs{
							Term:         currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: prevLogIndex,
							PrevLogTerm:  prevLogTerm,
							Entries:      newEntries,
							LeaderCommit: leaderCommit,
						}
						reply := &AppendEntriesReply{}
						DPrintf("Raft isntance %d (Leader) sends AppendEntries request for %d entries, PrevLogIndex=%d count=%d", rf.me, len(newEntries), prevLogIndex, len(newEntries))
						// TODO mb send requeses in parallel to tolerate crashed servers
						// Need workarounds to keep watermark consistend:
						// 1. if success=false, decrease nextIndex only if it hasn't changed. Compare-and-set based on prevLogIndex and prevLogTerm using request-response.
						// 2. if success=true, only increase watermarks, never decrease.
						ok := rf.sendAppendEntries(serverid, args, reply)
						if ok {
							rf.mu.Lock()
							if rf.increaseTerm(reply.Term) {
								DPrintf("Raft instance %d (Leader) detected higher term %d on AppendEntries response from %d. Converting to follower", rf.me, reply.Term, serverid)
							} else {
								if reply.Success {
									// increase follower watermarks
									if rf.nextIndex[serverid] > lastLogIndex {
										panic(fmt.Sprintf("nextIndex mismatch (%d) on instance %d (Leader)", rf.nextIndex[serverid], serverid))
									}
									DPrintf("Raft instance %d (Leader) increasing nextIndex[%d] from %d to %d", rf.me, serverid, rf.nextIndex[serverid], lastLogIndex+1)
									rf.nextIndex[serverid] = lastLogIndex + 1
									rf.matchIndex[serverid] = lastLogIndex

									// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
									// set commitIndex = N (§5.3, §5.4)
									matchIndexSorted := make([]int, len(rf.matchIndex))
									copy(matchIndexSorted, rf.matchIndex)
									sort.Ints(matchIndexSorted)
									commitIndex := matchIndexSorted[rf.majority] // don't take into accout the leader: it's matchIndex isn't increased by design
									if (commitIndex >= 0) && (commitIndex > rf.commitIndex) && (rf.log[commitIndex].term == rf.currentTerm) {
										DPrintf("Raft instance %d (Leader) increasing commitIndex from %d to %d", rf.me, rf.commitIndex, commitIndex)
										rf.commitIndex = commitIndex
									}
								} else {
									// if not success, decrease nextIndex
									if rf.nextIndex[serverid] > 0 {
										DPrintf("Raft instance %d (Leader) decreasing nextIndex[%d] to %d", rf.me, serverid, rf.nextIndex[serverid]-1)
										rf.nextIndex[serverid] = rf.nextIndex[serverid] - 1
									}
								}
							}
							rf.mu.Unlock()
							continue
						}
					}
				}

				time.Sleep(time.Duration(50) * time.Millisecond)
			}
		}(i)
	}
}

func (rf *Raft) handleAppendEntries(leaderTerm int, leaderCommit int, prevLogIndex int, prevLogTerm int, newEntries []LogEntryApiModel) bool {
	if leaderTerm < rf.currentTerm {
		return false // ignore zombie leaders: Reply false if term < currentTerm (§5.1)
	}
	rf.resetHeartbeatTimeout() // reset only after term check: ignore heartbeats from zombie leaders
	if rf.increaseTerm(leaderTerm) {
		DPrintf("Raft instance %d detected higher term %d while handling heartbeat. Converting to follower", rf.me, leaderTerm)
	}
	if len(newEntries) > 0 {
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		lastLogIndex := len(rf.log) - 1
		if (prevLogIndex >= 0) && ((prevLogIndex > lastLogIndex) || (rf.log[prevLogIndex].term != prevLogTerm)) {
			return false
		}
		// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		logIndex := prevLogIndex + 1
		newEntriesIndex := 0
		for _, entry := range newEntries {
			if logIndex > lastLogIndex {
				break
			}
			if entry.Term != rf.log[logIndex].term { // ? can Entries reorder due serialization? it can lead to log reordering
				rf.log = rf.log[:logIndex]
				break
			}
			logIndex++
			newEntriesIndex++
			if newEntriesIndex == len(newEntries) {
				break
			}
		}
		// Append any new entries not already in the log
		for newEntriesIndex < len(newEntries) {
			model := newEntries[newEntriesIndex]
			DPrintf("Raft instance %d (Follower) appending entry to local log with entry term=%d", rf.me, model.Term)
			rf.log = append(rf.log, LogEntry{command: model.Command, term: model.Term})
			newEntriesIndex++
		}
	}

	//DPrintf("Raft instance %d (Follower) AppendEntries recevied. leaderCommit=%d", rf.me, leaderCommit)

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if leaderCommit > rf.commitIndex {
		lastNewEntryIndex := prevLogIndex + len(newEntries)
		oldCommitIndex := rf.commitIndex
		if lastNewEntryIndex >= 0 {
			rf.commitIndex = min(leaderCommit, lastNewEntryIndex)
		} else {
			// Propagate commitIndex on heartbeat received from leader.
			// Paper doesn't specify that heartbeats propagate commitIndex.
			// But if they don't, looks like commitIndex on followers can never increase if there is not new log entries.
			// Also, on heartbeat the follower could receive leaderCommit bigger than the log's last index, if the follower wasn't in the quorum.
			// So commitIndex must be propagated to min of leaderCommit and the log last entry index.
			lastLogIndex := len(rf.log) - 1
			rf.commitIndex = min(leaderCommit, lastLogIndex)
		}

		DPrintf("Raft instance %d (Follower) increasing commitIndex from %d to %d", rf.me, oldCommitIndex, rf.commitIndex)
	}
	return true

}

func (rf *Raft) sendHeartbeat(serverid int, term int, leaderId int, commitIndex int) (bool, *AppendEntriesReply) {
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderId,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		LeaderCommit: commitIndex,
	}
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverid, args, reply)
	return ok, reply
}

func (rf *Raft) startHeartbeats() {
	resultCh := make(chan struct {
		ok    bool
		id    int
		reply *AppendEntriesReply
	})
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(serverid int) {
			for rf.killed() == false {
				rf.mu.Lock()
				currentTerm := rf.currentTerm
				isLeader := rf.peerState == Leader
				commitIndex := rf.commitIndex
				rf.mu.Unlock()
				if isLeader {
					// Send heartbits
					// Heartbeats are sent via fire and forget. In-flight requests could stack and grow indefinitely.
					// But if sent sequentially, tests fail on bad network due to strict timing: a single lost response leads to heatrbeats pause for a peer until it times out.
					// Follow-up: throttle in-flight requests (semaphore etc).
					go func() {
						//start := time.Now()
						ok, reply := rf.sendHeartbeat(serverid, currentTerm, rf.me, commitIndex)
						//DPrintf("Raft instance %d sent heartbeat to instance %d. ok=%v elapsed=%d", rf.me, i, ok, time.Since(start).Milliseconds())
						resultCh <- struct {
							ok    bool
							id    int
							reply *AppendEntriesReply
						}{ok, serverid, reply}
					}()
				}
				time.Sleep(time.Duration(heartbeatsPeriodicityMs) * time.Millisecond)
			}
		}(i)
	}

	// handle heartbeat responses
	for result := range resultCh {
		if result.ok {
			rf.mu.Lock()
			if rf.increaseTerm(result.reply.Term) {
				DPrintf("Raft instance %d detected higher term %d on heartbeat response from %d. Converting to follower", rf.me, result.reply.Term, result.id)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startElection() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		needElection := rf.needElection()
		rf.mu.Unlock()
		if needElection {
			// Start election
			DPrintf("Raft instance %d election timeout elapsed, starting election", rf.me)
			rf.mu.Lock()
			currentTerm, electionTimeoutMs := rf.transitionToCandidate()
			rf.mu.Unlock()

			success, higherTermSeen, higherTerm, votes := rf.collectQuorumVotes(currentTerm, electionTimeoutMs)
			if success {
				DPrintf("Raft instance %d received majority votes (%d), becoming leader", rf.me, votes)
				rf.mu.Lock()
				if rf.transitionToLeader(currentTerm) {
					DPrintf("Raft instance %d is now LEADER for term %d", rf.me, rf.currentTerm)
				} else {
					DPrintf("Raft instance %d transition to leader fail: already fallen back to follower", rf.me)
				}
				rf.mu.Unlock()
			} else {
				if higherTermSeen {
					rf.mu.Lock()
					if rf.increaseTerm(higherTerm) {
						DPrintf("Raft instance %d detected higher term %d while election. Converting to follower", rf.me, higherTerm)
					}
					rf.mu.Unlock()
				} else {
					DPrintf("Raft instance %d received only %d votes, staying as candidate", rf.me, votes)
				}
			}
		}

		time.Sleep(time.Duration(electionCheckPeriodicityMs) * time.Millisecond)
	}
}

func (rf *Raft) collectQuorumVotes(currentTerm int, electionTimeoutMs int) (ok bool, higherTermSeen bool, higherTerm int, votes int) {
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
			start := time.Now()
			// term is passed as parameter so the peer votes with an immutable term atomically read when the peer was a candidate. if the term is obsolete it will be fenced.
			// cause: if term is read from the state non atomically (e.g. here), it could be already have been incremented by RPCs and the candidate already fallen back to follower.
			//        so a peer as a candidate decided to collect votes, but when it does, it collects them as a follower with a newer term. which isn't correct.
			ok, reply := rf.requestVote(i, currentTerm, rf.me)
			DPrintf("Raft instance %d received response for RequestVote from instance %d: ok=%v voteGranted=%v. elapsed=%d", rf.me, i, ok, reply.VoteGranted, time.Since(start).Milliseconds())
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
			DPrintf("Raft instance %d hasn't election timeout of %d ms elapsed. Breaking election", rf.me, electionTimeoutMs)
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
				DPrintf("Raft instance %d waited for all voters, but got no succesful quorum with (%d) votes", rf.me, votes)
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
	return rf.currentTerm, electionTimeoutMs
}

func (rf *Raft) transitionToLeader(termWhenPeerWasCandidate int) (ok bool) {
	// TODO transision success only if enough votes received proveiously whic stored in a field
	//   mb not needed: the method isn't supposed to be public, no need to store votes as field.
	//   but mb needed: the Raft class is state of the Raft consensus. And the outer cycle is a client to the Raft class.
	if (rf.peerState == Candidate) && (rf.currentTerm == termWhenPeerWasCandidate) { // could have been changed to Follower by RPCs
		rf.transitionPeerState(Leader)
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
	newTimeoutMs := electionTimeoutMs + (int(rand.Int31()) % electionTimeoutMs)
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

func (rf *Raft) voteForCandidate(candidateId, candidateTerm int) bool {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if rf.increaseTerm(candidateTerm) {
		DPrintf("Raft instance %d detected higher term %d in RequestVote request. Converting to follower", rf.me, candidateTerm)
	}
	// 1. Reply false if args.term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	//    least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	// TODO  check log watermarks (3B)
	if (rf.currentTerm <= candidateTerm) && ((rf.votedFor == votedForNone) || (rf.votedFor == candidateId)) {
		rf.votedFor = candidateId // TODO persistence (3C)
		// Granging vote to candidate = heartbeat:
		// "If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate"
		rf.resetHeartbeatTimeout()
		return true
	}
	return false
}

func (rf *Raft) increaseTerm(newTerm int) bool {
	if newTerm > rf.currentTerm {
		// TODO persistence (3C)
		rf.currentTerm = newTerm
		rf.votedFor = votedForNone
		rf.transitionPeerState(Follower)
		return true
	}
	return false
}

func (rf *Raft) requestVote(serverid int, term int, candidateId int) (bool, *RequestVoteReply) {
	args := &RequestVoteArgs{
		Term:        term,
		CandidateId: candidateId,
		// 3B
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

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// ??? Why not 3C? We don't need to persist currentTerm and votedFor?
	if args.CandidateId == rf.me {
		panic(fmt.Sprintf("received RequestVote from self id=%d", rf.me))
	}
	rf.mu.Lock()
	reply.VoteGranted = rf.voteForCandidate(args.CandidateId, args.Term)
	reply.Term = rf.currentTerm
	DPrintf("Raft instance %d processed RequestVote from id=%d. currentTerm=%d votedFor=%d. Result: voteGranted=%v", rf.me, args.CandidateId, rf.currentTerm, rf.votedFor, reply.VoteGranted)
	rf.mu.Unlock()
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("Raft instance %d received AppendEntries from id=%d", rf.me, args.LeaderId)
	if args.LeaderId == rf.me {
		panic(fmt.Sprintf("received AppendEntries from self id=%d", rf.me))
	}
	rf.mu.Lock()
	reply.Success = rf.handleAppendEntries(args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, args.Entries) // 3B: log == nil: heartbeat
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
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
		index = len(rf.log) // pretend 1-indexed. If not, tests fail with "one(100) failed to reach agreement" or "got index 0 but expected 1": 1 is hardcoded in tests
		DPrintf("Raft instance %d (Leader) appended log entry to local log at index=%d", rf.me, index)
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
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
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
