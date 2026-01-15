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
)

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
	peerState               PeerState
	currentTerm             int
	votedFor                int
	electionTimeoutMs       int
	majority                int
	lastHeartbeatReceivedAt time.Time
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
	rf.peerState = Follower
	rf.votedFor = votedForNone
	rf.currentTerm = 0
	rf.majority = len(rf.peers)/2 + 1
	rf.resetElectionTimeout()
	rf.resetHeartbeatTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startElection()
	go rf.startHeartbeats()

	DPrintf("raft instance %d started", rf.me)

	return rf
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
				peerState := rf.peerState
				currentTerm := rf.currentTerm
				rf.mu.Unlock()
				if peerState == Leader {
					// send heartbits
					args := &AppendEntriesArgs{
						Term:     currentTerm,
						LeaderId: rf.me,
					}
					reply := &AppendEntriesReply{}
					start := time.Now()
					// Heartbeats are sent via fire and forget. In-flight requests could stack and grow indefinitely.
					// But if sent sequentially, tests fail on bad network due to strict timing: a single lost response leads to heatrbeats pause for a peer until it times out.
					// Follow-up: throttle in-flight requests (semaphore etc).
					go func() {
						ok := rf.sendAppendEntries(serverid, args, reply)
						DPrintf("Raft instance %d sent heartbeat to instance %d. ok=%v elapsed=%d", rf.me, i, ok, time.Since(start).Milliseconds())
						resultCh <- struct {
							ok    bool
							id    int
							reply *AppendEntriesReply
						}{ok, serverid, reply}
					}()
				}
				time.Sleep(time.Duration(150) * time.Millisecond)
			}
		}(i)
	}

	for result := range resultCh {
		if result.ok {
			rf.mu.Lock()
			if rf.increaseTerm(result.reply.Term) {
				DPrintf("Raft instance %d detected higher term %d on heartbeat response from %d. Converting to follower", rf.me, result.id, result.reply.Term)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startElection() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		startElection := false
		rf.mu.Lock()
		startElection = (rf.peerState != Leader) && rf.electionTimeoutElapsed()
		rf.mu.Unlock()
		if startElection {
			// Start election
			DPrintf("Raft instance %d election timeout elapsed, starting election", rf.me)

			rf.mu.Lock()
			rf.transitionPeerState(Candidate)
			electionTimeoutMs := rf.resetElectionTimeout()
			rf.currentTerm++
			currentTerm := rf.currentTerm
			rf.votedFor = rf.me
			rf.mu.Unlock()

			success, votes := rf.collectQuorumVotes(currentTerm, electionTimeoutMs)
			if success {
				DPrintf("Raft instance %d received majority votes (%d), becoming leader", rf.me, votes)
				rf.mu.Lock()
				// TODO why this breaks condition?: && (rf.currentTerm == currentTerm)
				// mb not: it passed and fails randomly. may be another reason
				if (rf.peerState == Candidate) && (rf.currentTerm == currentTerm) { // could have been changed to Follower by RPCs
					rf.transitionPeerState(Leader)
					DPrintf("Raft instance %d is now LEADER for term %d", rf.me, rf.currentTerm)
				} else {
					DPrintf("Raft instance %d transition to leader fail: already fallen back to follower", rf.me)
				}
				rf.mu.Unlock()
			} else {
				DPrintf("Raft instance %d received only %d votes, staying as candidate", rf.me, votes)
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 350)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (rf *Raft) resetElectionTimeout() int {
	newTimeoutMs := 500 + (int(rand.Int31()) % 500)
	rf.electionTimeoutMs = newTimeoutMs
	return newTimeoutMs
}

func (rf *Raft) collectQuorumVotes(currentTerm int, electionTimeoutMs int) (bool, int) {
	args := &RequestVoteArgs{
		// term is passed as parameter so the peer votes with an immutable term atomically read when the peer was a candidate. if the term is obsolete it will be fenced.
		// cause: if term is read from the state non atomically (e.g. here), it could be already have been incremented by RPCs and the candidate already fallen back to follower.
		//        so a peer as a candidate decided to collect votes, but when it does, it collects them as a follower with a newer term. which isn't correct.
		Term:        currentTerm,
		CandidateId: rf.me,
		// 3B
	}
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
			//DPrintf("Raft instance %d requests vote from instance %d", rf.me, i)
			reply := &RequestVoteReply{}
			start := time.Now()
			ok := rf.sendRequestVote(i, args, reply)
			DPrintf("Raft instance %d received response for RequestVote from instance %d: ok=%v voteGranted=%v. elapsed=%d", rf.me, i, ok, reply.VoteGranted, time.Since(start).Milliseconds())
			resultCh <- struct {
				ok    bool
				id    int
				reply *RequestVoteReply
			}{ok, i, reply}
		}()
	}

	votes := 1 // count me as +1 vote
	waitForCount := len(rf.peers) - 1
	waited := 0
	for {
		select {
		case <-time.After(time.Duration(electionTimeoutMs) * time.Millisecond):
			DPrintf("Raft instance %d hasn't election timeout of %d ms elapsed. Breaking election", rf.me, electionTimeoutMs)
			return false, votes
		case result := <-resultCh:
			//DPrintf("Raft instance %d retrieved response for RequestVote from instance %d from the channel", rf.me, result.id)
			if result.ok {
				// Candidate: if reply.Term > currentTerm: transition to follower and break election:
				// "If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)"
				higherTermSeen := false
				rf.mu.Lock()
				if rf.increaseTerm(result.reply.Term) {
					higherTermSeen = true
					DPrintf("Raft instance %d detected higher term %d while election. Converting to follower", rf.me, result.reply.Term)
				}
				rf.mu.Unlock()
				if higherTermSeen {
					return false, votes // not a candidate anymore
				}

				if result.reply.VoteGranted {
					votes++
					if votes >= rf.majority {
						return true, votes
					}
				}
			}
			waited++
			if waited >= waitForCount {
				DPrintf("Raft instance %d waited for all voters, but got no succesful quorum with (%d) votes", rf.me, votes)
				return false, votes
			}
		}
	}
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

func (rf *Raft) resetHeartbeatTimeout() {
	rf.lastHeartbeatReceivedAt = time.Now()
}

func (rf *Raft) electionTimeoutElapsed() bool {
	elapsed := time.Since(rf.lastHeartbeatReceivedAt)
	return elapsed.Milliseconds() >= int64(rf.electionTimeoutMs)
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

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// ??? Why not 3C? We don't need to persist currentTerm and votedFor?
	if args.CandidateId == rf.me {
		panic(fmt.Sprintf("received RequestVote from self id=%d", rf.me))
	}

	rf.mu.Lock()
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if rf.increaseTerm(args.Term) {
		DPrintf("Raft instance %d detected higher term %d in RequestVote request. Converting to follower", rf.me, args.Term)
	}

	reply.Term = rf.currentTerm
	// 1. Reply false if args.term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	//    least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	reply.VoteGranted = false
	// TODO  check log watermarks (3B)
	if (rf.currentTerm <= args.Term) && ((rf.votedFor == votedForNone) || (rf.votedFor == args.CandidateId)) {
		// TODO persistence (3C)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// Granging vote to candidate = heartbeat:
		// "If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate"
		rf.resetHeartbeatTimeout()
	}
	DPrintf("Raft instance %d processed RequestVote from id=%d. currentTerm=%d votedFor=%d. Result: voteGranted=%v", rf.me, args.CandidateId, rf.currentTerm, rf.votedFor, reply.VoteGranted)
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("Raft instance %d received AppendEntries from id=%d", rf.me, args.LeaderId)
	if args.LeaderId == rf.me {
		panic(fmt.Sprintf("received AppendEntries from self id=%d", rf.me))
	}

	rf.mu.Lock()
	reply.Success = false
	if args.Term >= rf.currentTerm {
		reply.Success = true
		rf.resetHeartbeatTimeout() // reset only after term check: ignore heartbeats from zombie leaders
		if rf.increaseTerm(args.Term) {
			DPrintf("Raft instance %d detected higher term %d while handling heartbeat. Converting to follower", rf.me, args.Term)
		}
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
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

// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
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
