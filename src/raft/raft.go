package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type State string

const (
  Follower State = "follower"
  Candidate = "candidate"
  Leader = "leader"
)

type LogEntry struct {
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	term      int
	votedFor  int
	log  []LogEntry
	leaderId  int
	leaderTimestamp time.Time
	state State
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.term
	isleader = rf.me == rf.leaderId
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

type AppendEntriesArgs struct {
  Term int
  LeaderId int
  Entries []LogEntry
}

type AppendEntriesReply struct {
  Term int
  Success bool
}

//handler for AppendEntries RPC
func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
  rf.mu.Lock()
  defer rf.mu.Unlock()
  reply.Term = rf.term
  reply.Success = false
  if args.Term < rf.term {
    DPrintf("%d Received a RequestAppendEntries from %d with term %d smaller then %d, discarded", rf.me, args.LeaderId, args.Term, rf.term)
    return
  }
  //heartbeat condition
  if len(args.Entries) == 0 {
    rf.term = args.Term
    rf.leaderTimestamp = time.Now()
    rf.votedFor = -1
    rf.state = Follower
    rf.leaderId = args.LeaderId
    reply.Success = true
    DPrintf("%d Received heartbeat from %d with term %d", rf.me, args.LeaderId, args.Term)
  }
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

func (rf *Raft) CallAppendEntries(term, me, server int) {
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}
	args.Entries = make([]LogEntry, 0)
	args.Term = term
	args.LeaderId = me
	DPrintf("%d sending heartbeat to %d in term %d", me, server, term)
	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
	  return
	}
	if !reply.Success {
	  rf.mu.Lock()
	  defer rf.mu.Unlock()
	  DPrintf("%d heartbeat received false from %d, setting term from %d to %d", me, server, term, reply.Term)
	  rf.state = Follower
	  rf.term = reply.Term
	  rf.votedFor = -1
	}
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.term
	DPrintf("%d received voteReqeust from %d with term %d", rf.me, args.CandidateId, args.Term)
	if args.Term < rf.term {
	  return
	}
	if args.Term > rf.term || rf.votedFor == -1 || rf.votedFor == args.CandidateId {
	  reply.VoteGranted = true
	  rf.term = args.Term
	  rf.votedFor = args.CandidateId
	  rf.leaderTimestamp = time.Now()
	  rf.state = Follower
	  DPrintf("%d vote for %d in term %d case1", rf.me, args.CandidateId, rf.term)
	}
	/*
	if args.Term > rf.term {
	  reply.VoteGranted = true
	  rf.term = args.Term
	  rf.votedFor = args.CandidateId
	  rf.leaderTimestamp = time.Now()
	  rf.state = Follower
	  DPrintf("%d vote for %d in term %d case2", rf.me, args.CandidateId, rf.term)
	}
	*/
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) CallRequestVote(term, candidateId, server int) *RequestVoteReply{
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	args.Term = term
	args.CandidateId = candidateId
	DPrintf("%d asking %d for vote in term %d", candidateId, server, term)
	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
	  return nil
	}
	return &reply
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) attemptElection(r *rand.Rand) bool{
  condMu := sync.Mutex{}
  cond := sync.NewCond(&condMu)
  count := 1
  nextRun := false
  electionTimeout := 300 + r.Intn(150)
  // setting up election timer
  go func() {
    time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
    condMu.Lock()
    defer condMu.Unlock()
    nextRun = true
    cond.Broadcast()
  }()
  rf.state = Candidate
  rf.term += 1
  rf.votedFor = rf.me
  rf.leaderId = -1
  curTerm := rf.term
  rf.mu.Unlock()
  for server, _ := range rf.peers{
    if server == rf.me {
      continue
    }
    go func(term, me, server int) {
      reply := rf.CallRequestVote(term, me, server)
      condMu.Lock()
      defer condMu.Unlock()
      if reply != nil && reply.VoteGranted {
	count += 1
	cond.Broadcast()
      }
    }(curTerm, rf.me, server)
  }
  condMu.Lock()
  for count < (len(rf.peers)/2)+1 && !nextRun {
    cond.Wait()
  }
  rf.mu.Lock()
  if curTerm < rf.term || rf.state != Candidate {
    DPrintf("%d lose the election since the term from %d to %d", rf.me, curTerm, rf.term)
    DPrintf("or we are no longer a Candidate for some reason")
  } else if count >= (len(rf.peers)/2)+1 {
    rf.state = Leader
    rf.leaderId = rf.me
    DPrintf("%d is leader now in term %d", rf.me, rf.term)
  } else {
    DPrintf("term %d election timeout", rf.term)
  }
  return nextRun
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.dead = 0
	rf.term = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry,1)
	rf.leaderId = -1
	rf.leaderTimestamp = time.Now()
	rf.state = Follower

	go func() {
	  for {
	    rf.mu.Lock()
	    if rf.state == Leader {
	      rf.leaderTimestamp = time.Now()
	      curTerm := rf.term
	      me := rf.me
	      for server, _ := range rf.peers {
		if server == me {
		  continue
		}
		go func(term, me, server int) {
		  rf.CallAppendEntries(term, me, server)
		}(curTerm, me, server)
	      }
	    }
	    rf.mu.Unlock()
	    time.Sleep(200 * time.Millisecond)
	  }
	}()
	go func() {
	  r := rand.New(rand.NewSource(int64(rf.me)))
	  for{
	    electionTimeout := 300 + r.Intn(150)
	    //DPrintf("%d electionInterval", electionTimeout)
	    t := time.Now()
	    rf.mu.Lock()
	    rf.votedFor = -1
	    nextRun := false
	    //DPrintf("time diff = %d", t.Sub(rf.leaderTimestamp).Milliseconds())
	    if t.Sub(rf.leaderTimestamp).Milliseconds() >= int64(electionTimeout) || nextRun {
	      //startElection
	      nextRun = rf.attemptElection(r)
	    }
	    rf.mu.Unlock()
	    time.Sleep(300 * time.Millisecond)
	  }
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
