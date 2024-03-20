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
//import "github.com/sasha-s/go-deadlock"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"
import "sort"
import "bytes"
import "../labgob"



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
  Term int
  Command interface{}
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
	nextIndex []int
	matchIndex []int
	committedIndex int
	lastApplied int
	applyCondMu sync.Mutex
	applyCond *sync.Cond
	applyCh  chan ApplyMsg
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
	b := new(bytes.Buffer)
	e := labgob.NewEncoder(b)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := b.Bytes()
	rf.persister.SaveRaftState(data)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []LogEntry
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
	  panic("readPersist panic")
	} else {
	  rf.term = term
	  rf.votedFor = votedFor
	  rf.log = log
	}
}

type AppendEntriesArgs struct {
  Term int
  LeaderId int
  Entries []LogEntry
  PrevLogIndex int
  PrevLogTerm int
  LeaderCommit int
}

type AppendEntriesReply struct {
  Term int
  Success bool
  Xterm int //conflicting follower's log entry, -1 if none
  Xindex int //first index of Xterm
  Xlen int // len of follower's log
}

func min(i, j int) int {
  if i < j {
    return i
  }
  return j
}

//handler for AppendEntries RPC
func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
  //rf.applyCondMu.Lock()
  rf.mu.Lock()
  reply.Term = rf.term
  reply.Success = false
  if args.Term < rf.term {
    DPrintf("%d Received a RequestAppendEntries from %d with term %d smaller then %d , discarded", rf.me, args.LeaderId, args.Term, rf.term)
    rf.mu.Unlock()
    return
  }

  rf.leaderTimestamp = time.Now()
  //rf.votedFor = -1
  rf.leaderId = args.LeaderId
  rf.state = Follower
  rf.term = args.Term

  rf.persist()

  if len(rf.log) <= args.PrevLogIndex {
      //case 3
      DPrintf("%d has too less logs in term %d", rf.me, rf.term)
      reply.Xterm = -1
      reply.Xindex = -1
      reply.Xlen = len(rf.log)
      rf.mu.Unlock()
      return
  }
  if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
    reply.Xterm = rf.log[args.PrevLogIndex].Term
    index := args.PrevLogIndex
    for ; index >= 1; index-- {
      if rf.log[index-1].Term != reply.Xterm {
	break
      }
    }
    DPrintf("first occurance of term %d is at log index %d for follower %d", rf.log[args.PrevLogIndex].Term, index, rf.me)
    reply.Xindex = index
    rf.mu.Unlock()
    return
  }

  reply.Success = true
  if len(args.Entries) == 0 {
    //DPrintf("%d Received heartbeat from %d with term %d and commitedIndex %d and lastapplied = %d", rf.me, args.LeaderId, args.Term, args.LeaderCommit, rf.lastApplied)
  } else {
    //find the first missing or conflicting log index
    offset := 0
    for ; offset < len(args.Entries); offset++ {
      followerIndex := args.PrevLogIndex+offset+1
      if followerIndex >= len(rf.log) || args.Entries[offset].Term != rf.log[followerIndex].Term {
	break
      }
    }
    //all match, must be from past, no need to do anything
    if offset == len(args.Entries) {
      DPrintf("%d is already up to date, no need to append", rf.me)
      rf.mu.Unlock()
      return
    }

    rf.log = rf.log[:args.PrevLogIndex+offset+1]
    for ; offset < len(args.Entries); offset++ {
      rf.log = append(rf.log, args.Entries[offset])
    }

    rf.persist()

    DPrintf("%d successfully append new log, last log index now = %d", rf.me, len(rf.log)-1)
  }
  rf.applyCondMu.Lock()
  if args.LeaderCommit > rf.committedIndex {
    rf.committedIndex = min(args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
    rf.applyCond.Broadcast()
  }
  rf.applyCondMu.Unlock()
  rf.mu.Unlock()
  //rf.applyCondMu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

func (rf *Raft) CallAppendEntries(server int) {
  rf.mu.Lock()

  if rf.state != Leader {
    DPrintf("%d is not leader anymore, don't sendRPC", rf.me)
    rf.mu.Unlock()
    return
  }

  args := AppendEntriesArgs{}
  reply := AppendEntriesReply{}
  args.Term = rf.term
  args.LeaderId = rf.me
  args.LeaderCommit = rf.committedIndex
  args.PrevLogIndex = rf.nextIndex[server]-1
  args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
  // neccesary?
  if rf.nextIndex[server] > len(rf.log) {
    panic("rf.nextIndex > len(rf.log)")
    //DPrintf("%d is up to date, %d return from sendRPC", server, rf.me)
    //rf.mu.Unlock()
    //return
  }
  args.Entries = rf.log[rf.nextIndex[server]:]
  DPrintf("%d sending appendRPC to %d in term %d, it's nextIndex = %d", rf.leaderId, server, rf.term, rf.nextIndex[server])
  rf.mu.Unlock()
  rf.sendAppendEntries(server, &args, &reply)

  rf.mu.Lock()
  //advise form student's guide to RAFT: Term confusion
  //if reply.Term < rf.term || rf.state != Leader{
  if args.Term != rf.term || rf.state != Leader{
    DPrintf("%d AppendEntriesRPC received from %d with old term %d which is less then curterm %d , or not leader anymore so return", rf.me, server, reply.Term, args.Term)
    rf.mu.Unlock()
    return
  }

  //if term change when received reply, step down
  if reply.Term > rf.term {
    DPrintf("%d AppendRPC received old reply from %d with term %d conflicting with curTerm %d, step down to follower", rf.me, server, reply.Term, rf.term)
    rf.state = Follower
    rf.leaderId = -1
    rf.votedFor = -1
    rf.mu.Unlock()
    return
  }

  if reply.Success {
    DPrintf("Leader %d in term %d receive Success from %d in term %d", rf.me, args.Term, server, reply.Term)
    if rf.nextIndex[server] < args.PrevLogIndex + len(args.Entries) + 1 {
      rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
    }
    if rf.matchIndex[server] < args.PrevLogIndex + len(args.Entries) {
      rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
   }
  } else {
    if reply.Xterm == -1 {
      rf.nextIndex[server] = reply.Xlen
    } else {
      lastXtermIndex := lastXtermIndex(rf.log, reply.Xterm)
      if lastXtermIndex != -1 {
	rf.nextIndex[server] = lastXtermIndex+1
      } else {
	rf.nextIndex[server] = reply.Xindex
      }
    }
  }
  rf.mu.Unlock()
  //labs hint suggest we sleep between loops
}

func lastXtermIndex(log []LogEntry, Xterm int) int {
  l := 0
  r := len(log)
  for; l < r ; {
    m := l + (r-l)/2
    if log[m].Term <= Xterm {
      l = m+1
    } else if log[m].Term > Xterm {
      r = m
    }
  }
  if l == 0 && log[l].Term == Xterm {
    return 0
  }
  if log[l-1].Term != Xterm {
    return -1
  }
  return l-1
}

func (rf *Raft) doHeartbeat() {
  for server, _ := range rf.peers {
    if server == rf.me {
      continue
    }
    go func(server int) {
      rf.CallAppendEntries(server)
    }(server)
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
	LastLogIndex int
	LastLogTerm int
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
	  DPrintf("%d reject vote for %d since %d's term %d is bigger then %d's %d", rf.me, args.CandidateId, rf.me, rf.term, args.CandidateId, args.Term)
	  return
	}
	if args.Term > rf.term {
	  DPrintf("%d in term %d received voteRequest from %d with higher term %d, updating term and covert to follower", rf.me, rf.term, args.CandidateId, args.Term)
	  rf.term = args.Term
	  rf.state = Follower
	  rf.votedFor = -1
	  rf.persist()
	}
	if args.LastLogTerm < rf.log[len(rf.log)-1].Term {
	  DPrintf("%d reject vote for %d since %d's last logTerm %d is bigger then %d's %d", rf.me, args.CandidateId, rf.me, rf.log[len(rf.log)-1].Term, args.CandidateId, args.LastLogTerm)
	  return
	}
	if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1 {
	DPrintf("%d reject vote for %d since %d's lastlogIndex %d is bigger then %d's %d", rf.me, args.CandidateId, rf.me, len(rf.log)-1, args.CandidateId, args.LastLogIndex)
	  return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
	  reply.VoteGranted = true
	  rf.term = args.Term
	  rf.votedFor = args.CandidateId
	  rf.leaderTimestamp = time.Now()
	  rf.state = Follower
	  DPrintf("%d vote for %d in term %d case1", rf.me, args.CandidateId, rf.term)

	  rf.persist()
	}
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

func (rf *Raft) CallRequestVote(term, candidateId, server, lastLogIndex, lastLogTerm int) *RequestVoteReply{
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	args.Term = term
	args.CandidateId = candidateId
	args.LastLogIndex = lastLogIndex
	args.LastLogTerm = lastLogTerm
	DPrintf("%d asking %d for vote in term %d", candidateId, server, term)
	ok := rf.sendRequestVote(server, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.term != args.Term{
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
	rf.mu.Lock()
	if rf.state != Leader {
	  rf.mu.Unlock()
	  isLeader = false
	  return index, term, isLeader
	}
	DPrintf("LeaderId %d Start!!", rf.me)

	newlog := LogEntry{}
	newlog.Term = rf.term
	newlog.Command = command
	index = len(rf.log)
	term = rf.term
	rf.log = append(rf.log, newlog)

	rf.persist()

	rf.mu.Unlock()
	/*
	for server, _ := range rf.peers {
	  if server == rf.me {
	    continue
	  } else {
	    go rf.CallAppendEntries(server, false)
	  }
	}
	*/

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

// attmpeelection, return should we restart a new election
func (rf *Raft) attemptElection(electionInterval, electionTime int) bool{
  rf.mu.Lock()
  restart := false
  t := time.Now()
  //if no need to start election, return
  if rf.state == Leader || t.Sub(rf.leaderTimestamp).Milliseconds() < int64(electionInterval) {
    rf.mu.Unlock()
    return restart
  }
  condMu := sync.Mutex{}
  cond := sync.NewCond(&condMu)
  // setting up election timer
  go func() {
    time.Sleep(time.Duration(electionTime) * time.Millisecond)
    condMu.Lock()
    defer condMu.Unlock()
    restart = true
    cond.Broadcast()
  }()

  count := 1
  rf.state = Candidate
  rf.term += 1
  rf.votedFor = rf.me
  rf.leaderId = -1
  curTerm := rf.term
  lastLogIndex := len(rf.log)-1
  lastLogTerm := rf.log[lastLogIndex].Term

  rf.persist()

  rf.mu.Unlock()

  for server, _ := range rf.peers{
    if server == rf.me {
      continue
    }
    go func(term, me, server, lastLogIndex, lastLogTerm int) {
      reply := rf.CallRequestVote(term, me, server, lastLogIndex, lastLogTerm)
      condMu.Lock()
      defer condMu.Unlock()
      if reply != nil && reply.VoteGranted {
	count += 1
	cond.Broadcast()
      }
    }(curTerm, rf.me, server, lastLogIndex, lastLogTerm)
  }

  condMu.Lock()
  for ;count < (len(rf.peers)/2)+1 && !restart; {
    cond.Wait()
  }

  rf.mu.Lock()
  defer rf.mu.Unlock()
  if curTerm < rf.term || rf.state != Candidate {
    DPrintf("%d lose the election since the term from %d to %d or we are no longer a Candidate", rf.me, curTerm, rf.term)
  } else if count >= (len(rf.peers)/2)+1 {
    rf.state = Leader
    rf.leaderId = rf.me
    rf.initializeIndex()
    rf.doHeartbeat()
    DPrintf("---%d sending heartbeat upon being leader---", rf.me)
  } else {
    DPrintf("term %d election timeout", rf.term)
  }
  return restart
}

func (rf *Raft) initializeIndex() {
  //Already holding lock, no need to lock
  for i := 0; i < len(rf.peers); i++ {
    rf.matchIndex[i] = 0
    rf.nextIndex[i] = len(rf.log)
  }
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

	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(rf.peers); i++ {
	  if i == rf.me {
	    continue
	  }
	  rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))
	rf.committedIndex = 0
	rf.lastApplied = 0
	rf.applyCondMu = sync.Mutex{}
	rf.applyCond = sync.NewCond(&rf.applyCondMu)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// applier
	go func() {
	  for rf.killed() == false {
	    rf.applyCondMu.Lock()
	    for ;; {
	      if rf.lastApplied < rf.committedIndex {
		break
	      }
	      rf.applyCond.Wait()
	    }
	    //rf.mu.Lock()
	    DPrintf("lastapplied = %d, committedIndex = %d", rf.lastApplied, rf.committedIndex)
	    for ;rf.lastApplied < rf.committedIndex;{
	      rf.lastApplied++
	      msg := ApplyMsg{}
	      msg.CommandValid = true
	      msg.Command = rf.log[rf.lastApplied].Command
	      msg.CommandIndex = rf.lastApplied
	      DPrintf("%d's lastApplied = %d",rf.me, rf.lastApplied)
	      rf.applyCh <- msg
	    }
	    //rf.mu.Unlock()
	    rf.applyCondMu.Unlock()
	  }
	}()
	// heartbeat
	go func() {
	  for rf.killed() == false {
	    rf.mu.Lock()
	    if rf.state == Leader {
	      rf.doHeartbeat()
	    }
	    rf.mu.Unlock()
	    time.Sleep(100 * time.Millisecond)
	  }
	}()
	//election
	go func() {
	  for rf.killed() == false{
	    r := rand.New(rand.NewSource(time.Now().UnixNano()))
	    electionTimeout := 100 + r.Intn(200)
	    electionInterval := 300
	    electionTime := 100 + r.Intn(200)
	    restart := rf.attemptElection(electionInterval, electionTime)
	    if !restart {
	      time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
	    }
	  }
	}()
	//CommitChecker
	go func() {
	  for rf.killed() == false {
	    //rf.applyCondMu.Lock()
	    rf.mu.Lock()
	    if rf.state == Leader {
	      matched := make([]int, 0)
	      for i, _ := range rf.peers {
		if i == rf.me {
		  matched = append(matched, len(rf.log)-1)
		} else {
		  matched = append(matched, rf.matchIndex[i])
		}
	      }
	      sort.Ints(matched)
	      majority := matched[(len(matched)-1)/2]
	      curTerm := rf.term
	      rf.applyCondMu.Lock()
	      if rf.committedIndex < majority && rf.log[majority].Term == curTerm {
		DPrintf("Before commitedIndex = %d, after = %d for %d", rf.committedIndex, majority, rf.me)
		rf.committedIndex = majority
		rf.applyCond.Broadcast()
	      }
	      rf.applyCondMu.Unlock()
	    }
	    rf.mu.Unlock()
	    //rf.applyCondMu.Unlock()
	    time.Sleep(300 * time.Millisecond)
	  }
	}()


	return rf
}
