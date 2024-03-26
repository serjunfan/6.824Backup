package kvraft

import (
	"../labgob"
	"../labrpc"
	//"log"
	"../raft"
	"sync"
	"sync/atomic"
	"strings"
)


/*
const D = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if D > 0 {
		log.Printf(format, a...)
	}
	return
}
*/

/*
type OpType string
const (
  Get OpType = "Get"
  Put OpType = "Put"
  Append OpType = "Append"
)
*/

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string
	Key string
	Value string
	CommandId int
	ClientId int
	LeaderId int
}

type Result struct {
  Value string
  CommandId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	applyChans map[int] chan Result
	kvMap map[string] string
	lastRequest map[int] Result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	Debug(dServer, "%d received Get from %d with key %v", kv.me, args.ClientId, args.Key)

	kv.mu.Lock()
	if kv.lastRequest[args.ClientId].CommandId == args.CommandId {
	  reply.Err = ""
	  reply.Value = kv.lastRequest[args.ClientId].Value
	  kv.mu.Unlock()
	  return
	}
	kv.mu.Unlock()
	command := Op{
	  Type: "Get",
	  Key: args.Key,
	  CommandId: args.CommandId,
	  ClientId: args.ClientId,
	  LeaderId: kv.me,
	}
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
	 reply.Err = "NotLeader"
	 //Debug(dServer, "%d replies with notLeader",kv.me)
	 return
        }
	// is possible for applier to see no chan in map?
	ch := make(chan Result)
	kv.applyChans[args.ClientId] = ch
	select {
	case res := <-kv.applyChans[args.ClientId]:
	  reply.Value = res.Value
	  reply.Err = ""
	}
	Debug(dServer, "%d reply.Value = %v, reply.Err = %v", kv.me, reply.Value, reply.Err)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	Debug(dServer, "%d received %s from %d with key %s and value %s", kv.me, args.Op, args.ClientId, args.Key, args.Value)

	kv.mu.Lock()
	if kv.lastRequest[args.ClientId].CommandId == args.CommandId {
	  reply.Err = ""
	  kv.mu.Unlock()
	  return
	}
	kv.mu.Unlock()
	command := Op{
	  Type: args.Op,
	  Key: args.Key,
	  Value: args.Value,
	  CommandId: args.CommandId,
	  ClientId: args.ClientId,
	  LeaderId: kv.me,
	}
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
	 reply.Err = "NotLeader"
	 //Debug(dServer, "%d replies with notLeader",kv.me)
	 return
        }
	// is possible for applier to see no chan in map?
	ch := make(chan Result)
	kv.applyChans[args.ClientId] = ch
	select {
	case <-kv.applyChans[args.ClientId]:
	  reply.Err = ""
	}
	Debug(dServer, "%d reply.Err = %v", kv.me, reply.Err)

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.

	labgob.Register(Op{})
	labgob.Register(Result{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.applyChans = make(map[int] chan Result)
	kv.kvMap = make(map[string] string)
	kv.lastRequest = make(map[int] Result)

	go kv.applier()
	return kv
}

func (kv *KVServer) applier () {
  for !kv.killed() {
    select {
    case msg := <-kv.applyCh:
      valid := msg.CommandValid
      command := msg.Command.(Op)
      if !valid {
	Debug(dServer, "%d apply command not valid", kv.me)
      }
      kv.mu.Lock()
      if kv.lastRequest[command.ClientId].CommandId == command.CommandId {
	Debug(dServer, "%d apply command has already been applied", kv.me)
      } else {
	res := Result{}
	res.CommandId = command.CommandId
	switch command.Type {
	case "Get":
	  Debug(dServer, "%d Get with Key %s is %s", kv.me, command.Key, kv.kvMap[command.Key])
	  res.Value = kv.kvMap[command.Key]
	  kv.lastRequest[command.ClientId] = res
	case "Put":
	  Debug(dServer, "%d applying Put with key %s value %s to stateMachine", kv.me, command.Key, command.Value)
	  //Debug(dServer, "%d Before put is %v", kv.me, kv.kvMap[command.Key])
	  res.Value = kv.kvMap[command.Key]
	  kv.kvMap[command.Key] = command.Value
	  Debug(dServer, "%d after is %s", kv.me, kv.kvMap[command.Key])
	  res.Value = ""
	case "Append":
	  Debug(dServer, "%d applying Append with Key %s value %s to stateMachine",kv.me, command.Key, command.Value)
	  Debug(dServer, "%d before append is %s", kv.me, kv.kvMap[command.Key])
	  var sb strings.Builder
	  sb.WriteString(kv.kvMap[command.Key])
	  sb.WriteString(command.Value)
	  kv.kvMap[command.Key] = sb.String()
	  Debug(dServer, "%d after is %s", kv.me, kv.kvMap[command.Key])
	  res.Value = ""
	}
	if command.LeaderId == kv.me {
	  kv.applyChans[command.ClientId] <- res
	}
      }
      kv.mu.Unlock()
    }
  }
}


