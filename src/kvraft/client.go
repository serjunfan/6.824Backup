package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "time"
//import "debug"
//import "sync"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	clientId int
	commandId int
	//mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	// how to obtain unique clientId?
	ck.commandId = 0
	ck.clientId = int(nrand())
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.commandId++
	for {
	  //ck.mu.Lock()
	  args := GetArgs{}
	  reply := GetReply{}
	  args.Key = key
	  args.ClientId = ck.clientId
	  args.CommandId = ck.commandId
	  //ck.mu.Unlock()

	  Debug(dClient, "%d sends Get request to %d with key %s", ck.clientId, ck.leaderId, key)
	  ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)

	  if ok && reply.Err == "" {
	    return reply.Value
	  }
	  if ok {
	    Debug(dClient, "%d received Err %s", ck.clientId, reply.Err)
	  }
	  ck.leaderId = int((ck.leaderId+1)%len(ck.servers))
	  time.Sleep(10 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.commandId++
	for {
	  //ck.mu.Lock()
	  args := PutAppendArgs{}
	  reply := PutAppendReply{}
	  args.Key = key
	  args.Value = value
	  args.ClientId = ck.clientId
	  args.CommandId = ck.commandId
	  args.Op = op
	  //ck.mu.Unlock()

	  Debug(dClient, "%d sends %s request to %d with key %s and value %s", ck.clientId, op, ck.leaderId, key, value)
	  ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)

	  if ok && reply.Err == "" {
	    Debug(dClient, "%d PutAppend request success", ck.clientId)
	    return
	  }
	  if ok {
	    Debug(dClient, "%d received Err %s", ck.clientId, reply.Err)
	  }
	  ck.leaderId = int((ck.leaderId+1)%len(ck.servers))
	  time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
