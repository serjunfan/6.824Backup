package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Master struct {
	// Your definitions here.
	Files     []string
	MapStatus []int
	//ReduceStatus []int
	NReduce int
	Lock    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Request(args *RequestArgs, reply *RequestReply) error {
	m.Lock.Lock()
	index := 0
	for i := 0; i < len(m.Files); i++ {
		if m.MapStatus[i] == 0 {
			index = i
			break
		}
	}
	if index == len(m.Files) {
		reply.abort = true
		m.Lock.Lock()
		return nil
	}
	reply.FileName = m.Files[index]
	reply.NReduce = m.NReduce
	reply.MapIndex = index
	m.MapStatus[index] = 1
	m.Lock.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.Files = files
	m.MapStatus = make([]int, len(files))
	//m.ReduceStatus = make([]int, nReduce)
	m.NReduce = nReduce
	m.server()
	return &m
}
