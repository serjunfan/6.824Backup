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
	MapIndex  int
	NReduce   int
	Lock      sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Request(args *RequestArgs, reply *RequestReply) error {
	m.Lock.Lock()
	reply.FileName = m.Files[m.MapIndex]
	reply.NReduce = m.NReduce
	reply.MapIndex = m.MapIndex
	m.MapStatus[m.MapIndex] = 1
	m.MapIndex++
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
	m.MapIndex = 0
	m.MapStatus = make([]int, len(files))
	m.NReduce = nReduce
	m.server()
	return &m
}
