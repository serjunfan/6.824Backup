package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"

type Master struct {
	// Your definitions here.
	Files     []string
	N int
	//0 = not assigned, 1 = assigned, 2 = completed
	MapStatus []int
	MapDone bool
	ReduceStatus []int
	ReduceDone bool
	NReduce int
	Lock    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Request(args *RequestArgs, reply *RequestReply) error {
	m.Lock.Lock()
	index := -1
	for i := 0; i < m.N; i++ {
		if m.MapStatus[i] == 0 {
			index = i
			break
		}
	}
	if index >= m.N || index < 0 {
		reply.NoWork = true
		m.Lock.Unlock()
		return nil
	}
	reply.NoWork = false
	reply.FileName = m.Files[index]
	reply.NReduce = m.NReduce
	reply.MapIndex = index
	m.MapStatus[index] = 1
	m.Lock.Unlock()
	return nil
}

func (m *Master) Report(args *ReportArgs, reply *ReportReply) error {
  m.Lock.Lock()
  success := args.Success
  index := args.Index
  if index < 0 || index >= m.N {
    m.Lock.Unlock()
    log.Fatalf("Map worker returned a invalid index, should terminate")
  }
  if success {
    m.MapStatus[index] = 2
  } else {
    m.MapStatus[index] = 0
  }
  done := true
  for i := 0; i < m.N; i++ {
    if m.MapStatus[i] != 2 {
      done = false
      break
    }
  }
  m.MapDone = done
  fmt.Println("mapdone = ", done)
  m.Lock.Unlock()
  return nil
}

func (m *Master) ReduceRequest(args *ReduceRequestArgs, reply *ReduceRequestReply) error {
  m.Lock.Lock()
  reply.CanReduce = true
  if !m.MapDone {
    reply.CanReduce = false
    m.Lock.Unlock()
    return nil
  }
  index := -1
  for i := 0; i < m.NReduce; i++ {
    if m.ReduceStatus[i] == 0 {
      index = i
      break
    }
  }
  if index == -1 {
    reply.CanReduce = false
    m.Lock.Unlock()
    return nil
  }
  m.ReduceStatus[index] = 1
  reply.N = m.N
  reply.NReduce = m.NReduce
  reply.Index = index
  m.Lock.Unlock()
  return nil
}

func (m *Master) ReduceReport(args *ReduceReportArgs, reply *ReduceReportReply) error {
  m.Lock.Lock()
  success := args.Success
  index := args.Index
  if index < 0 || index >= m.NReduce {
    m.Lock.Unlock()
    log.Fatalf("reduce worker returned a invalid index, should terminate")
  }
  if success {
    m.ReduceStatus[index] = 2
  } else {
    m.ReduceStatus[index] = 0
  }
  done := true
  for i := 0; i < m.N; i++ {
    if m.ReduceStatus[i] != 2 {
      done = false
      break
    }
  }
  m.ReduceDone = done
  fmt.Println("Reducedone = ", done)
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
	//ret := false

	// Your code here.

	return m.ReduceDone
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
	m.N = len(files)
	m.MapStatus = make([]int, m.N)
	m.ReduceStatus = make([]int, nReduce)
	m.NReduce = nReduce
	m.server()
	return &m
}
