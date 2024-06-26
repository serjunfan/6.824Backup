package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"
//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type HeartBeatArgs struct {
  Index int
  State int
  Time time.Time
}
type HeartBeatReply struct {
  Abort bool
}
type RequestArgs struct {
}

type RequestReply struct {
	FileName string
	NoWork    bool
	MapIndex int
	NReduce  int
}

type ReportArgs struct {
	Index int
	Success bool
}
type ReportReply struct {
}

type ReduceRequestArgs struct {
}

type ReduceRequestReply struct {
  NReduce int
  Index int
  CanReduce bool
  N int
}
type ReduceReportArgs struct {
  Index int
  Success bool
}
type ReduceReportReply struct {
}
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
