package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"
import "encoding/json"
import "strings"
import "strconv"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type MapStatus struct {
	MapIndex int
	FileName string
	NReduce  int
	Success bool
}
type ReduceStatus struct {
	NReduce int
	CanReduce bool
	Index int
	N int
	Success bool
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()
	finished := false
	for ;!finished; {
	  s := CallRequest()
	  if s.NReduce > 0 { // = nowork
	  fmt.Printf("RequestReply.FileName %v, Index %v, NReduce %v\n", s.FileName, s.MapIndex, s.NReduce)
	    MapFunction(s, mapf)
	    CallReport(s)
	  }
	  rs := CallReduceRequest()
	  //fmt.Println("rs.CanReduce = ", rs.CanReduce)
	  if rs.CanReduce {
	    ReduceFunction(rs, reducef)
	    CallReduceReport(rs)
	  }
	  time.Sleep(5*time.Second)
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallRequest() *MapStatus{
	args := RequestArgs{}
	reply := RequestReply{}
	s := MapStatus{}
	call("Master.Request", &args, &reply)

	s.Success = reply.NoWork
	if s.Success {
	  return &s
	}
	s.FileName = reply.FileName
	s.MapIndex = reply.MapIndex
	s.NReduce = reply.NReduce
	return &s
}

func CallReduceRequest() *ReduceStatus {
	args := ReduceRequestArgs{}
	reply := ReduceRequestReply{}
	rs := ReduceStatus{}
	call("Master.ReduceRequest", &args, &reply)

	rs.Index = reply.Index
	rs.CanReduce = reply.CanReduce
	rs.NReduce = reply.NReduce
	rs.N = reply.N
	return &rs
}

func CallReport(s *MapStatus) {
  args := ReportArgs{}
  reply := ReportReply{}
  args.Index = s.MapIndex
  args.Success = s.Success
  call("Master.Report", &args, &reply)
}

func MapFunction(s *MapStatus, mapf func(string, string) []KeyValue) {
	intermediate := make([][]KeyValue, s.NReduce)
	flag := true
	file, err := os.Open(s.FileName)
	if err != nil {
		flag = false
		log.Fatalf("MapWorkerIndex %v cannot open %v\n", s.MapIndex, s.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		flag = false
		log.Fatalf("MapWorkerIndex %v cannot read %v\n", s.MapIndex, file)
	}
	file.Close()
	kva := mapf(s.FileName, string(content))
	for _, kv := range kva {
		intermediate[ihash(kv.Key)%s.NReduce] = append(intermediate[ihash(kv.Key)%s.NReduce], kv)
	}
	for i := 0; i < s.NReduce; i++ {
		//sort.Sort(ByKey(intermediate[i]))
		var sb strings.Builder
		sb.WriteString("mr-")
		sb.WriteString(strconv.Itoa(s.MapIndex))
		sb.WriteString("-")
		sb.WriteString(strconv.Itoa(i))
		oname := sb.String()
		fmt.Println("test oname = ", oname)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Encoding to intermediate file went wrong with %v %v", kv.Key, kv.Value)
			}
		}
	}
	s.Success = flag
}

func ReduceFunction(rs *ReduceStatus, redf func (string, []string)string) {
  tmpfile, err := ioutil.TempFile(".","tmp")
  flag := true
  if err != nil {
    log.Fatal(err)
    flag = false
  }
  defer os.Remove(tmpfile.Name()) //clean up
  kva := []KeyValue{}
  for i := 0; i < rs.N; i++ {
    var sb strings.Builder
    sb.WriteString("mr-")
    sb.WriteString(strconv.Itoa(i))
    sb.WriteString("-")
    sb.WriteString(strconv.Itoa(rs.Index))
    iname := sb.String()
    f, err := os.Open(iname)
    if err != nil {
      log.Fatal(err)
      flag = false
    }
    dec := json.NewDecoder(f)
    for {
      var kv KeyValue
      if err := dec.Decode(&kv); err != nil {
	break
      }
      kva = append(kva, kv)
    }
    f.Close()
  }
  sort.Sort(ByKey(kva))

  i := 0
  for i < len(kva) {
    j := i + 1
    for j < len(kva) && kva[j].Key == kva[i].Key {
      j++
    }
    values := []string{}
    for k := i; k < j; k++ {
      values = append(values, kva[k].Value)
    }
    output := redf(kva[i].Key, values)
    fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
    i = j
  }

  var sb strings.Builder
  sb.WriteString("mr-out-")
  sb.WriteString(strconv.Itoa(rs.Index))
  oname := sb.String()
  e := os.Rename(tmpfile.Name(), oname)
  //defer tmpfile.Close()
  if e != nil {
    log.Fatal(e)
    flag = false
  }
  fmt.Println("reduce output = ", oname)
  rs.Success = flag
}

func CallReduceReport(rs *ReduceStatus) {
  args := ReportArgs{}
  reply := ReportReply{}
  args.Index = rs.Index
  args.Success = rs.Success
  call("Master.ReduceReport", &args, &reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
