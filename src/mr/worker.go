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
	  s := CallRequest(mapf)
	  if s.Success {
	    CallReport(s)
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

func CallRequest(mapf func(string, string) []KeyValue) MapStatus{
	args := RequestArgs{}
	reply := RequestReply{}
	s := MapStatus{}
	call("Master.Request", &args, &reply)

	s.Success = reply.NoWork
	if s.Success {
	  fmt.Println("no map work")
	  return s
	}
	s.FileName = reply.FileName
	s.MapIndex = reply.MapIndex
	s.NReduce = reply.NReduce
	fmt.Printf("RequestReply.FileName %v, Index %v, NReduce %v\n", s.FileName, s.MapIndex, s.NReduce)
	s.Success = MapFunction(s, mapf)
	fmt.Println("worker success = ", s.Success)
	return s
}

func CallReport(s MapStatus) {
  args := ReportArgs{}
  reply := ReportReply{}
  args.Index = s.MapIndex
  args.Success = s.Success
  call("Master.Report", &args, &reply)
}

func MapFunction(s MapStatus, mapf func(string, string) []KeyValue) bool{
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
		sort.Sort(ByKey(intermediate[i]))
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
	return flag
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
