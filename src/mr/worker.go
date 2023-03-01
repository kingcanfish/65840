package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	askArgs := &AskArgs{}

	for {
		time.Sleep(500 * time.Millisecond)
		reply := &AskReply{}
		ok := call("Coordinator.Ask", askArgs, reply)
		if !ok {
			return
		}
		switch reply.WorkType {
		case WorkType_Done:
			return
		case WorkType_Map:
			doMap(reply.TaskNumber, reply.ReduceNum, reply.FileName, mapf)
		case WorkType_Reduce:
			doReduce(reply.TaskNumber, reply.MapNum, reducef)
		case WorktType_Wait:
			time.Sleep(1 * time.Second)
			continue
		}
		noticeArgs := &DoneNoticeArgs{WorkType: reply.WorkType, TaskNum: reply.TaskNumber}
		ok = call("Coordinator.DoneNotice",
			noticeArgs, &DoneNoticeReply{})
		if !ok {
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMap(taskNumber int, nReduce int, fileName string, mapFunc func(string, string) []KeyValue) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapFunc(fileName, string(content))
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		intermediateName := fmt.Sprintf("mr-%v-%v", taskNumber, i)
		mediateFile, err := os.CreateTemp(".", intermediateName)
		if err != nil {
			log.Fatalf("[CreateTemp] err: %v\n", err)
		}
		defer mediateFile.Close()
		defer func(oldpath, newpath string) {
			err := os.Rename(oldpath, newpath)
			if err != nil {
				log.Fatal("rename err:%+v", err)
			}
		}(mediateFile.Name(), intermediateName)
		enc := json.NewEncoder(mediateFile)
		encoders[i] = enc
	}

	for i, kv := range kva {
		key := kv.Key
		n := ihash(key) % nReduce
		err := encoders[n].Encode(kva[i])
		if err != nil {
			log.Fatalf("Encode failed,err:%+v,kv: %+v", err, kv)
		}
	}

	return
}

func doReduce(taskNumber int, nMap int, reduceFunc func(string, []string) string) {
	var kva []KeyValue
	for i := 0; i < nMap; i++ {
		mediateFileName := fmt.Sprintf("mr-%v-%v", i, taskNumber)
		f, err := os.Open(mediateFileName)
		if err != nil {
			log.Fatalf("do reduce cannot open %v", mediateFileName)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	outFileName := fmt.Sprintf("mr-out-%d", taskNumber)
	mediateFile, _ := os.CreateTemp(".", outFileName)
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
		output := reduceFunc(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(mediateFile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	mediateFile.Close()
	os.Rename(mediateFile.Name(), outFileName)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
