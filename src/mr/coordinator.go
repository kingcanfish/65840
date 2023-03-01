package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	NMap    int
	NReduce int
	Files   []string

	MapTask        []int
	ReduceTask     []int
	MapPendings    []int
	ReducePendings []int

	lock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Ask(args *AskArgs, reply *AskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(c.MapTask) != 0 {
		reply.WorkType = WorkType_Map
		reply.TaskNumber = c.MapTask[0]
		reply.FileName = c.Files[c.MapTask[0]]
		reply.ReduceNum = c.NReduce
		reply.MapNum = c.NMap

		c.MapPendings = append(c.MapPendings, c.MapTask[0])
		c.MapTask = c.MapTask[1:]
		go c.watch(reply.TaskNumber, WorkType_Map)

	} else if len(c.MapPendings) != 0 {
		reply.WorkType = WorktType_Wait

	} else if len(c.ReduceTask) != 0 {
		reply.WorkType = WorkType_Reduce
		reply.TaskNumber = c.ReduceTask[0]
		reply.ReduceNum = c.NReduce
		reply.MapNum = c.NMap

		c.ReducePendings = append(c.ReducePendings, c.ReduceTask[0])
		c.ReduceTask = c.ReduceTask[1:]

		go c.watch(reply.TaskNumber, WorkType_Reduce)
	} else if len(c.ReducePendings) != 0 {
		reply.WorkType = WorktType_Wait

	} else {
		reply.WorkType = WorkType_Done
	}
	return nil
}

func (c *Coordinator) DoneNotice(args *DoneNoticeArgs, reply *DoneNoticeReply) error {

	c.lock.Lock()
	defer c.lock.Unlock()
	switch args.WorkType {
	case WorkType_Map:
		for i, v := range c.MapPendings {
			if v == args.TaskNum {
				c.MapPendings = append(c.MapPendings[:i], c.MapPendings[i+1:]...)
			}
		}
	case WorkType_Reduce:
		for i, v := range c.ReducePendings {
			if v == args.TaskNum {
				c.ReducePendings = append(c.ReducePendings[:i], c.ReducePendings[i+1:]...)
			}
		}
	}
	return nil
}

func (c *Coordinator) watch(taskNUm int, workType WorkType) {
	time.Sleep(10 * time.Second)
	c.lock.Lock()
	defer c.lock.Unlock()
	if workType == WorkType_Map {
		for i, v := range c.MapPendings {
			if v == taskNUm {
				c.MapTask = append(c.MapTask, v)
				c.MapPendings = append(c.MapPendings[:i], c.MapPendings[i+1:]...)
			}
		}
	}
	if workType == WorkType_Reduce {
		for i, v := range c.ReducePendings {
			if v == taskNUm {
				c.ReduceTask = append(c.ReduceTask, v)
				c.ReducePendings = append(c.ReducePendings[:i], c.ReducePendings[i+1:]...)
			}
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(c.ReduceTask) == 0 && len(c.MapPendings) == 0 &&
		len(c.ReduceTask) == 0 && len(c.ReducePendings) == 0 {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NMap = len(files)
	c.NReduce = nReduce

	c.Files = files
	c.MapTask = []int{}
	c.ReduceTask = []int{}
	c.MapPendings = []int{}
	c.ReducePendings = []int{}
	c.lock = sync.Mutex{}

	for i := 0; i < len(files); i++ {
		c.MapTask = append(c.MapTask, i)
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTask = append(c.ReduceTask, i)
	}
	c.server()
	return &c
}
