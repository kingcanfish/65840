package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type WorkType int

const (
	WorkType_Done   WorkType = 0
	WorkType_Map    WorkType = 1
	WorkType_Reduce WorkType = 2
	WorktType_Wait  WorkType = 3
)

type AskArgs struct {
}
type AskReply struct {
	WorkType   WorkType
	TaskNumber int
	ReduceNum  int
	MapNum     int
	FileName   string
}

type DoneNoticeArgs struct {
	WorkType WorkType
	TaskNum  int
}

type DoneNoticeReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
