package mr

import (
	"os"
	"strconv"
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	FinishTask
)

type MrArgs struct {
	// for Coordinator.Finish
	Task         TaskType
	MapFileIndex int
}

type MrReply struct {
	Task         TaskType
	MapFile      string
	MapFileIndex int
	ReduceNum    int
	NReduce      int
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
