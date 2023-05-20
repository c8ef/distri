package mr

import (
	"os"
	"strconv"
)

type TaskStage int

const (
	MapStage TaskStage = iota
	ReduceStage
	FinishStage
	WaitStage
)

type MrArgs struct {
	// for Coordinator.FinishTask
	Task            TaskStage
	MapFileIndex    int
	ReduceFileIndex int
}

type MrReply struct {
	Stage        TaskStage
	MapFile      string
	MapFileIndex int
	ReduceNum    int
	NReduce      int
	MapNum       int
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
