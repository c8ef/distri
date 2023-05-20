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
	ExitStage
)

type MrArgs struct {
	// Coordinator.GetTask has no argument
	// for Coordinator.FinishTask
	Stage TaskStage
	Index int
}

type MrReply struct {
	Stage     TaskStage
	TaskIndex int
	MapFile   string
	NMap      int
	NReduce   int
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
