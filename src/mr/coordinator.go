package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	Todo TaskState = iota
	Doing
	Done
)

type MapTask struct {
	filename string
	state    TaskState
	epoch    int64
}

type ReduceTask struct {
	state TaskState
	epoch int64
}

type Coordinator struct {
	mu             sync.Mutex
	stage          TaskStage
	finishedMap    int
	files          []MapTask
	nReduce        []ReduceTask
	finishedReduce int
}

func (c *Coordinator) GetTask(args *MrArgs, reply *MrReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stage == MapStage {
		reply.Stage = MapStage
		for i, v := range c.files {
			if v.state == Todo || v.state == Doing && (time.Now().Unix()-v.epoch >= 10) {
				reply.MapFile = v.filename
				reply.MapFileIndex = i
				reply.NReduce = len(c.nReduce)

				c.files[i].state = Doing
				c.files[i].epoch = time.Now().Unix()

				return nil
			}
		}
		reply.Stage = WaitStage
		return nil
	}

	if c.stage == ReduceStage {
		reply.Stage = ReduceStage
		reply.MapNum = len(c.files)
		for i, v := range c.nReduce {
			if v.state == Todo || v.state == Doing && (time.Now().Unix()-v.epoch >= 10) {
				reply.ReduceNum = i
				reply.NReduce = len(c.nReduce)

				c.nReduce[i].state = Doing
				c.nReduce[i].epoch = time.Now().Unix()

				return nil
			}
		}
		reply.Stage = WaitStage
		return nil
	}

	return nil
}

func (c *Coordinator) FinishTask(args *MrArgs, reply *MrReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Task == MapStage {
		c.files[args.MapFileIndex].state = Done
		c.finishedMap++

		if c.finishedMap == len(c.files) {
			c.stage = ReduceStage
		}

		return nil
	}

	if args.Task == ReduceStage {
		c.nReduce[args.ReduceFileIndex].state = Done
		c.finishedReduce++

		if c.finishedReduce == len(c.nReduce) {
			c.stage = FinishStage
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stage == FinishStage
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	for _, v := range files {
		c.files = append(c.files, MapTask{v, Todo, 0})
	}
	c.finishedMap = 0
	c.nReduce = make([]ReduceTask, nReduce)
	c.stage = MapStage

	c.server()
	return &c
}
