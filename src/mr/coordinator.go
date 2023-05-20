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
	maps           []MapTask
	finishedMap    int
	reduces        []ReduceTask
	finishedReduce int
}

func (c *Coordinator) GetTask(args *MrArgs, reply *MrReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stage == MapStage {
		reply.Stage = MapStage
		for i, v := range c.maps {
			if v.state == Todo || v.state == Doing && (time.Now().Unix()-v.epoch >= 10) {
				reply.MapFile = v.filename
				reply.TaskIndex = i
				reply.NReduce = len(c.reduces)

				c.maps[i].state = Doing
				c.maps[i].epoch = time.Now().Unix()

				return nil
			}
		}
		reply.Stage = WaitStage
		return nil
	}

	if c.stage == ReduceStage {
		reply.Stage = ReduceStage
		reply.NMap = len(c.maps)
		for i, v := range c.reduces {
			if v.state == Todo || v.state == Doing && (time.Now().Unix()-v.epoch >= 10) {
				reply.TaskIndex = i
				reply.NReduce = len(c.reduces)

				c.reduces[i].state = Doing
				c.reduces[i].epoch = time.Now().Unix()

				return nil
			}
		}
		reply.Stage = WaitStage
		return nil
	}
	reply.Stage = ExitStage
	return nil
}

func (c *Coordinator) FinishTask(args *MrArgs, reply *MrReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Stage == MapStage {
		c.maps[args.Index].state = Done
		c.finishedMap++

		if c.finishedMap == len(c.maps) {
			c.stage = ReduceStage
		}
		return nil
	}

	if args.Stage == ReduceStage {
		c.reduces[args.Index].state = Done
		c.finishedReduce++

		if c.finishedReduce == len(c.reduces) {
			c.stage = FinishStage
		}
		return nil
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
	c.stage = MapStage
	for _, v := range files {
		c.maps = append(c.maps, MapTask{v, Todo, 0})
	}
	c.finishedMap = 0
	c.reduces = make([]ReduceTask, nReduce)
	c.finishedReduce = 0

	c.server()
	return &c
}
