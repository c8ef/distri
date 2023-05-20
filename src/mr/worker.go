package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

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

	for {
		time.Sleep(time.Second)
		args := MrArgs{}
		reply := MrReply{}

		ok := call("Coordinator.GetTask", &args, &reply)

		if !ok || reply.Stage == WaitStage {
			continue
		}

		if reply.Stage == MapStage {
			file, err := os.Open(reply.MapFile)
			if err != nil {
				log.Fatalf("cannot open %v", reply.MapFile)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.MapFile)
			}
			file.Close()

			kva := mapf(reply.MapFile, string(content))
			intermediate := make([][]KeyValue, reply.NReduce)

			for _, kv := range kva {
				intermediate[ihash(kv.Key)%reply.NReduce] = append(intermediate[ihash(kv.Key)%reply.NReduce], kv)
			}

			for i := 0; i < reply.NReduce; i++ {
				file, err := os.Create(fmt.Sprintf("mr-%v-%v", reply.MapFileIndex, i))
				if err != nil {
					log.Fatalf("cannot create file")
				}
				enc := json.NewEncoder(file)
				for _, kv := range intermediate[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode json")
					}
				}
				file.Close()
			}

			ok := call("Coordinator.FinishTask", &args, &reply)

			if !ok {
				continue
			}
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
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
