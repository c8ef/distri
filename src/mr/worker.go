package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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
		if reply.Stage == ExitStage {
			return
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

			args := MrArgs{}
			args.Stage = MapStage
			args.MapFileIndex = reply.MapFileIndex
			ok := call("Coordinator.FinishTask", &args, &reply)

			if !ok {
				continue
			}
		}
		if reply.Stage == ReduceStage {
			intermediate := []KeyValue{}
			for i := 0; i < reply.MapNum; i++ {
				file, err := os.Open(fmt.Sprintf("mr-%d-%d", i, reply.ReduceNum))
				if err != nil {
					log.Fatalf("cannot open %v", fmt.Sprintf("mr-%d-%d", i, reply.ReduceNum))
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", reply.ReduceNum)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			ofile.Close()

			args := MrArgs{}
			args.Stage = ReduceStage
			args.ReduceFileIndex = reply.ReduceNum
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
