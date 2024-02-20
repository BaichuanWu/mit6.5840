package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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
		reply := Task{}
		if call("Coordinator.RequestTask", &Ignore{}, &reply) {
			switch reply.Typ {
			case TaskTypQuit:
				return
			case TaskTypWait:
				time.Sleep(time.Second)
			case TaskTypWork:
				if reply.Work.Typ == WorkTypMap {
					HandleMapTask(&reply, mapf)
				} else if reply.Work.Typ == WorkTypReduce {
					HandleReduceTask(&reply, reducef)
				}
			}
		}
	}
}

func HandleMapTask(task *Task, mapf func(string, string) []KeyValue) {
	intermediate := make([][]KeyValue, task.NReduce)
	for _, filename := range task.Work.Inputs {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		for _, kv := range kva {
			reduceIdx := ihash(kv.Key) % task.NReduce
			intermediate[reduceIdx] = append(intermediate[reduceIdx], kv)
		}
	}
	for reduceIdx, v := range intermediate {
		oname := fmt.Sprintf("mr-%v-%v", task.Work.Idx, reduceIdx)
		file, err := os.CreateTemp("", oname)
		if err != nil {
			log.Fatalf("cannot open tmp file")
		}
		enc := json.NewEncoder(file)
		err = enc.Encode(&v)
		if err != nil {
			log.Fatalf("map intermediate %v \n", intermediate)
			os.Exit(1)
		}
		file.Close()
		os.Rename(file.Name(), oname)
	}
	call("Coordinator.FinishTask", &task, &Ignore{})

}

func HandleReduceTask(task *Task, reducef func(string, []string) string) {
	kvm := map[string][]string{}
	for _, filename := range task.Work.Inputs {
		file, err := os.Open(filename)
		if err != nil {
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kva []KeyValue
			if err := dec.Decode(&kva); err != nil {
				break
			}
			for _, kv := range kva {
				if _, ok := kvm[kv.Key]; !ok {
					kvm[kv.Key] = []string{}
				}
				kvm[kv.Key] = append(kvm[kv.Key], kv.Value)
			}
		}
		defer os.Remove(filename)
	}
	oname := fmt.Sprintf("mr-out-%v", task.Work.Idx)
	ofile, _ := os.Create(oname)
	for k, v := range kvm {
		output := reducef(k, v)
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}
	ofile.Close()
	call("Coordinator.FinishTask", &task, &Ignore{})
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
