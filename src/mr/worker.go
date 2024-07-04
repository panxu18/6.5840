package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
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
		// claim map task
		task := claimTask()
		if task == nil {
			time.Sleep(time.Second)
		}

		// do task
		switch task.Type {
		case Map:
			doMap(task, mapf)
			break
		case Reduce:
			doReduce(task, reducef)
			break
		default:
			continue
		}

		if task.Status == DONE {
			submitTask(task)
		}
	}

}

func doMap(task *Task, mapf func(string, string) []KeyValue) {
	filename := task.InputFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := []KeyValue{}
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	nIntermediate := make([][]KeyValue, task.NReduce)

	for _, kv := range intermediate {
		i := ihash(kv.Key) % task.NReduce
		nIntermediate[i] = append(nIntermediate[i], kv)
	}

	var intermediateFiles []string
	for i := 0; i < task.NReduce; i++ {
		if len(nIntermediate[i]) <= 0 {
			continue
		}
		oname := "mr-" + strconv.Itoa(task.Seq) + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range nIntermediate[i] {
			enc.Encode(kv)
		}
		intermediateFiles = append(intermediateFiles, oname)
		ofile.Close()
	}

	task.InterMeDiateFiles = intermediateFiles
	task.Status = DONE
}

func doReduce(task *Task, reducef func(string, []string) string) {
	var kva []KeyValue
	for _, filename := range task.InterMeDiateFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))

	ofile, _ := os.CreateTemp("", "*")

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	oname := "mr-out-" + strconv.Itoa(task.TagetId)
	os.Rename(ofile.Name(), oname)
	ofile.Close()
	task.OutputFile = oname
	task.Status = DONE
}

func claimTask() *Task {
	args := ExampleArgs{
		Type: ClaimTask,
	}
	reply := ExampleReply{}
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		task := &reply.Task
		return task
	} else {
		return nil
	}
}

func submitTask(task *Task) {
	args := ExampleArgs{
		Type: SubmitTask,
		Task: *task,
	}
	reply := ExampleReply{}
	call("Coordinator.Example", &args, &reply)
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
