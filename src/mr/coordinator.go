package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type MapTask struct {
	Status            int
	InputFile         string
	InterMeDiateFiles []string
}

type ReduceTask struct {
	Status            int
	InterMeDiateFiles []string
	OutputFile        string
}

type Coordinator struct {
	// Your definitions here.
	InputFiles  []string
	MapTasks    []MapTask
	ReduceTasks []ReduceTask
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	switch args.Type {
	case ClaimTask:

	default:
		log.Fatal("unkown RequestType")
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		InputFiles: files,
	}

	// Your code here.

	c.server()
	return &c
}
