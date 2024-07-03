package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type TaskType int

const (
	Map    TaskType = 1
	Reduce TaskType = 2
)

type TaskStatus int

const (
	UNCLAIME   TaskStatus = 1
	INPROGRESS TaskStatus = 2
	DONE       TaskStatus = 3
)

type Task struct {
	Status            TaskStatus
	Type              TaskType
	num               int
	InputFile         string
	InterMeDiateFiles []string
	OutputFile        string
}

type Coordinator struct {
	// Your definitions here.
	InputFiles  []string
	MapTasks    []Task
	ReduceTasks []Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	switch args.Type {
	case ClaimTask:
		task := c.claimMapTask()
		reply.Code = 0
		reply.Type = ClaimTask
		reply.MapTask = task
		break
	default:
		log.Fatal("unkown RequestType")
	}
	return nil
}

func (c *Coordinator) claimMapTask() Task {
	if !c.mapDone() {
		// return map task
		for _, task := range c.MapTasks {
			if task.Status == UNCLAIME {
				return task
			}
		}
	} else if !c.reduceDone() {
		// return reduce task

		return task
	} else {

	}

}

func (c *Coordinator) reduceDone() bool {
	var done = true
	for _, task := range c.ReduceTasks {
		if task.Status != DONE {
			done = false
		}
	}
	return done
}

func (c *Coordinator) mapDone() bool {
	var done = true
	for _, task := range c.MapTasks {
		if task.Status != DONE {
			done = false
		}
	}
	return done
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
