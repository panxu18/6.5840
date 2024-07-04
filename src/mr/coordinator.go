package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskType int

const (
	Map    TaskType = 1
	Reduce TaskType = 2
)

type TaskStatus int

const (
	INIT       TaskStatus = 1
	INPROGRESS TaskStatus = 2
	DONE       TaskStatus = 3
)

type Task struct {
	Status            TaskStatus
	Type              TaskType
	TagetId           int
	Seq               int
	NReduce           int
	StartTime         int
	EndTime           int
	InputFile         string
	InterMeDiateFiles []string
	OutputFile        string
}

type Coordinator struct {
	InputFiles    []string
	nextTaskSeq   int
	nMap          int
	nReduce       int
	pendingMap    map[int]int
	pendingReduce map[int]int
	fMap          map[int]int
	fReduce       map[int]int
	tasks         map[int]Task
	mutex         sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	switch args.Type {
	case ClaimTask:
		var task Task
		c.claimTask(&task)
		reply.Code = 0
		reply.Task = task
		break
	case SubmitTask:
		err := c.doneTask(args.Task)
		if err != nil {
			return err
		}
		reply.Code = 0
		break
	default:
		log.Fatal("unkown RequestType")
		return errors.New("unkown RequestType")
	}
	return nil
}

func (c *Coordinator) doneTask(task Task) error {
	switch task.Type {
	case Map:
		return c.submitMapTask(task)
	case Reduce:
		return c.submitReduceTask(task)
	default:
		return errors.New("unkown task type")
	}
}

func (c *Coordinator) submitMapTask(task Task) error {
	oTask, ok := c.tasks[task.Seq]
	if !ok {
		log.Fatal("unkown task")
		return errors.New("unkown map task")
	}
	if task.Seq != oTask.Seq {
		log.Fatal("unkown task")
		return errors.New("unkown map task")
	}
	oTask.Status = DONE
	oTask.EndTime = time.Now().Second()
	oTask.InterMeDiateFiles = task.InterMeDiateFiles

	c.mutex.Lock()
	c.mutex.Unlock()

	c.fMap[oTask.TagetId] = oTask.Seq
	delete(c.pendingMap, oTask.TagetId)

	return nil
}

func (c *Coordinator) submitReduceTask(task Task) error {
	oTask, ok := c.tasks[task.Seq]
	if !ok {
		log.Fatal("unkown task")
		return errors.New("unkown map task")
	}
	if task.Seq != oTask.Seq {
		log.Fatal("unkown task")
		return errors.New("unkown map task")
	}
	oTask.Status = DONE
	oTask.EndTime = time.Now().Second()
	oTask.OutputFile = task.OutputFile

	c.mutex.Lock()
	c.mutex.Unlock()
	c.fMap[oTask.TagetId] = oTask.Seq
	delete(c.pendingMap, oTask.TagetId)
	return nil
}

func (c *Coordinator) getNextTaskSeq() int {
	// sync
	c.nextTaskSeq++
	return c.nextTaskSeq
}

func (c *Coordinator) claimTask(task *Task) {
	c.createMapTask(task)
	if task != nil {
		return
	}
	c.createReduceTask(task)
}

func (c *Coordinator) createMapTask(task *Task) {
	c.mutex.Lock()
	i := 0
	var file string
	iSeq := c.getNextTaskSeq()
	for ; i < len(c.InputFiles); i++ {
		_, pending := c.pendingMap[i]
		if !pending {
			c.pendingMap[i] = iSeq
			break
		}
		_, done := c.fMap[i]
		if !done {
			c.pendingMap[i] = iSeq
			break
		}
	}
	c.mutex.Unlock()
	if i >= len(c.InputFiles) {
		return
	}
	task.Status = INPROGRESS
	task.Type = Map
	task.TagetId = i
	task.Seq = iSeq
	task.NReduce = c.nReduce
	task.InputFile = file
	task.StartTime = time.Now().Second()
	c.tasks[i] = *task
}

func (c *Coordinator) createReduceTask(task *Task) {
	c.mutex.Lock()
	i := 0
	iSeq := c.getNextTaskSeq()
	for ; i < c.nReduce; i++ {
		_, pending := c.pendingReduce[i]
		if !pending {
			c.pendingReduce[i] = iSeq
			break
		}
		_, done := c.fReduce[i]
		if !done {
			c.pendingReduce[i] = iSeq
			break
		}
	}
	c.mutex.Unlock()
	if i >= c.nReduce {
		return
	}

	// create reduce task
	task.Status = INPROGRESS
	task.Type = Reduce
	task.TagetId = i
	task.Seq = c.getNextTaskSeq()
	task.NReduce = c.nReduce
	task.StartTime = time.Now().Second()
	task.InterMeDiateFiles = make([]string, 0)
	for _, seq := range c.fMap {
		mTask := c.tasks[seq]
		for _, file := range mTask.InterMeDiateFiles {
			task.InterMeDiateFiles = append(task.InterMeDiateFiles, file)
		}
	}

	c.tasks[iSeq] = *task
}

func (c *Coordinator) mapAllDone() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.nMap == len(c.fMap)
}

func (c *Coordinator) reduceAllDone() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.nReduce == len(c.fReduce)
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
	return c.reduceAllDone()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		InputFiles:    files,
		nextTaskSeq:   0,
		nReduce:       nReduce,
		nMap:          len(files),
		pendingMap:    make(map[int]int),
		pendingReduce: make(map[int]int),
		fMap:          make(map[int]int),
		fReduce:       make(map[int]int),
	}
	c.server()
	return &c
}
