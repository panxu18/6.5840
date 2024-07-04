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
	iMap []*Task
	pMap map[int]*Task
	fMap []*Task
	// iReduce []int
	// pReduce []int
	// fReduce []int
	nMap    int
	nReduce int
	mutex   sync.Mutex
	mDone   bool
	rDone   bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	switch args.Type {
	case ClaimTask:
		var task Task
		c.fetchTask(&task)
		reply.Code = 0
		reply.Task = task
	case SubmitTask:
		err := c.doneTask(args.Task)
		if err != nil {
			return err
		}
		reply.Code = 0
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
	defer c.mutex.Unlock()

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
	defer c.mutex.Unlock()

	c.fMap[oTask.TagetId] = oTask.Seq
	delete(c.pendingMap, oTask.TagetId)
	return nil
}

func (c *Coordinator) getNextTaskSeq() int {
	// sync
	c.nextTaskSeq++
	return c.nextTaskSeq
}

func (c *Coordinator) fetchTask() *Task {
	task := c.fetchMapTask()
	if task != nil {
		return task
	}
	c.createReduceTask(task)
}

func (c *Coordinator) fetchMapTask() *Task {
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.mDone && c.rDone {
		return true
	}
	if !c.mDone && len(c.fMap) == c.nMap {
		c.mDone = true
		return false
	}
	if c.mDone && !c.rDone && len(c.fMap) == c.nReduce {
		c.rDone = true
		return true
	}
	return false
}

func (c *Coordinator) pendingTask() *Task {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	task := c.iMap[0]
	c.pMap[task.Seq] = task
	c.iMap = c.iMap[1:]
	return task
}

func (c *Coordinator) finishTask(seq int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	task := c.pMap[seq]
	delete(c.pMap, seq)
	c.fMap = append(c.fMap, task)
}

func (c *Coordinator) resetTask(seq int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	task := c.pMap[seq]
	delete(c.pMap, seq)
	c.iMap = append(c.iMap, task)
}

func (c *Coordinator) initTask(files []string, nReduce int) {
	seq := 0
	// init map
	for _, file := range files {
		task := &Task{
			Status:    INIT,
			Type:      Map,
			Seq:       seq,
			InputFile: file,
		}
		c.iMap = append(c.iMap, task)
		seq = seq + 1
	}
	c.nMap = len(files)

	// init reduce
	// for i := 0; i < nReduce; i++ {
	// 	task := Task{
	// 		Status: INIT,
	// 		Type: Reduce,
	// 		Seq: seq,
	// 	}
	// 	c.tasks[seq] = &task
	// 	c.iReduce = append(c.iReduce, seq)
	// 	seq = seq + 1
	// }
	c.nReduce = nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.initTask(files, nReduce)
	c.server()
	return &c
}
