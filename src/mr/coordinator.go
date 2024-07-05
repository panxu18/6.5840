package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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
	None    TaskStatus = 0
	INIT    TaskStatus = 1
	Pending TaskStatus = 2
	DONE    TaskStatus = 3
)

type Task struct {
	Status            TaskStatus
	Type              TaskType
	Id                int
	Seq               int
	NReduce           int
	StartTime         int64
	EndTime           int64
	InputFile         string
	InterMeDiateFiles []string
	OutputFile        string
}

type Coordinator struct {
	seq  int
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
	start   bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {

	c.mutex.Lock()
	if !c.start {
		reply.Code = 0
		return errors.New("not start")
	}
	c.mutex.Unlock()

	switch args.Type {
	case ClaimTask:
		task := c.fetchTask()
		if task == nil {
			reply.Task = Task{
				Status: None,
			}
		} else {
			reply.Task = *task
		}
		reply.Code = 0
	case SubmitTask:
		task := args.Task
		c.submitTask(&task)
		reply.Code = 0
	default:
		log.Fatal("unkown RequestType")
		return errors.New("unkown RequestType")
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// fmt.Printf("state mDone: %v rDone: %v i: %v p: %v f: %v\n", c.mDone, c.rDone, len(c.iMap), len(c.pMap), len(c.fMap))
	if c.mDone && c.rDone {
		return true
	}

	c.resetTimeoutTask()

	if !c.mDone && len(c.fMap) == c.nMap {
		c.mDone = true
		c.initReduce()
		// fmt.Printf("state mDone: %v rDone: %v i: %v p: %v f: %v\n", c.mDone, c.rDone, len(c.iMap), len(c.pMap), len(c.fMap))
		return false
	}
	if c.mDone && !c.rDone && len(c.fMap) == c.nReduce {
		c.rDone = true
		return true
	}
	return false
}

func (c *Coordinator) resetTimeoutTask() {
	now := time.Now().Unix()
	var dSeq []int = make([]int, 0)
	for _, task := range c.pMap {
		// fmt.Printf("task timeout type: %v id: %v seq: %v startTime: %d duration: %d\n", task.Type, task.Id, task.Seq, task.StartTime, now-task.StartTime)
		if task.StartTime+10 < now {
			// fmt.Printf("task timeout type: %v id: %v seq: %v \n", task.Type, task.Id, task.Seq)
			dSeq = append(dSeq, task.Seq)
		}
	}
	for _, seq := range dSeq {
		task := c.pMap[seq]
		delete(c.pMap, seq)
		task.Status = INIT
		task.StartTime = now
		c.seq++
		task.Seq = c.seq
		c.iMap = append(c.iMap, task)
	}
}

func (c *Coordinator) fetchTask() *Task {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if len(c.iMap) <= 0 {
		return nil
	}
	task := c.iMap[0]
	c.pMap[task.Seq] = task
	task.Status = Pending
	task.StartTime = time.Now().Unix()
	c.iMap = c.iMap[1:]
	return task
}

func (c *Coordinator) submitTask(task *Task) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	_, ok := c.pMap[task.Seq]
	if !ok {
		return
	}
	delete(c.pMap, task.Seq)
	task.Status = DONE
	task.EndTime = time.Now().Unix()
	c.fMap = append(c.fMap, task)
}

func (c *Coordinator) initReduce() {
	var intermeiate = make([][]string, c.nReduce)
	for _, task := range c.fMap {
		for _, file := range task.InterMeDiateFiles {
			sa := strings.Split(file, "-")
			i, _ := strconv.Atoi(sa[len(sa)-1])
			intermeiate[i] = append(intermeiate[i], file)
		}
	}
	c.fMap = make([]*Task, 0)
	for i := 0; i < c.nReduce; i++ {
		task := &Task{
			Status:            INIT,
			Type:              Reduce,
			Id:                i,
			Seq:               c.seq,
			InterMeDiateFiles: intermeiate[i],
		}
		c.seq++
		c.iMap = append(c.iMap, task)
	}
}

func (c *Coordinator) initMap(files []string) {
	// init map
	for i, file := range files {
		task := &Task{
			Status:    INIT,
			Type:      Map,
			Id:        i,
			Seq:       c.seq,
			InputFile: file,
			NReduce:   c.nReduce,
		}
		c.iMap = append(c.iMap, task)
		c.seq++
	}
	c.nMap = len(files)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		iMap:    make([]*Task, 0),
		pMap:    make(map[int]*Task),
		fMap:    make([]*Task, 0),
		mutex:   sync.Mutex{},
		nReduce: nReduce,
	}

	c.mutex.Lock()
	c.initMap(files)
	c.start = true
	c.mutex.Unlock()

	c.server()
	return &c
}
