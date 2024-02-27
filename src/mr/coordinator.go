package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TaskNotExecuted = iota
	TaskExecuting
	TaskDone
)

const (
	WorkerWorking = iota
	WorkerWaiting
)

const (
	TaskMap = iota
	TaskReduce
)

// TaskStatus 具体单个任务的信息
type TaskStatus struct {
	TaskId    int
	FileName  string
	FileNames []string
}

type Coordinator struct {
	// Your definitions here.
	FileNames          []string
	NReduce            int
	TaskCompleteStatus []int               // 记录对应index的任务状态：未运行，运行中，完成
	Timer              map[int]*time.Timer // 对应任务的超时定时器
	TaskMapChannel     chan *TaskStatus    // 在channel中的任务都是没有被完成的
	TaskReduceChannel  chan *TaskStatus
	TaskPhase          int
	AllDone            bool
	Mutex              sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetWork(args *TaskArgs, reply *TaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	switch {
	case c.TaskPhase == TaskMap:
		select {
		case task := <-c.TaskMapChannel:

			reply.FileName = task.FileName
			reply.NReduce = c.NReduce
			reply.TaskPhase = TaskMap
			reply.TaskId = task.TaskId
			reply.TaskType = TaskExecuting
			reply.WorkerStatus = WorkerWorking
			c.TaskCompleteStatus[reply.TaskId] = TaskExecuting

			//TODO 定时器等待10s
			timer := time.NewTimer(10 * time.Second)
			//fmt.Printf("map任务: %v，开始等待\n", reply.TaskId)
			c.Timer[reply.TaskId] = timer //注意不能并发写
			go func(id int) {
				select {
				case <-timer.C:
					c.Mutex.Lock()
					defer c.Mutex.Unlock()
					//fmt.Printf("map任务: %v，已经过期\n", id)
					taskStatus := TaskStatus{FileName: c.FileNames[id], TaskId: id}
					c.TaskMapChannel <- &taskStatus
					c.Timer[id].Stop()
					delete(c.Timer, id) //从映射中删除指定键值对 //注意不能并发写
				}
			}(reply.TaskId)

		default:
			reply.WorkerStatus = WorkerWaiting
			for _, i := range c.TaskCompleteStatus {
				if i == TaskExecuting {
					// 定时器解决宕机问题
					return nil
				}
			}
			//fmt.Println("map over")
			reply.MapDone = true
			err := c.GoToReducePhase()
			if err != nil {
				fmt.Println("GoToReducePhase error")
				return nil
			}
		}

	case c.TaskPhase == TaskReduce:
		select {
		case task := <-c.TaskReduceChannel:
			reply.FileNames = task.FileNames
			reply.TaskPhase = TaskReduce
			reply.TaskId = task.TaskId
			reply.TaskType = TaskExecuting
			reply.WorkerStatus = WorkerWorking
			c.TaskCompleteStatus[reply.TaskId] = TaskExecuting

			// 定时器等待10s
			timer := time.NewTimer(10 * time.Second)
			//fmt.Printf("reduce任务: %v，开始等待\n", reply.TaskId)
			c.Timer[reply.TaskId] = timer //注意不能并发写
			go func(id int) {
				select {
				case <-timer.C:
					c.Mutex.Lock()
					defer c.Mutex.Unlock()
					//fmt.Printf("reduce任务: %v，已经过期\n", id)
					taskStatus := TaskStatus{FileNames: c.GetTempOut(id), TaskId: id}
					c.TaskReduceChannel <- &taskStatus
					c.Timer[id].Stop()
					delete(c.Timer, id) //从映射中删除指定键值对 //注意不能并发写
				}
			}(reply.TaskId)

		default:
			reply.WorkerStatus = WorkerWaiting
			for _, i := range c.TaskCompleteStatus {
				if i == TaskExecuting {
					// 定时器解决宕机问题
					return nil
				}
			}
			//fmt.Println("reduce over")
			c.AllDone = true
			reply.AllDone = true
		}
	}
	return nil
}

func (c *Coordinator) GoToReducePhase() error {
	for i := range c.NReduce {
		taskStatus := TaskStatus{TaskId: i}
		taskStatus.FileNames = c.GetTempOut(i)
		c.TaskReduceChannel <- &taskStatus
	}
	//c.TaskCompleteStatus = make([]int, c.NReduce)
	//fmt.Println("reduceWork 已全传入channel")

	c.TaskPhase = TaskReduce
	return nil
}

func (c *Coordinator) GetTempOut(i int) (ret []string) {
	workPath, _ := os.Getwd()
	files, _ := os.ReadDir(workPath)

	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), "mr-out-temp-") && strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
			ret = append(ret, file.Name())
		}
	}
	return
}

func (c *Coordinator) WorkDone(args *TaskArgs, reply *TaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	c.TaskCompleteStatus[args.TaskId] = TaskDone
	c.Timer[args.TaskId].Stop()
	delete(c.Timer, args.TaskId) //从映射中删除指定键值对
	//fmt.Printf("任务：%v，已经完成\n", args.TaskId)
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	//ret := false

	// Your code here.
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	return c.AllDone == true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.FileNames = files
	c.NReduce = nReduce
	c.TaskMapChannel = make(chan *TaskStatus, len(files))
	c.TaskReduceChannel = make(chan *TaskStatus, nReduce)
	//c.TaskCompleteStatus = make([]int, len(files))
	c.TaskCompleteStatus = make([]int, nReduce)
	c.Timer = make(map[int]*time.Timer, len(files))
	c.TaskPhase = TaskMap

	for i := range len(files) {
		task := TaskStatus{
			TaskId:   i,
			FileName: c.FileNames[i],
		}
		c.TaskMapChannel <- &task
	}
	//fmt.Println("任务已全传入channel")

	c.server()
	return &c
}
