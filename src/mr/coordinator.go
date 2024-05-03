package mr

import (
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 标记该任务是否已经完成，map阶段或者reduce阶段
const (
	TaskNotExecuted = iota
	TaskExecuting
	TaskDone
)

// 标记worker的状态，有任务或者空闲等待
const (
	WorkerWorking = iota
	WorkerWaiting
)

// 标识任务阶段
const (
	TaskMap = iota
	TaskReduce
)

// TaskStatus 具体任务的信息
type TaskStatus struct {
	TaskId    int
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
	select {
	// Map
	case task := <-c.TaskMapChannel:
		c.DealWithWork(task, reply)
	// Reduce
	case task := <-c.TaskReduceChannel:
		c.DealWithWork(task, reply)
	default:
		reply.WorkerStatus = WorkerWaiting
		for _, i := range c.TaskCompleteStatus {
			if i == TaskExecuting {
				// 定时器解决宕机问题
				return nil
			}
		}
		if c.TaskPhase == TaskMap {
			err := c.GoToReducePhase()
			if err != nil {
				fmt.Println("GoToReducePhase error")
				return nil
			}
			close(c.TaskMapChannel)
		} else if c.TaskPhase == TaskReduce {
			c.AllDone = true
			reply.AllDone = true
			// 删除中间文件
			files, err := filepath.Glob("mr-out-temp-*")
			if err != nil {
				fmt.Println("匹配文件时出错:", err)
				return nil
			}
			// 遍历匹配到的文件列表，逐个删除文件
			for _, file := range files {
				err = os.Remove(file)
				if err != nil {
					fmt.Printf("删除文件 %s 时出错: %v\n", file, err)
				}
			}
			close(c.TaskReduceChannel)
		}
	}
	return nil
}

func (c *Coordinator) DealWithWork(task *TaskStatus, reply *TaskReply) {
	reply.FileNames = task.FileNames
	reply.NReduce = c.NReduce
	reply.TaskPhase = c.TaskPhase
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
			if c.TaskPhase == TaskReduce {
				taskStatus := TaskStatus{FileNames: c.GetTempOut(id), TaskId: id}
				c.TaskReduceChannel <- &taskStatus
			} else if c.TaskPhase == TaskMap {
				taskStatus := TaskStatus{FileNames: []string{c.FileNames[id]}, TaskId: id}
				c.TaskMapChannel <- &taskStatus
			}
			c.Timer[id].Stop()
			delete(c.Timer, id) //从映射中删除指定键值对 //注意不能并发写
		}
	}(reply.TaskId)
}

func (c *Coordinator) GoToReducePhase() error {
	for i := range c.NReduce {
		taskStatus := TaskStatus{
			TaskId:    i,
			FileNames: c.GetTempOut(i),
		}
		c.TaskReduceChannel <- &taskStatus
	}
	//fmt.Println("reduceWork 已全传入channel")

	c.TaskPhase = TaskReduce
	return nil
}

func (c *Coordinator) GetTempOut(i int) (ret []string) {
	workPath, _ := os.Getwd()
	files, _ := os.ReadDir(workPath)

	// 对比前后缀
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

// Example
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

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//ret := false

	// Your code here.
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	return c.AllDone == true
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.FileNames = files
	c.NReduce = nReduce
	c.TaskMapChannel = make(chan *TaskStatus, len(files))
	c.TaskReduceChannel = make(chan *TaskStatus, nReduce)
	c.TaskCompleteStatus = make([]int, max(nReduce, len(files)))
	c.Timer = make(map[int]*time.Timer, len(files))
	c.TaskPhase = TaskMap

	// 将map任务放入channel
	for i, v := range files {
		task := TaskStatus{
			TaskId:    i,
			FileNames: []string{v},
		}
		c.TaskMapChannel <- &task
	}
	//fmt.Println("任务已全传入channel")

	c.server()
	return &c
}
