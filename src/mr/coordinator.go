package mr

import (
	"fmt"
	"log"
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
	TaskMap = iota
	TaskReduce
)

// TaskStatus 具体单个任务的信息
type TaskStatus struct {
	TaskId     int
	FileName   string
	TaskStatus int
}

type Coordinator struct {
	// Your definitions here.
	FileNames          []string
	TaskCompleteStatus []int            // 记录对应index的任务状态：未运行，运行中，完成
	TaskMapChannel     chan *TaskStatus // 在channel中的任务都是没有被完成的
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetWork(args *TaskArgs, reply *TaskReply) error {
	select {
	case task := <-c.TaskMapChannel:
		reply.FileName = task.FileName
		reply.TaskId = task.TaskId
		reply.TaskType = TaskExecuting
	default:
		for i := range c.TaskCompleteStatus {
			if i == 0 {
				// TODO 默认不出错
				return nil
			}
		}
		fmt.Println("map over")
		reply.AllDone = true
	}

	// TODO reduce
	var task TaskStatus
	reply.FileName = task.FileName
	reply.TaskId = task.TaskId
	reply.TaskType = TaskExecuting
	return nil
}

func (c *Coordinator) WorkDone(args *TaskArgs, reply *TaskReply) error {
	c.TaskCompleteStatus[args.TaskId] = 1
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.FileNames = files
	c.TaskMapChannel = make(chan *TaskStatus, len(files))
	c.TaskCompleteStatus = make([]int, len(files))

	for i := range len(files) {
		task := TaskStatus{
			TaskId:     i,
			FileName:   c.FileNames[i],
			TaskStatus: TaskNotExecuted,
		}
		c.TaskMapChannel <- &task
	}
	fmt.Println("任务已全传入channel")

	c.server()
	return &c
}
