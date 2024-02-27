package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		args := TaskArgs{}
		reply := TaskReply{}
		ok := call("Coordinator.GetWork", &args, &reply)
		// work请求一次拿一个map任务执行，完成后返回ok
		if ok {
			switch reply.TaskPhase {
			case TaskMap:
				if reply.FileName != "" {
					MapWorker(mapf, &reply)
				} else if reply.WorkerStatus == WorkerWaiting {
					//TODO 暂时暴力停一秒等待
					time.Sleep(time.Second)
					//fmt.Println("worker waiting...")
				}
			case TaskReduce:
				if reply.FileNames != nil {
					ReduceWorker(reducef, &reply)
				} else if reply.WorkerStatus == WorkerWaiting {
					time.Sleep(time.Second)
					//fmt.Println("worker waiting...")
				}

			default:
				panic("unhandled default case")
			}
		}
		if reply.AllDone {
			//fmt.Println("work over")
			files, err := filepath.Glob("mr-out-temp-*")
			if err != nil {
				fmt.Println("匹配文件时出错:", err)
				return
			}
			// 遍历匹配到的文件列表，逐个删除文件
			for _, file := range files {
				err = os.Remove(file)
				if err != nil {
					fmt.Printf("删除文件 %s 时出错: %v\n", file, err)
				}
			}
			return
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}

func MapWorker(mapf func(string, string) []KeyValue, reply *TaskReply) {
	intermediate := []KeyValue{}
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	file.Close()
	kva := mapf(reply.FileName, string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))

	// 在本地保存intermediate
	// 按照key进行分进不同的HashKV桶
	HashKV := make([][]KeyValue, reply.NReduce)
	for _, kv := range intermediate {
		HashKV[ihash(kv.Key)%reply.NReduce] = append(HashKV[ihash(kv.Key)%reply.NReduce], kv)
	}

	for i := range reply.NReduce {
		tempName := "mr-out-temp-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(i)
		tempFile, _ := os.Create(tempName)
		// 创建一个JSON编码器，并将其关联到先前创建的文件对象ofile。JSON编码器可以将数据编码为JSON格式并写入到文件中;
		enc := json.NewEncoder(tempFile)
		for _, kv := range HashKV[i] {
			enc.Encode(kv)
		}
		tempFile.Close()
	}

	args := TaskArgs{}
	args.TaskId = reply.TaskId
	//fmt.Println("work ok: ", reply.FileName)
	ok := call("Coordinator.WorkDone", &args, &reply)
	// TODO 处理失败的情况，开一个go程重复发送
	if !ok {
		fmt.Println("mapWorker call error")
		return
	}
}

func ReduceWorker(reducef func(string, []string) string, reply *TaskReply) {
	var kva []KeyValue
	for _, file := range reply.FileNames {
		f, _ := os.Open(file)
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))

	oname := "mr-out-" + strconv.Itoa(reply.TaskId)
	ofile, _ := os.Create(oname)

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
	ofile.Close()

	args := TaskArgs{}
	args.TaskId = reply.TaskId
	//fmt.Println("redecue work ok: ", reply.TaskId)
	ok := call("Coordinator.WorkDone", &args, &reply)
	// TODO 处理失败的情况，开一个go程重复发送
	if !ok {
		fmt.Println("Worker call error")
		return
	}
}

func SendACK(reply *TaskReply) {

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
