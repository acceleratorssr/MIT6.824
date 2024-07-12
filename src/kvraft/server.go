package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

var (
	debugLog *log.Logger
)

func init() {
	file, err := os.Create("debug_KVServer.log")
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	debugLog = log.New(file, "", log.Lshortfile)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		debugLog.Printf(format, a...)
		//log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	TaskID   int
	ClientID int
	Key      string
	Value    string
	Op       string // "Put" or "Append" or "Get"
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kv           map[string]string
	LastTaskID   map[int]int
	applyChan    map[int]chan Op
	lastCommitID int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// 如果是leader才执行get请求
	//_, _ = DPrintf("server:%d 是leader (GET) \n", kv.me)

	logValue := Op{
		TaskID:   args.TaskID,
		ClientID: args.ClientID,
		Key:      args.Key,
		Value:    kv.kv[args.Key],
		Op:       "Get",
	}

	index, _, is := kv.rf.Start(logValue)

	if !is {
		reply.Err = Err(fmt.Sprintf("server:%d 不是leader (Get) \n", kv.me))
		return
	}

	kv.mu.Lock()
	ch := kv.GetApplyChanForCommitID(index)
	kv.mu.Unlock()

	select {
	case msg := <-ch:
		// 正常返回
		if msg.TaskID == logValue.TaskID && msg.ClientID == logValue.ClientID {
			_, _ = DPrintf("server:%d [%s]%s \n", kv.me, args.Key, kv.kv[args.Key])
			reply.Value = msg.Value
			return
		} else {
			_, _ = DPrintf("server:%d commond error:%v \n", kv.me, msg)
			reply.Err = Err(fmt.Sprintf("server:%d commond error:%v \n", kv.me, msg))
		}
	case <-time.After(1 * time.Second): //可能是leader过时了
		_, _ = DPrintf("server:%d 超时\n", kv.me)
		reply.Err = Err(fmt.Sprintf("server:%d outtime \n", kv.me))
	}

	go func() {
		kv.mu.Lock()
		delete(kv.applyChan, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// 如果是leader才执行PutAppend请求

	//_, _ = DPrintf("server:%d 是leader (PutAppend) \n", kv.me)

	logValue := Op{
		TaskID:   args.TaskID,
		ClientID: args.ClientID,
		Key:      args.Key,
		Value:    args.Value,
		Op:       args.Op,
	}

	//if args.Op == "Append" {
	//	logValue.Value = kv.kv[args.Key] + args.Value
	//}

	_, _ = DPrintf("server:%d need new log:%v \n", kv.me, logValue)
	index, _, is := kv.rf.Start(logValue)

	if !is {
		reply.Err = Err(fmt.Sprintf("server:%d 不是leader (PutAppend) \n", kv.me))
		return
	}

	kv.mu.Lock()
	ch := kv.GetApplyChanForCommitID(index)
	kv.mu.Unlock()

	select {
	case msg := <-ch:
		if msg.TaskID == logValue.TaskID && msg.ClientID == logValue.ClientID {
			_, _ = DPrintf("raft日志达成大多数\n")
			_, _ = DPrintf("server:%d [%s]%s \n", kv.me, args.Key, kv.kv[args.Key])
			reply.Err = ""
			return
		} else {
			_, _ = DPrintf("server:%d commond error:%v \n", kv.me, msg)
			reply.Err = Err(fmt.Sprintf("server:%d commond error:%v \n", kv.me, msg))
		}
	case <-time.After(1 * time.Second):
		_, _ = DPrintf("server:%d 超时\n", kv.me)
		reply.Err = Err(fmt.Sprintf("server:%d outOfTime \n", kv.me))
	}

	go func() {
		kv.mu.Lock()
		delete(kv.applyChan, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) apply() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastCommitID {
					kv.mu.Unlock()
					continue
				}

				kv.lastCommitID = msg.CommandIndex
				command := msg.Command.(Op)
				_, _ = DPrintf("%d server applyCh msg = %v\n", kv.me, msg)

				if command.Op == "Get" {
					command.Value = kv.kv[command.Key]
					_, _ = DPrintf("%d server applyCh msg = %v, value = %v\n", kv.me, msg, command.Value)
				}

				v, ok := kv.LastTaskID[command.ClientID]
				if ok {
					if command.TaskID > v {
						switch command.Op {
						case "Append":
							kv.kv[command.Key] += command.Value

						case "Put":
							kv.kv[command.Key] = command.Value
						}

						kv.LastTaskID[command.ClientID] = command.TaskID
					}
				}

				if _, isLeader := kv.rf.GetState(); isLeader {
					kv.GetApplyChanForCommitID(msg.CommandIndex) <- command
				}

				kv.mu.Unlock()
			}
		}
	}
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// servers[]包含一组服务器的端口，
// 这些端口将通过Raft进行协作以形成容错密钥/值服务。
// me is the index of the current server in servers[].
// me是服务器[]中当前服务器的索引。
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// k/v服务器应该通过底层Raft实现来存储快照，该实现应该调用persister.SaveStateAndSnapshot()
// 以原子方式将Raft状态与快照一起保存。
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// k/v服务器应该在Raft的保存状态超过maxraftstate字节时进行快照，
// 以便允许Raft垃圾收集其日志。如果maxraftstate为-1，则不需要快照。
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
// StartKVServer（）必须快速返回，因此它应该为任何长时间运行的工作启动goroutines。
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kv = make(map[string]string)
	kv.LastTaskID = make(map[int]int)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.applyChan = make(map[int]chan Op)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.apply()

	return kv
}

func (kv *KVServer) GetApplyChanForCommitID(index int) chan Op {
	ch, ok := kv.applyChan[index]
	if !ok {
		kv.applyChan[index] = make(chan Op, 1)
		ch = kv.applyChan[index]
	}
	_, _ = DPrintf("server:%d create chan for index:%d \n", kv.me, index)
	return ch
}
