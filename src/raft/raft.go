package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   创建一个新的Raft服务器
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
//	 启动对新日志条目的协议
// rf.GetState() (term, isLeader)
//   询问Raft的当前任期，以及它是否认为自己是领导者
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer should
//   send an ApplyMsg to the service (or tester) in the same server.
// 	 每次向日志提交新条目时，每个Raft对等体都应该向同一服务器中的服务（或测试人员）发送ApplyMsg
//

import (
	"context"
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Leader = iota
	Follower
	Candidate
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are committed,
// 当每个Raft对等体意识到提交了连续的日志条目时，
// the peer should send an ApplyMsg to the service (or tester) on the same server,
// 对等方应当向同一服务器上的服务（或测试者）发送ApplyMsg，
// via the applyCh passed to Make().
// 通过传递给Make()的applyCh。
// set CommandValid to true to indicate that the ApplyMsg contains a newly committed log entry.
// 将CommandValid设置为true，表示ApplyMsg包含新提交的日志条目。
//
// in part 2D you'll want to send other kinds of messages (e.g. snapshots) on the applyCh,
// 在第2D部分中，您将希望在applyCh上发送其他类型的消息（例如快照），
// but set CommandValid to false for these other uses.
// 但是对于这些其他用途，将CommandValid设置为false。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntries struct {
	LogID   int
	TermID  int
	Command interface{}
}

type LogSuccessCount struct {
	SuccessCount map[int]int
	LogIndex     int
}

// Raft 实现单个Raft对等体的Go对象。
type Raft struct {
	mu        sync.Mutex          // 锁定以保护对此对等状态的共享访问
	peers     []*labrpc.ClientEnd // 所有对等端的RPC端点
	persister *Persister          // 对象以保持此对等方的持久状态
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// 请参阅本文的图2，了解Raft服务器必须保持的状态。
	// 响应RPC前，在持久性存储
	CurrentTerm int          // 服务器看到的最新任期（首次启动时初始化为0，单调增加）
	VotedFor    int          // 当前任期获得选票的候选人ID（如果没有则为空）
	Log         []LogEntries // 日志条目：每个日志条目包含状态机的命令，以及领导者收到条目的时间（第一个索引为1）

	// 易失性状态
	CommitIndex int // 已知被提交的最高日志条目的索引（初始化为0，单调增加）
	LastApplied int // 已应用于状态机的最高日志条目的索引（初始化为0，单调增加）

	// leader的易失性状态
	NextIndex  []int // 对于每个服务器，要发送给该服务器的下一个日志条目的索引（初始化为领导者的最后一个日志索引+1）
	MatchIndex []int // 对于每个服务器，已知在服务器上复制的最高日志条目的索引（初始化为0，单调增加）

	State             int32
	ResetChan         chan int
	AppendEntriesChan chan []LogEntries
	ApplyMsg          chan ApplyMsg
	SuccessCount      map[int]int //记录对应日志的同步情况，follower全返回true则删除对应kv
	Ctx               context.Context
	Cancel            context.CancelFunc
}

// GetState return currentTerm，以及此服务器是否认为自己是leader。
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.State == Leader
}

// 将Raft的持久状态保存到稳定存储中，
// 在那里它可以稍后在崩溃和重新启动之后被检索。
// 关于什么应该是持久性的描述，请参见本文的图2。
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// 恢复以前的持久状态。
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// CondInstallSnapshot 服务希望切换到快照。只有当Raft在applyCh上传递快照后没有更新的信息时才这样做。
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has all info up to and including index. this means the service no longer needs the log through (and including) that index. Raft should now trim its log as much as possible.
// 该服务表示，它已经创建了一个快照，其中包含索引之前的所有信息。这意味着服务不再需要通过（包括）该索引进行日志记录。木筏现在应该尽可能多地修剪原木。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs 示例RequestVote RPC参数结构。
// 字段名称必须以大写字母开头！
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期号
	CandidateID  int // 请求选票的候选人ID
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目任期号
}

// RequestVoteReply RequestVote RPC回复结构示例。
// 字段名称必须以大写字母开头！
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号，候选人会更新自己的任期号
	VoteGranted bool // true表示候选人获得了选票
}

type AppendEntriesArgs struct {
	Term         int          // leader当前任期
	LeaderID     int          // 使follower可以找到leader，为clients重定向
	PrevLogIndex int          // 紧接着新日志之前的日志条目的索引
	PrevLogTerm  int          // 紧接着新日志之前的日志条目的任期
	Entries      []LogEntries // 需要被保存的日志条目（心跳包的内容为空，运行一次发送多个）
	LeaderCommit int          // leader已知已提交的最高日志条目的索引
}

type AppendEntriesReply struct {
	Term         int  // 当前任期
	Success      bool // 如果日志条目顺序匹配，则返回true
	HopeLogIndex int  //当前follower期望最新日志
}

// RequestVote RPC处理程序示例
// 如果term < currentTerm，则返回false
// 如果VotedFor是nil/candidateID，且候选人日志至少和接收人的日志一样新，则投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("server:%d 接收到server:%d发送的投票RPC\n", rf.me, args.CandidateID)
	if rf.CurrentTerm >= args.Term { // 此时的args.Term已经自增了
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		fmt.Printf("server:%d 当前任期大于等于请求RPC的任期，故返回false\n", rf.me)
		return
	}

	// 候选人日志不够新
	// 即使投false，也要更新到候选人的任期
	if args.LastLogIndex < rf.CommitIndex {
		rf.CurrentTerm = args.Term
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		if rf.State == Leader {
			rf.Cancel()
			//rf.Kill()
			rf.State = Follower
		}
		fmt.Printf("server:%d 接收到节点:%d的投票请求，它的任期为:%d，最新日志索引为:%d，任期大于当前任期，但是日志不是最新的，故拒绝投票\n", rf.me, args.CandidateID, args.Term, args.LastLogIndex)
		return
	}
	// TODO 如果反对投票，follower任期大于leader,leader直接下岗,然后任一节点超时发起选举，有最新已提交日志的才有可能胜选
	//if args.Term > rf.CurrentTerm && args.LastLogIndex < rf.CommitIndex {
	//	rf.CurrentTerm = args.Term
	//	reply.Term = rf.CurrentTerm
	//	reply.VoteGranted = false
	//}

	// follower任期要变
	// 这里有注意还有第二种情况，
	// 如果follower没有收到响应（投给某一个candidate后得到的响应），
	// 那么此时voted在接收到下一个appendRPC前都是已经投票的状态；
	// 所以避免这种问题，如果下一个投票请求任期高于当前任期，则直接向他投票；
	// 貌似不用判断之前的投票，只要前面符合要求后，最后任期高于当前的就直接投票并更新自己的状态
	if args.Term > rf.CurrentTerm {
		rf.ResetChan <- 1 // 请求投票RPC也会刷新计时，之前忘了加
		rf.VotedFor = args.CandidateID
		rf.CurrentTerm = args.Term // 更新follower的任期为候选人的任期

		if rf.State == Candidate {
			rf.State = Follower
		} else if rf.State == Leader {
			rf.State = Follower
			rf.Cancel()
			//rf.Kill()
		}
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		fmt.Printf("server:%d 投票给了:%d\n", rf.me, args.CandidateID)
		rf.ResetChan <- 1 // 通知ticker重新初始化
		return
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.ResetChan <- 1 // 先刷新计时？

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果term < currentTerm，则返回false
	if args.Term < rf.CurrentTerm {
		fmt.Printf("server:%d 接收到追加日志RPC的任期为:%d, 小于自身当前任期:%d，将无视该RPC\n", rf.me, args.Term, rf.CurrentTerm)
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	} else if args.Term > rf.CurrentTerm { // 即使拒绝心跳包，也要更新自己的任期为: max（peers发送的, 自己的）
		fmt.Printf("server:%d 任期小于appendRPC的任期:%d\n", rf.me, args.Term)
		rf.CurrentTerm = args.Term
		if rf.State == Leader {
			rf.State = Follower
			rf.Cancel()
			//rf.Kill()
			fmt.Printf("旧leader:%d 转变为follower\n", rf.me)
		}
	}

	// follower日志落后
	if args.PrevLogIndex > len(rf.Log)-1 {
		fmt.Printf("args.PrevLogIndex, PrevLogTerm:%d %d\n", args.PrevLogIndex, args.PrevLogTerm)
		fmt.Printf("follower:%d 日志落后超过一个日志条目\n", rf.me)
		reply.Term = rf.CurrentTerm
		reply.Success = false
		reply.HopeLogIndex = rf.CommitIndex + 1
		fmt.Printf("follower:%d 期望日志索引:%d\n", rf.me, reply.HopeLogIndex)
		fmt.Printf("follower:%d 日志:%v\n", rf.me, rf.Log)
		return
	}
	// 如果日志在prevLogIndex处不包含term与prevLogTerm匹配的条目，则返回false，表示需要回退；
	if args.PrevLogTerm != rf.Log[args.PrevLogIndex].TermID {
		fmt.Printf("args.PrevLogIndex, PrevLogTerm:%d %d\n", args.PrevLogIndex, args.PrevLogTerm)
		fmt.Printf("follower:%d 日志在prevLogIndex:%d 处不包含term:%d 与prevLogTerm:%d 匹配的条目\n",
			rf.me, args.PrevLogIndex, rf.Log[args.PrevLogIndex].TermID, args.PrevLogTerm)
		fmt.Printf("follower:%d 日志落后超过一个日志条目\n", rf.me)
		reply.Term = rf.CurrentTerm
		reply.Success = false
		//reply.HopeLogIndex = args.PrevLogIndex //等效于后退一位
		reply.HopeLogIndex = rf.CommitIndex + 1
		fmt.Printf("follower:%d 期望日志索引:%d\n", rf.me, reply.HopeLogIndex)
		fmt.Printf("follower:%d 日志:%v\n", rf.me, rf.Log)
		return
	}

	// 如果一个现有的条目与一个新的条目相冲突（相同索引但是不同任期），则删除现有条目和后面所有条目
	// 如果leader发来的日志，就是当前follower拥有的最新日志，这里先这样处理：直接返回false
	// len(rf.Log) 是follower将存放新日志的位置，args.PrevLogIndex+1 是将要追加的新日志期望存放的位置
	if args.Entries != nil {
		if len(rf.Log) > args.PrevLogIndex+1 && args.Entries[0].TermID != rf.Log[args.PrevLogIndex+1].TermID {
			rf.Log = rf.Log[:args.PrevLogIndex+1]
			fmt.Printf("server:%d 一个现有的条目%d 与一个新的条目%d 相冲突\n", rf.me, len(rf.Log), args.PrevLogIndex+1)
			fmt.Printf("server:%d 日志:%v\n", rf.me, rf.Log)
		} else if len(rf.Log) > args.PrevLogIndex+1 && args.Entries[0].TermID == rf.Log[args.PrevLogIndex+1].TermID {
			fmt.Printf("server:%d 收到重复的相同日志: %d\n", rf.me, args.PrevLogIndex+1)
			reply.Term = rf.CurrentTerm
			reply.Success = true
			return
		}
	}

	if args.Entries == nil {
		// 如果此时节点还是candidate时,收到新leader心跳包,且其任期大于等于自己,则会转变回follower
		if rf.State == Candidate && rf.CurrentTerm <= args.Term {
			rf.State = Follower
		}

		// 表示为心跳包
		rf.ResetChan <- 1 // 继续睡眠

		fmt.Println("------------------")
		fmt.Printf("follower:%d 收到 %d 的心跳\n", rf.me, args.LeaderID)
		fmt.Println("------------------")

		reply.Term = rf.CurrentTerm
		reply.Success = true
	} else {
		for _, v := range args.Entries {
			rf.Log = append(rf.Log, v)
		}
		fmt.Printf("follower:%d 收到日志，序号: %d，日志: %v\n", rf.me, args.PrevLogIndex+1, rf.Log)
		rf.ResetChan <- 1 // 继续睡眠
		reply.Term = rf.CurrentTerm
		reply.Success = true
	}

	if args.LeaderCommit > rf.CommitIndex { // follower提交日志
		rf.CommitIndex = min(args.LeaderCommit, len(rf.Log)-1) // 0不是日志，从1开始
		rf.LastApplied = rf.CommitIndex                        // 暂时直接提交及应用
		fmt.Printf("follower:%d 更新最新的提交日志索引: min(%d, %d)\n", rf.me, args.LeaderCommit, len(rf.Log)-1)
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[rf.CommitIndex].Command,
			CommandIndex: rf.CommitIndex,
		}
		rf.ApplyMsg <- applyMsg
		fmt.Printf("follower:%d 日志: %v\n", rf.me, rf.Log)
		reply.Term = rf.CurrentTerm
		reply.Success = true
	}
}

// example code to send a RequestVote RPC to a server.
// 向服务器发送RequestVote RPC的示例代码。
// server is the index of the target server in rf.peers[].
// server是rf.peers[]中目标服务器的索引。
// expects RPC arguments in args.
// 参数中应包含RPC参数。
// fills in *reply with RPC reply, so caller should pass &reply.
// 用RPC回复填充回复，所以调用者应该通过并回复。
// the types of the args and reply passed to Call() must be the
// same as the types of the arguments declared
// in the handler function (including whether they are pointers).
// 传递给Call（）的参数和回复的类型必须与处理程序函数中声明的参数的类型相同（包括它们是否为指针）。
// The labrpc package simulates a lossy network, in which servers may be unreachable,
// and in which requests and replies may be lost.
// labrpc包模拟了一个有损网络，其中服务器可能无法访问，请求和回复可能丢失
// Call() sends a request and waits for a reply. If a reply arrives within a timeout interval,
// Call() returns true; otherwise Call() returns false. Thus Call() may not return for a while.
// Call()发送一个请求并等待答复。如果回复在超时间隔内到达，则Call()返回true；否则，Call()返回false。因此，Call()可能在一段时间内不会返回。
// A false return can be caused by a dead server,
// a live server that can't be reached, a lost request, or a lost reply.
// 错误的返回可能是由于服务器失效、无法连接到活动服务器、请求丢失或回复丢失造成的。

// Call() is guaranteed to return (perhaps after a delay) *except*
// if the handler function on the server side does not return.
// Thus there is no need to implement your own timeouts around Call().
// 除非服务器端的处理程序函数没有返回，否则Call()保证会返回（可能在延迟之后）。因此，没有必要在Call()周围实现您自己的超时。

// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that
// you've capitalized all field names in structs passed over RPC,
// and that the caller passes the address of the reply struct with &, not the struct itself.
// 如果您在使RPC工作时遇到问题，请检查您是否已将通过RPC传递的结构中的所有字段名大写，并且调用者是否使用&传递回复结构的地址，而不是结构本身。
// flag 为true时，胜选变为leader
func (rf *Raft) sendRequestVote(server int, votesCount *int32, flag *sync.Once, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		fmt.Printf("server:%d to %d, sendRequestVote -> Call Raft.RequestVote error\n", rf.me, server)
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("server:%d, term:%d, commitIndex:%d\n", rf.me, rf.CurrentTerm, rf.CommitIndex)
	// 发出投票请求后，当前候选人的任期如果比follower任期大，则会更新follower任期
	if reply.VoteGranted {
		fmt.Printf("server:%d 获取到server:%d的选票\n", rf.me, server)
		//*votesCount++
		atomic.AddInt32(votesCount, 1)
	} else {
		return false
	}

	// 胜选
	if *votesCount > int32(len(rf.peers))/2 && rf.State == Candidate && rf.CurrentTerm == args.Term {
		flag.Do(func() {
			rf.NextIndex = make([]int, len(rf.peers))
			for i := range rf.NextIndex {
				// leader不可能覆盖自己的日志，所有应该初始化为最新日志的索引+1
				//rf.NextIndex[i] = rf.CommitIndex + 1
				rf.NextIndex[i] = len(rf.Log)
			}
			rf.MatchIndex = make([]int, len(rf.peers))

			rf.SuccessCount = make(map[int]int, 256)
			// 新leader上任后，并不知道未提交的日志达成了多少的大多数，先投上自己一票
			for i := rf.CommitIndex + 1; i < len(rf.Log); i++ {
				rf.SuccessCount[i]++
			}
			rf.dead = 0
			if rf.State == Candidate {
				rf.State = Leader
			}
			rf.Ctx, rf.Cancel = context.WithCancel(context.Background())
			fmt.Printf("server:%d <- 胜选 ->，任期为: %d\n", rf.me, rf.CurrentTerm)
			//发送心跳包
			go func() {
				rf.sendHeartOrAppend()
				for {
					// 这里胜选后立刻发送心跳;
					select {
					case <-time.After(100 * time.Millisecond):
						rf.sendHeartOrAppend()
					case <-rf.Ctx.Done():
						fmt.Printf("leader:%d 停止发送心跳包\n", rf.me)
						return
					}
					//if rf.killed() {
					//	return
					//}
				}
			}()
		})
	}

	return false
}

// leader 这里是leader定时发送心跳，只有leader一个go程执行
func (rf *Raft) sendHeartOrAppend() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 貌似下次心跳就会刷新掉flag
	//var flag sync.Once

	for i := range len(rf.peers) {
		if i == rf.me {
			continue
		}

		if rf.State == Follower {
			//检查自己如果不是leader就停止发送
			return true
		}
		args := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: rf.NextIndex[i] - 1,
			PrevLogTerm:  rf.Log[rf.NextIndex[i]-1].TermID,
			//Entries:      []LogEntries{entries},
			LeaderCommit: rf.CommitIndex,
		}
		reply := AppendEntriesReply{}

		if args.PrevLogIndex < len(rf.Log)-1 {
			var entries LogEntries

			fmt.Printf("Leader:%d 发现server:%d, 日志落后，现在可能需要日志:%d\n", rf.me, i, rf.NextIndex[i])
			entries = LogEntries{
				LogID:   rf.NextIndex[i],
				TermID:  rf.Log[rf.NextIndex[i]].TermID,
				Command: rf.Log[rf.NextIndex[i]].Command,
			}
			args.Entries = []LogEntries{entries}
			fmt.Printf("Leader:%d 任期为:%d 发送日志到follower:%d, logIndex:%d\n", rf.me, rf.CurrentTerm, i, rf.NextIndex[i]) // 新的nextIndex从1开始

			go rf.sendAppendEntries(i, &args, &reply)
			continue
		}
		fmt.Printf("leader:%d 发送心跳包到follower:%d\n", rf.me, i)
		go rf.sendAppendEntries(i, &args, &reply)
	}
	return true
}

// leader
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		fmt.Printf("leader %d to %d, sendAppendEntries -> Call Raft.AppendEntries error\n", rf.me, server)
		return true
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 正常情况：
	if reply.Success && args.Entries != nil {
		//rf.NextIndex[server]++
		if rf.NextIndex[server] != args.Entries[0].LogID+1 {
			//successCount.SuccessCount[args.Entries[0].LogID]++
			rf.SuccessCount[args.Entries[0].LogID]++
		}
		rf.NextIndex[server] = args.Entries[0].LogID + 1
		rf.MatchIndex[server] = args.Entries[0].LogID

		//successCount.LogIndex 不等于args.PrevLogIndex+1

		fmt.Printf("leader:%d 收到follower:%d 的响应:%d ok\n", rf.me, server, args.Entries[0].LogID)
		fmt.Printf("日志:%d 目前达成%d个节点的同意\n", args.PrevLogIndex+1, rf.SuccessCount[args.Entries[0].LogID])
	}

	// 日志达成大多数
	if args.Entries != nil && rf.SuccessCount[args.Entries[0].LogID] > len(rf.peers)/2 {
		if rf.SuccessCount[args.Entries[0].LogID] == len(rf.peers) {
			delete(rf.SuccessCount, args.Entries[0].LogID)
			fmt.Printf("该日志:%d已被同步到所有follower\n", args.Entries[0].LogID)
		}
		//flag.Do(func() {
		rf.CommitIndex = max(args.Entries[0].LogID, rf.CommitIndex)
		fmt.Printf("leader:%d 更新被提交的最高日志条目的索引为:%d\n", rf.me, rf.CommitIndex)

		rf.LastApplied = rf.CommitIndex // 暂时直接提交=应用
		fmt.Printf("leader:%d 日志:%v", rf.me, rf.Log)
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[rf.CommitIndex].Command,
			CommandIndex: rf.CommitIndex,
		}
		rf.ApplyMsg <- applyMsg
		//})
	}

	// TODO 返回false的处理
	// 此时返回的false可能是因为leader任期小于follower，或者follower日志不够新需要回退
	if !reply.Success && reply.Term == rf.CurrentTerm && reply.HopeLogIndex != 0 {
		// leader已经提交了，但是follower缺少日志在追的过程中，leader必须也先要投一票
		for i := rf.NextIndex[server] - 1; i >= reply.HopeLogIndex; i-- {
			if rf.SuccessCount[i] == 0 {
				rf.SuccessCount[i]++
			}
		}
		rf.NextIndex[server] = reply.HopeLogIndex // 回退日志
		fmt.Printf("leader:%d 收到follower:%d 的回退日志请求，回退到:%d\n", rf.me, server, rf.NextIndex[server])
	} else if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		if rf.State == Leader {
			rf.State = Follower
			//rf.Kill()
			rf.Cancel()
		}
		fmt.Printf("old leader:%d 发现自己任期小于follower:%d 故变回follower\n ", rf.me, server)
	}
	return true
}

// Start the service using Raft (e.g. a k/v server) wants to start agreement on the next command to be appended to Raft's log.
// 使用Raft的服务（例如，kv服务器）希望开始就要附加到Raft的日志的下一个命令达成一致。
// if this server isn't the leader, returns false. otherwise start the agreement and return immediately.
// 如果此服务器不是领导者，则返回false。否则，启动协议并立即返回。
// there is no guarantee that this command will ever be committed to the Raft log,
// since the leader may fail or lose an election.
// 不能保证这个命令会被提交给拉夫特日志，因为领导人可能会失败或输掉选举。
// even if the Raft instance has been killed, this function should return gracefully.
// 即使Raft实例已被终止，此函数也应正常返回。

// Start
// the first return value is the index that the command will appear at if it's ever committed.
// 第一个返回值是如果命令被提交，它将出现在的索引。
// the second return value is the current term.
// 第二个返回值是当前任期。
// the third return value is true if this server believes it is the leader.
// 如果此服务器认为它是领导者，则第三个返回值为true。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term, isLeader := rf.GetState()
	//index := rf.CommitIndex + 1 //注意此处要+1，因为是leader确定下一个需要被提交的日志的index
	index := len(rf.Log)
	// 这里没写重定向到leader
	if !isLeader {
		fmt.Printf("follower:%d return CommitIndex:%d\n", rf.me, rf.CommitIndex)
		//return index, term, isLeader
		return -1, term, isLeader //-1
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println("find leader")
	entries := LogEntries{
		LogID:   len(rf.Log),
		TermID:  rf.CurrentTerm,
		Command: command, // TODO 处理多条命令？
	}
	fmt.Printf("leader:%d 收到新日志:%d 追加请求\n", rf.me, len(rf.Log))
	rf.Log = append(rf.Log, entries)
	rf.SuccessCount[len(rf.Log)-1]++ //记得leader同步的日志也算进大多数内
	fmt.Printf("leader:%d 日志:%v\n", rf.me, rf.Log)
	// 如果请求和心跳包两个是不同的发送appendRPC，那么请求有可能返回多次false，导致nextIndex不正常回退

	// Your code here (2B).
	//fmt.Printf("leader:%d return CommitIndex:%d\n", rf.me, rf.CommitIndex)
	fmt.Printf("leader将返回该日志期望的提交位置:%d，当前任期:%d，和自己是leader:%v\n", index, term, isLeader)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test, but it does call the Kill() method.
// 测试人员不会在每次测试后停止Raft创建的goroutine，但它会调用Kill（）方法。
// your code can use killed() to check whether Kill() has been called.
// 您的代码可以使用killed（）来检查是否调用了Kill（）。
// the use of atomic avoids the need for a lock.
// 原子的使用避免了对锁的需要。

// Kill
// the issue is that long-running goroutines use memory and may chew up CPU time,
// 问题是长时间运行的goroutine使用内存并且可能占用CPU时间，
// perhaps causing later tests to fail and generating confusing debug output.
// 可能会导致以后的测试失败，并生成令人困惑的调试输出。
// any goroutine with a long-running loop should call killed() to check whether it should stop.
// 任何具有长时间运行循环的goroutine都应该调用killed（）来检查它是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	// 从 rf.dead 中加载（Load）一个 int32 类型的值，
	// 并且确保在加载期间不会被中断或者其他goroutine影响，保证操作的原子性。
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 生成随机数范围从 300ms 到 800ms 的函数
func randomDuration() time.Duration {
	// 使用当前时间作为种子
	source := rand.NewSource(time.Now().UnixNano())
	// 创建本地的伪随机数生成器
	generator := rand.New(source)

	// 生成一个介于 0 到 400 之间的随机数，加上 700，得到随机范围内的毫秒数
	randomMilliseconds := generator.Intn(501) + 300
	// 将毫秒数转换为 Duration 类型
	duration := time.Duration(randomMilliseconds) * time.Millisecond
	return duration
}

// The ticker go routine starts a new election if this peer hasn't received heartsBeats recently.
// 如果这位同行最近没有收到心跳，那么自动投票程序将开始新的选举。
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// 调用函数生成随机时长
		randomTime := randomDuration()
		time.Sleep(randomTime)

		//fmt.Printf("%d server, term:%d, i am :%v\n", rf.me, rf.CurrentTerm, rf.State)
		// 继续睡
		// 可能是其他节点的投票请求重置，或者是心跳包重置（voteFor置-1），
		reset := len(rf.ResetChan)
		if reset != 0 {
			fmt.Printf("server:%d Reset:%d\n", rf.me, reset)
			for reset != 0 {
				<-rf.ResetChan
				reset = len(rf.ResetChan)
			}
			fmt.Println("----------")
			continue
		}

		rf.mu.Lock()
		if rf.State == Leader {
			rf.mu.Unlock()
			continue
		}
		fmt.Printf("%d server, term:%d, start vote\n", rf.me, rf.CurrentTerm)
		// 任期号自增
		rf.CurrentTerm++

		rf.VotedFor = rf.me // 投给自己
		rf.State = Candidate

		rf.mu.Unlock()

		var VotesCount int32
		VotesCount = 1
		var flag sync.Once
		for i := range len(rf.peers) {
			if i == rf.me {
				continue
			}

			args := RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateID:  rf.me,
				LastLogIndex: rf.CommitIndex,
				LastLogTerm:  rf.CurrentTerm,
			}
			reply := RequestVoteReply{}

			fmt.Printf("server:%d send voteRPC to %d\n", rf.me, i)
			go rf.sendRequestVote(i, &VotesCount, &flag, &args, &reply)
		}
	}
}

// Make
// the service or tester wants to create a Raft server.
// 服务或测试人员想要创建一个Raft服务器。
// the ports of all the Raft servers (including this one) are in peers[].
// 所有Raft服务器（包括这一个）的端口都在对等端[]中。
// this server's port is peers[me]. all the servers' peers[] arrays have the same order.
// 此服务器的端口是peers[me]。所有服务器的对等方[]阵列具有相同的顺序。
// persister is a place for this server to save its persistent state,
// persister是该服务器保存其持久状态的地方，
// and also initially holds the most recent saved state,
// 并且最初还保持最近保存的状态，
// if any. applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.
// 如果有的话。applyCh是测试人员或服务希望Raft在其上发送ApplyMsg消息的通道。
// Make() must return quickly, so it should start goroutines for any long-running work.
// Make()必须快速返回，所以它应该为任何长时间运行的工作启动goroutines。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.Log = append(rf.Log, LogEntries{})

	// Your initialization code here (2A, 2B, 2C).
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, len(rf.peers))
	// 这里需要初始化为1
	for i := range rf.NextIndex {
		rf.NextIndex[i] = rf.CommitIndex + 1
	}

	rf.MatchIndex = make([]int, len(rf.peers))
	rf.ApplyMsg = applyCh //!!!!!

	rf.State = Follower
	rf.ResetChan = make(chan int, 10)
	// // 将写入leader的日志内容，通过channel发送到负责心跳包的goroutine并且发送出去
	rf.AppendEntriesChan = make(chan []LogEntries, 10)
	rf.SuccessCount = make(map[int]int, 256)

	// initialize from state persisted before a crash
	// 从崩溃前保持的状态初始化
	rf.readPersist(persister.ReadRaftState())

	fmt.Printf("%d server, term:%d\n", rf.me, rf.CurrentTerm)

	// start ticker goroutine to start elections
	// 启动ticker goroutine启动选举
	go rf.ticker()

	return rf
}
