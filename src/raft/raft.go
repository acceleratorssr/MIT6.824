package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   创建一个新的Raft服务器
// rf.Start(command interface{}) (index, term, isleader)
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
	CurrentTerm int   // 服务器看到的最新任期（首次启动时初始化为0，单调增加）
	VotedFor    int   // 当前任期获得选票的候选人ID（如果没有则为空）
	Log         []int // 日志条目：每个日志条目包含状态机的命令，以及领导者收到条目的时间（第一个索引为1）

	// 易失性状态
	CommitIndex int // 已知被提交的最高日志条目的索引（初始化为0，单调增加）
	LastApplied int // 已应用于状态机的最高日志条目的索引（初始化为0，单调增加）

	// leader的易失性状态
	NextIndex  []int // 对于每个服务器，要发送给该服务器的下一个日志条目的索引（初始化为领导者的最后一个日志索引+1）
	MatchIndex []int // 对于每个服务器，已知在服务器上复制的最高日志条目的索引（初始化为0，单调增加）

	State     int
	ResetChan chan int
}

// GetState return currentTerm，以及此服务器是否认为自己是leader。
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (2A).

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
	Term         int   // leader当前任期
	LeaderID     int   // 使follower可以找到leader，为clients重定向
	PrevLogIndex int   // 紧接着新日志之前的日志条目的索引
	PrevLogTerm  int   // 紧接着新日志之前的日志条目的任期
	Entries      []int // 需要被保存的日志条目（心跳包的内容为空，运行一次发送多个）
	LeaderCommit int   // leader已知已提交的最高日志条目的索引
}

type AppendEntriesReply struct {
	Term    int  // 当前任期
	Success bool // 如果日志条目顺序匹配，则返回true
}

// RequestVote RPC处理程序示例
// 如果term < currentTerm，则返回false
// 如果VotedFor是nil/candidateID，且候选人日志至少和接收人的日志一样新，则投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.CurrentTerm >= args.Term { // 此时的args.Term已经自增了
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		fmt.Printf("%d server, term:%d, votefor:%d", rf.me, rf.CurrentTerm, args.CandidateID)
	}
	// TODO 候选人日志不够新
	// 即使投false，也要更新到候选人的任期

	//follower任期要变
	if rf.VotedFor == -1 {
		rf.VotedFor = args.CandidateID
		rf.CurrentTerm = args.Term // 更新follower的任期为候选人的任期
		rf.State = Follower
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true

		rf.ResetChan <- 1 // 通知ticker重新初始化
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Entries == nil {
		// 表示为心跳包
		rf.ResetChan <- 1 //继续睡眠
		rf.VotedFor = -1
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
func (rf *Raft) sendRequestVote(server int, votesCount *int, flag *sync.Once, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		fmt.Println("sendRequestVote -> Call Raft.RequestVote error")
		return false
	}
	// 发出投票请求后，当前候选人的任期如果比follower任期大，则会更新follower任期
	if reply.VoteGranted {
		*votesCount++
	}

	// 胜选
	if *votesCount > len(rf.peers)/2 && rf.State == Candidate {
		rf.VotedFor = -1
		//rf.CurrentTerm == args.Term // 好像不需要
		rf.State = Leader
		flag.Do(func() {
			fmt.Printf("%d win, term:%d\n", rf.me, rf.CurrentTerm)
			//发送心跳包
			go func() {
				for {
					time.Sleep(100 * time.Millisecond)
					rf.sendHeart()
					//fmt.Printf("%d server, term:%d\n", rf.me, rf.CurrentTerm)
				}
			}()
		})
	}

	return false
}

func (rf *Raft) sendHeart() bool {
	for i := range len(rf.peers) {
		if i == rf.me {
			continue
		}
		request := AppendEntriesArgs{
			Term:     rf.CurrentTerm,
			LeaderID: rf.me,
			//PrevLogIndex: rf.CommitIndex - 1, // no sure
			//PrevLogTerm:  rf.CurrentTerm,
			LeaderCommit: rf.CommitIndex,
		}
		response := AppendEntriesReply{}
		go rf.sendAppendEntries(i, &request, &response)
	}
	return true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		fmt.Printf("sendAppendEntries -> Call Raft.AppendEntries error, server %d to %d\n", rf.me, server)
		return true
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
// 第二个返回值是当前项。
// the third return value is true if this server believes it is the leader.
// 如果此服务器认为它是领导者，则第三个返回值为true。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

// 生成随机数范围从 500ms 到 800ms 的函数
func randomDuration() time.Duration {
	// 使用当前时间作为种子
	source := rand.NewSource(time.Now().UnixNano())
	// 创建本地的伪随机数生成器
	generator := rand.New(source)

	// 生成一个介于 0 到 300 之间的随机数，加上 500，得到随机范围内的毫秒数
	randomMilliseconds := generator.Intn(301) + 500
	// 将毫秒数转换为 Duration 类型
	duration := time.Duration(randomMilliseconds) * time.Millisecond
	return duration
}

// The ticker go routine starts a new election if this peer hasn't received heartsbeats recently.
// 如果这位同行最近没有收到心跳，那么自动投票程序将开始新的选举。
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// 调用函数生成随机时长
		randomTime := randomDuration()
		time.Sleep(randomTime)

		fmt.Printf("%d server, term:%d, i am :%v\n", rf.me, rf.CurrentTerm, rf.State)
		// 继续睡
		// 可能是其他节点的投票请求重置，或者是心跳包重置（voteFor置-1），
		reset := len(rf.ResetChan)
		if rf.VotedFor != -1 || reset != 0 || rf.State == Leader {
			<-rf.ResetChan
			fmt.Println("Reset:", reset)
			fmt.Println(rf.VotedFor)
			continue
		}

		fmt.Printf("%d server, term:%d, start vote\n", rf.me, rf.CurrentTerm)
		// 任期号自增
		rf.CurrentTerm++
		args := RequestVoteArgs{
			Term:         rf.CurrentTerm,
			CandidateID:  rf.me,
			LastLogIndex: rf.CommitIndex,
			LastLogTerm:  rf.CurrentTerm,
		}
		reply := RequestVoteReply{}
		rf.VotedFor = rf.me // 投给自己
		rf.State = Candidate

		VotesCount := 1
		for i := range len(rf.peers) {
			if i == rf.me {
				continue
			}
			//go func(num int) {
			//	rf.sendRequestVote(num, &args, &reply)
			//}(i)
			var flag sync.Once
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

	// Your initialization code here (2A, 2B, 2C).
	rf.CurrentTerm = 1 //不是应该初始化为0吗
	rf.VotedFor = -1
	rf.CommitIndex = 0
	rf.LastApplied = 0

	rf.State = Follower
	rf.ResetChan = make(chan int, 5)
	// initialize from state persisted before a crash
	// 从崩溃前保持的状态初始化
	rf.readPersist(persister.ReadRaftState())

	fmt.Printf("%d server, term:%d\n", rf.me, rf.CurrentTerm)

	// start ticker goroutine to start elections
	// 启动ticker goroutine启动选举
	go rf.ticker()

	return rf
}
