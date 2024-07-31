# MIT6.824
lab1、2、3及课程笔记：https://www.yuque.com/u32495242/hqyqhm/tu2545e0ml3d7g1p?singleDoc#

博客：https://acceleratorssr.github.io/

lab2单次测试结果：[go_race_test_6_824_raft.html](go_race_test_6_824_raft.html)

lab3单次测试结果：[go_race_test_6_824_kvraft.html](go_race_test_6_824_kvraft.html)

# 说明
具体逻辑或者小优化就不展示了，条条大路通罗马，不同实现方案的倾向都不一样；

### 代码优化：
- 原：raft 节点初始化时额外开启一个 goroutine 循环睡眠，睡眠间隔需要加锁检查当前节点状态，如果为leader则同步发送心跳，否则继续睡眠；
- 新：raft 节点胜选后再开启心跳同步的 goroutine，使用 context 控制该 goroutine，即**不再需要加锁检查状态**；当节点不再是leader时，会调用cancel()，从而**直接退出**心跳同步goroutine；

<br>
	lab2中，在通过所有测试后，保证正确性的基础上，最后调优的参考依据为 Robert Morris 老师给出的通过测试的消耗：

-	测试的实际运行时间（以秒为单位）；
-	对等节点的数量；
-	RPC 发送数目；
-	发送的总字节数；
-	Raft 协议达成一致的总次数；

参考 Robert Morris 老师的测试结果，随机选取一次自己的结果，单次的性能对比（-race）：
<pre>
自己（原）：      <strong>13.5MB</strong>	429s

Robert（2022）：	169.8MB	468s 

自己（新）：      34.8MB	<strong>339s</strong>
</pre>

<p>
&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp通过观察测试用例发现，传输字节数主要差距在 TestC 的 Figure 8 (unreliable) 和 churn，老师的传输为<strong>三千万</strong>、<strong>四千万</strong>级别，及churn(unreliable)为<strong>百万</strong>级 ，而本实现的传输字节数分别为<strong>一千万</strong>级别、<strong>百万</strong>、<strong>百万</strong>级别，其他<strong>部分</strong>测试用例的传输消耗约为老师的<strong>一半</strong>；
</p>

### 坑
<p>
1、使用原来的 raft 时，在运行 lab3A 的 Speed 测试用例时，会因为超时而失败，在完善 lab3A 剩下部分后，通过了除 Speed 以外的所有 Part3A 的测试用例，最后排除才发现可能是 原实现的 raft 同步速度的过慢了；
在排除设计方面的问题后，将心跳间隔缩短一半（100ms缩短至50ms；选举判断间隔保持不变，为300±150ms），raft 集群依然可以正常工作，此时，使用新版本的 raft 即可正常通过 lab3A 的所有测试了；
</p>



> 注：之所以选择100ms作为心跳间隔，是因为在Lab2的布置的Task中，有一个老师提供的Hint：
> Hint: The tester requires that the leader send heartbeat RPCs no more than ten times per second.

故为了提升 raft 集群的通信效率，最开始时选用 Hint 的最短的间隔（1s / 10 = 100ms）；

<br>

2、lab3B 的 Speed 测试用例进一步要求每33.3ms完成一个操作，需要进一步缩短心跳间隔为（32ms），

> 注：由于本实现的 lab2 将 心跳和 append 合并为同一个 rpc 发送函数，所以心跳间隔会影响操作速度（ms/op）；
> 故由于lab3对速度的要求，个人认为标准做法应该将其分离，心跳间隔设置为100ms没问题，但同时append日志条目的方法需要单独抽离出来，独立发送即可；
> 按照当前间隔32ms的实现，有概率lab2测试会提示发送rpc次数过多（50ms不会触发）；