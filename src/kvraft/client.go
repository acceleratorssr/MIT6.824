package kvraft

import (
	"6.824/labrpc"
	"6.824/raft"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ID         int64
	LastLeader int
	LastTaskID int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ID = nrand()
	ck.LastLeader = 0
	ck.LastTaskID = 0

	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		TaskID:   ck.LastTaskID,
		ClientID: ck.ID,
		Key:      key,
	}
	reply := GetReply{}

	for i := ck.LastLeader; i <= len(ck.servers); i++ {
		i %= len(ck.servers)
		_, _ = DPrintf("client send GET, TaskID:%v key:%v \n", args.TaskID, key)

		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)

		if ok && reply.Err == OK {
			_, _ = DPrintf("KVServer Get: %v\n", reply.Value)
			ck.LastLeader = i
			ck.LastTaskID++

			return reply.Value
		}
	}
	// You will have to modify this function.
	return ""
}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		TaskID:   ck.LastTaskID,
		ClientID: ck.ID,
		Key:      key,
		Value:    value,
		Op:       op,
	}
	reply := PutAppendReply{}

	for i := ck.LastLeader; i <= len(ck.servers); i++ {
		i %= len(ck.servers)
		_, _ = raft.DPrintf("client send:%v, TaskID:%v key:%v value%v \n", op, args.TaskID, key, value)

		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)

		if ok && reply.Err == OK {
			_, _ = DPrintf("KVServer Put successful\n")
			ck.LastLeader = i
			_, _ = DPrintf("client 记录leader为:%d \n", ck.LastLeader)
			ck.LastTaskID++

			return
		} else {
			_, _ = DPrintf("KVServer ok:%v err:%v\n", ok, reply.Err)
			reply.Err = "" //清空err
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
