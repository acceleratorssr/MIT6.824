package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	LastLeader int
	LastTaskID int64
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
	ck.LastLeader = 0
	ck.LastTaskID = 1
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
		ID:  ck.LastTaskID,
		Key: key,
	}
	reply := GetReply{}
	ck.LastTaskID++

	for i := ck.LastLeader; i <= len(ck.servers); i++ {
		i %= len(ck.servers)
		_, _ = DPrintf("client send GET, ID:%v key:%v \n", args.ID, key)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == "" {
			_, _ = DPrintf("KVServer Get: %v\n", reply.Value)
			ck.LastLeader = i

			return reply.Value
		} else {
			_, _ = DPrintf("KVServer ok:%v err:%v\n", ok, reply.Err)
			if reply.Err == "此请求已经被处理过了" {
				break
			}
			reply.Err = "" //清空err
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
	switch op {
	case "Put":
		args := PutAppendArgs{
			ID:    ck.LastTaskID,
			Key:   key,
			Value: value,
			Op:    "Put",
		}
		reply := PutAppendReply{}
		ck.LastTaskID++

		for i := ck.LastLeader; i <= len(ck.servers); i++ {
			i %= len(ck.servers)
			_, _ = DPrintf("client send PUT, ID:%v key:%v value%v \n", args.ID, key, value)
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err == "" {
				_, _ = DPrintf("KVServer Put successful\n")
				ck.LastLeader = i
				_, _ = DPrintf("client 记录leader为:%d \n", ck.LastLeader)

				return
			} else {
				_, _ = DPrintf("KVServer ok:%v err:%v\n", ok, reply.Err)
				if reply.Err == "此请求已经被处理过了" {
					break
				}
				reply.Err = "" //清空err
			}
		}
	case "Append":
		args := PutAppendArgs{
			ID:    ck.LastTaskID,
			Key:   key,
			Value: value,
			Op:    "Append",
		}
		reply := PutAppendReply{}
		ck.LastTaskID++

		for i := ck.LastLeader; i <= len(ck.servers); i++ {
			i %= len(ck.servers)
			_, _ = DPrintf("client send APPEND, ID:%v key:%v value%v \n", args.ID, key, value)
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err == "" {
				_, _ = DPrintf("KVServer Put successful\n")
				ck.LastLeader = i
				return
			} else {
				_, _ = DPrintf("KVServer ok:%v err:%v\n", ok, reply.Err)
				if reply.Err == "此请求已经被处理过了" {
					break
				}
				reply.Err = "" //清空err
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
