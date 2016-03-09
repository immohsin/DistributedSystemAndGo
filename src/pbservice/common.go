package pbservice

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
	// By Yan
	Op      string
	Id      int64
	AckedId int64
	Me      string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	// By Yan
	Id      int64
	AckedId int64
	Me      string
}

type GetReply struct {
	Err   Err
	Value string
	Me    string
}

// Your RPC definitions here.
// By Yan
type ReplicateAllArgs struct {
	Me              string
	DB              map[string]string
	LastPutAppendId map[int64]time.Time
	//LastForwardPutAppendId map[int64]time.Time
	//LastReplicateAllId     map[int64]time.Time
	Id      int64
	AckedId int64
}

type ReplicateAllReply struct {
	Err Err
}

type GetRes struct {
	Succeed bool
	Reply   GetReply
}
