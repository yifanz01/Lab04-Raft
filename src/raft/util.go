package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) randomTimeout() time.Duration {
	return time.Duration(200+rand.Int31n(200)) * time.Millisecond // 200 - 400ms
}

func (rf *Raft) commitLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > len(rf.logs)-1 {
		log.Fatalf("[commitLog] Error\n")
	}

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandIndex: i + 1,
			Command:      rf.logs[i].Command,
			CommandValid: true,
		}
	}
	rf.lastApplied = rf.commitIndex
}
