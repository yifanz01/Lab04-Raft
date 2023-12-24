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
