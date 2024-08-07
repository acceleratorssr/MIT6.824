package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = false

//const Debug = true

var (
	debugLog *log.Logger
)

func init() {
	file, err := os.Create("debug_raft.log")
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
