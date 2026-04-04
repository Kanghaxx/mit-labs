package raft

import "log"

// Debugging
const Debug = false
const DebugImp = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func DPrintfImp(format string, a ...interface{}) {
	if DebugImp {
		log.Printf(format, a...)
	}
}
