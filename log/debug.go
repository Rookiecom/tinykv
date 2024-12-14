package log

import (
	"fmt"
	"os"
	"time"
)

var (
	debugStart     time.Time
	debugVerbosity string
)

type logTopic string

const (
	DClient  logTopic = "CLNT"
	DCommit  logTopic = "CMIT"
	DDrop    logTopic = "DROP"
	DError   logTopic = "ERRO"
	DInfo    logTopic = "INFO"
	DLeader  logTopic = "LEAD"
	DLog     logTopic = "LOG1"
	DLog2    logTopic = "LOG2"
	DPersist logTopic = "PERS"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMR"
	DTrace   logTopic = "TRCE"
	DVote    logTopic = "VOTE"
	DWarn    logTopic = "WARN"
)

func init() {
	debugStart = time.Now()
	debugVerbosity = os.Getenv("RAFT_DEBUG")
}

func RaftLog(topic logTopic, format string, v ...any) {
	if debugVerbosity == "" {
		return
	}
	time := time.Since(debugStart).Microseconds() / 100
	Infof(fmt.Sprintf("raftStateMachine%07d %v ", time, topic)+format, v...)
}
