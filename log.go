package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type TimedLog struct {
	t   time.Time
	log string
}

type TimedLogSlice []TimedLog

func (p TimedLogSlice) Len() int           { return len(p) }
func (p TimedLogSlice) Less(i, j int) bool { return p[i].t.Before(p[j].t) }
func (p TimedLogSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

var logs []TimedLog
var logM sync.Mutex

func logInput(name string, input int64) {
	log(fmt.Sprintf("%s: %d", name, input))
}

func logOpen(name string) {
	log(name + ": OPEN")
}

func logClose(name string) {
	log(name + ": CLOSE")
}

func log(s string) {
	logM.Lock()
	logs = append(logs, TimedLog{time.Now(), s})
	logM.Unlock()
}

func FlushLogs() {
	logM.Lock()
	timedLogSlice := TimedLogSlice(logs)
	sort.Sort(timedLogSlice)
	for i := range timedLogSlice {
		fmt.Println(timedLogSlice[i].log)
	}
	logs = make([]TimedLog, 0, 0)
	logM.Unlock()
}
