package sraft

import (
	"math/rand"
	"time"
)

const (
	minElectionTimeoutVal = 150
	maxElectionTimeoutVal = 300
	heartbeatTimeoutDur   = time.Millisecond * (minElectionTimeoutVal / 2)
)

func getRandElectionTimeout() time.Duration {
	timeout := rand.Intn(maxElectionTimeoutVal-minElectionTimeoutVal) + minElectionTimeoutVal
	return time.Millisecond * time.Duration(timeout)
}
