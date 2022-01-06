package sraft

import (
	"math"
	"sync"
)

// isMajority checks if a integer is greater than half another integer.
func isMajority(len, num int) bool {
	return num > int(math.Floor(float64(len)/2))
}

// consensusMan keeps track of the commit index that followers have reached a consensus on.
type consensusMan struct {
	index float64
	count int
	cache []bool
	mut   *sync.Mutex
}

// newConsensusMan creates a new consensus manager.
func newConsensusMan(peerLen int) *consensusMan {
	return &consensusMan{
		count: 1,
		mut:   &sync.Mutex{},
		index: math.Inf(1),
		cache: make([]bool, peerLen),
	}
}

// check if a consensus has been reached by followers up to a specific leader commit index.
//
// This function accepts two parameters:
//    peerIndex identifies the peer.
//    commitIndex = (peer log length + peer commit length) - (leader log length)
//
// This function returns -1 is a consensus has not yet been reached.
// Otherwise, it returns a value greater than 0 that represents the commit index in which followers have reached a consensus on.
func (c *consensusMan) check(peerIndex, commitIndex int) int {
	c.mut.Lock()
	defer c.mut.Unlock()

	fCommitIndex := float64(commitIndex)
	if c.index > fCommitIndex {
		c.index = fCommitIndex
	}
	if !c.cache[peerIndex] {
		c.cache[peerIndex] = true
		c.count++
	}
	if isMajority(len(c.cache)+1, c.count) {
		consensusIndex := c.index
		c.cache = make([]bool, len(c.cache))
		c.count = 1
		c.index = math.Inf(1)
		return int(consensusIndex)
	}
	return -1
}
