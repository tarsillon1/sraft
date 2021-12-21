package sraft

import (
	"math"
	"math/rand"
	"time"
)

const (
	minElectionTimeout = 150
	maxElectionTimeout = 300
)

func getElectionTimeout() time.Duration {
	timeout := rand.Intn(maxElectionTimeout-minElectionTimeout) + minElectionTimeout
	return time.Millisecond * time.Duration(timeout)
}

type node struct {
	peers                      []*peer
	termWaitForElectionTimeout chan bool
	electionTimeout            *time.Timer
	heartbeatTimeout           *time.Timer
	commit                     [][]byte
	log                        Log
	term                       uint64
	voteCount                  int
	state                      State
}

func newNode() *node {
	return &node{
		electionTimeout: time.NewTimer(getElectionTimeout()),
		state:           Follower,
	}
}

func (n *node) resetElectionTimeout() {
	n.electionTimeout.Reset(getElectionTimeout())
}

func (n *node) termElectionTimeout() {
	n.electionTimeout.Stop()
	n.termWaitForElectionTimeout <- true
}

func (n *node) isMajority(num int) bool {
	return num > int(math.Floor(float64(len(n.peers))/2))
}

func (n *node) acceptVote() {
	if n.state != Candidate {
		return
	}
	n.voteCount++
	if n.isMajority(n.voteCount) {
		n.state = Leader
		n.voteCount = 0
		n.termElectionTimeout()
	}
}

func (n *node) voteForCandidate(index int, term uint64) {
	if n.term >= term {
		return
	}
	peer := n.peers[index]
	if peer == nil {
		return
	}
	if n.state == Leader {
		n.commit = make([][]byte, 0)
		go n.waitForElectionTimeout()
	}
	n.term = term
	n.state = Follower
	peer.out.vote <- true
	n.resetElectionTimeout()
}

func (n *node) advertiseCandidacy() {
	if n.state == Leader {
		return
	}
	n.term++
	n.voteCount = 1
	n.state = Candidate
	for _, peer := range n.peers {
		peer.out.election <- n.term
	}
	n.resetElectionTimeout()
	go n.waitForElectionTimeout()
}

func (n *node) acknowledgeHeartbeat(hb *heartbeat) {
	if n.state == Leader {
		if hb.term > n.term {
			n.state = Follower
			n.commit = make([][]byte, 0)
			go n.waitForElectionTimeout()
		}
	}
}

func (n *node) handlePeer(i int) {
	peer := n.peers[i]
	if peer == nil {
		return
	}
	select {
	case <-peer.in.vote:
		n.acceptVote()
	case term := <-peer.in.election:
		n.voteForCandidate(i, term)
	case hb := <-peer.in.heartbeat:
		n.acknowledgeHeartbeat(hb)
	}
}

func (n *node) appendLog(entry []byte) {
	n.commit = append(n.commit, entry)
}

func (n *node) waitForElectionTimeout() {
	select {
	case <-n.electionTimeout.C:
		n.advertiseCandidacy()
	case <-n.termWaitForElectionTimeout:
	}
}

func (n *node) loop() {
	for i := 0; i < len(n.peers); i++ {
		go func(index int) {
			for index < len(n.peers) {
				n.handlePeer(index)
			}
		}(i)
	}
}
