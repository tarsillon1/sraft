package sraft

import (
	"sync"
	"time"
)

type nodeConfig struct {
	id     int
	log    Log
	commit Commit
	peers  []*peer
}

type node struct {
	id                int
	term              uint64
	vote              uint64
	peers             []*peer
	termMut           *sync.Mutex
	voteMut           *sync.Mutex
	commitMut         *sync.Mutex
	electionTimeout   *time.Timer
	heartbeatTimeouts []*time.Timer
	commit            Commit
	log               Log // Manages all log entries that have been committed
	state             State
	consensus         *consensusMan
	isStarted         bool
	interrupt         chan bool
	interruptWFET     chan bool
}

func newNode(config *nodeConfig) *node {
	peerLen := len(config.peers)
	heartbeatTimeouts := make([]*time.Timer, peerLen)
	for i := 0; i < peerLen; i++ {
		timer := time.NewTimer(heartbeatTimeoutDur)
		timer.Stop()
		heartbeatTimeouts[i] = timer
	}
	return &node{
		id:                config.id,
		peers:             config.peers,
		electionTimeout:   time.NewTimer(getRandElectionTimeout()),
		heartbeatTimeouts: heartbeatTimeouts,
		state:             Follower,
		interrupt:         make(chan bool),
		interruptWFET:     make(chan bool),
		termMut:           &sync.Mutex{},
		voteMut:           &sync.Mutex{},
		commitMut:         &sync.Mutex{},
		consensus:         newConsensusMan(len(config.peers)),
		log:               config.log,
		commit:            config.commit,
	}
}

// getPeer gets a peer by its index.
// If the peer is not found an error will be returned.
func (n *node) getPeer(peerIndex int) (*peer, error) {
	peer := n.peers[peerIndex]
	if peer == nil {
		return nil, errPeerNotFound
	}
	return peer, nil
}

// clearCommit clears the current commit.
func (n *node) clearCommit() {
	n.commitMut.Lock()
	defer n.commitMut.Unlock()
	n.commit.Shift(n.commit.Length())
}

// getMatchIndex is the index of the highest known log index to be replicated on the server.
// This is calculated by adding the length of the known committed and uncommitted log entries.
func (n *node) getMatchIndex() int {
	return n.log.Length() + n.commit.Length()
}

// resetElectionTimeout resets the election timeout.
func (n *node) resetElectionTimeout() {
	n.electionTimeout.Reset(getRandElectionTimeout())
}

// resetHeartbeatTimeouts resets all heartbeat timeouts.
func (n *node) resetHeartbeatTimeouts() {
	for _, t := range n.heartbeatTimeouts {
		t.Reset(heartbeatTimeoutDur)
	}
}

// stopHeartbeatTimeouts stops all heartbeat timeouts.
func (n *node) stopHeartbeatTimeouts() {
	for _, t := range n.heartbeatTimeouts {
		t.Stop()
	}
}

// stopElectionTimeout stops the election timeout and interrupts any threads waiting for an election timeout.
func (n *node) stopElectionTimeout() {
	n.electionTimeout.Stop()

	interruptWFET := n.interruptWFET
	n.interruptWFET = make(chan bool)
	close(interruptWFET)
}

// waitForElectionTimeout waits for the election timer to timeout.
// The method will return an interrupt error if the election timer is stopped while waiting for it to timeout.
func (n *node) waitForElectionTimeout() error {
	select {
	case <-n.electionTimeout.C:
		return nil
	case <-n.interruptWFET:
	case <-n.interrupt:
	}
	return errInterrupt
}

// advertiseCandidacyOnElectionTimeout advertises candidacy to peers when the election timer times out.
func (n *node) advertiseCandidacyOnElectionTimeout() error {
	err := n.waitForElectionTimeout()
	if err != nil {
		return err
	}
	return n.advertiseCandidacy()
}

// acceptVote accepts a vote from a peer.
func (n *node) acceptVote() error {
	if n.state != Candidate {
		return errNotCandidate
	}

	n.voteMut.Lock()
	defer n.voteMut.Unlock()

	n.vote++
	if isMajority(len(n.peers)+1, int(n.vote)) {
		n.resetHeartbeatTimeouts()
		n.state = Leader
		n.vote = 0
		n.stopElectionTimeout()
	}
	return nil
}

// voteForCandidate votes for a peer in the candidate state.
func (n *node) voteForCandidate(peerIndex int, term uint64) error {
	if n.term >= term {
		return errStaleTerm
	}

	peer, err := n.getPeer(peerIndex)
	if err != nil {
		return err
	}

	n.termMut.Lock()
	defer n.termMut.Unlock()

	if n.state == Leader {
		n.clearCommit()
		n.stopHeartbeatTimeouts()
		go n.advertiseCandidacyOnElectionTimeout()
	}
	n.term = term
	n.state = Follower
	n.resetElectionTimeout()

	go func() {
		peer.out.vote <- true
	}()
	return nil
}

// advertiseCandidacy advertises this node as a candidate for leader election to its peers.
func (n *node) advertiseCandidacy() error {
	if n.state == Leader {
		return errIsLeader
	}

	n.termMut.Lock()
	defer n.termMut.Unlock()

	n.term++

	n.voteMut.Lock()
	n.vote = 1
	n.voteMut.Unlock()

	n.state = Candidate

	for _, p := range n.peers {
		go func(peer *peer) {
			peer.out.election <- n.term
		}(p)
	}

	return nil
}

// handleHeartbeatFromFollower handles a heartbeat received from a peer in the follower state.
//
// First this node will check if a consensus has been reached on a particular commit index.
// If a consensus has been reached, all entries up to said commit index will be removed and added to the log.
//
// Then the node will check if the peer in follower state is behind on any log entries.
// If it is, it will send over the log entries that the peer node is missing.
func (n *node) handleHeartbeatFromFollower(peerIndex int, hb *heartbeat) error {
	peer, err := n.getPeer(peerIndex)
	if err != nil {
		return err
	}
	if hb.term < n.term {
		return errStaleTerm
	}
	if n.state != Leader {
		return errNotLeader
	}

	followerMatchIndex := int(*hb.index)

	n.commitMut.Lock()
	defer n.commitMut.Unlock()

	logIndex := n.log.Length()
	commitIndex := followerMatchIndex - logIndex
	if commitIndex > 0 {
		consensusIndex := n.consensus.check(peerIndex, commitIndex)
		if consensusIndex > 0 {
			n.log.Append(n.commit.Shift(consensusIndex)...)
		}
	}

	leaderMatchIndex := n.getMatchIndex()
	if followerMatchIndex < leaderMatchIndex {
		var entry []byte
		entries := make([][]byte, 0)
		for i := followerMatchIndex; i < leaderMatchIndex; i++ {
			if i < logIndex {
				entry = n.log.Get(i)
			} else {
				entry = n.commit.Get(i - logIndex)
			}
			entries = append(entries, entry)
		}

		heartbeatTimeout := n.heartbeatTimeouts[peerIndex]
		heartbeatTimeout.Reset(heartbeatTimeoutDur)

		go func() {
			fromIndex := uint64(followerMatchIndex)
			peer.out.heartbeat <- &heartbeat{
				index:   &fromIndex,
				entries: entries,
				term:    n.term,
			}
		}()
	}

	return nil
}

func (n *node) handleHeartbeatFromLeader(peerIndex int, hb *heartbeat) error {
	peer, err := n.getPeer(peerIndex)
	if err != nil {
		return err
	}
	if hb.term < n.term {
		return errStaleTerm
	}
	if n.state == Leader {
		return errIsLeader
	}

	n.commitMut.Lock()

	n.resetElectionTimeout()

	commitLen := n.commit.Length()
	logDiff := int(*hb.index) - n.log.Length()
	if logDiff > 0 {
		shiftIndex := logDiff
		if commitLen < logDiff {
			shiftIndex = commitLen
		}
		n.log.Append(n.commit.Shift(shiftIndex)...)
		logDiff -= shiftIndex
		commitLen -= shiftIndex
	}

	if logDiff == 0 && hb.entries != nil && len(hb.entries) != 0 {
		n.commit.Append(hb.entries[commitLen:]...)
	}

	n.commitMut.Unlock()

	go func() {
		matchIndex := uint64(n.getMatchIndex())
		peer.out.heartbeat <- &heartbeat{
			index: &matchIndex,
			term:  hb.term,
		}
	}()
	return nil
}

func (n *node) handleHeartbeat(peerIndex int, hb *heartbeat) error {
	_, err := n.getPeer(peerIndex)
	if err != nil {
		return err
	}
	if hb.term < n.term {
		return errStaleTerm
	}

	n.termMut.Lock()

	if hb.term > n.term {
		n.term = hb.term
		if n.state == Leader {
			n.clearCommit()
			n.stopHeartbeatTimeouts()
			go n.advertiseCandidacyOnElectionTimeout()
		}
		n.state = Follower
	}

	n.termMut.Unlock()

	if n.state == Leader {
		return n.handleHeartbeatFromFollower(peerIndex, hb)
	}
	return n.handleHeartbeatFromLeader(peerIndex, hb)
}

func (n *node) handlePeer(i int) error {
	peer, err := n.getPeer(i)
	if err != nil {
		return err
	}

	heartbeatTimeout := n.heartbeatTimeouts[i]

	select {
	case <-peer.in.vote:
		return n.acceptVote()
	case term := <-peer.in.election:
		return n.voteForCandidate(i, term)
	case hb := <-peer.in.heartbeat:
		return n.handleHeartbeat(i, hb)
	case <-heartbeatTimeout.C:
		heartbeatTimeout.Reset(heartbeatTimeoutDur)
		go func() {
			logIndex := uint64(n.log.Length())
			peer.out.heartbeat <- &heartbeat{index: &logIndex, term: n.term}
		}()
	case <-n.interrupt:
		return errInterrupt
	}
	return nil
}

func (n *node) appendLog(entry []byte) {
	n.commitMut.Lock()
	defer n.commitMut.Unlock()
	n.commit.Append(entry)
}

func (n *node) start() {
	n.isStarted = true
	for i := 0; i < len(n.peers); i++ {
		go func(index int) {
			for index < len(n.peers) && n.isStarted {
				n.handlePeer(index)
			}
		}(i)
	}
	go n.advertiseCandidacyOnElectionTimeout()
}

func (n *node) stop() {
	n.isStarted = false
	n.state = Follower
	n.stopElectionTimeout()
	n.stopHeartbeatTimeouts()
	n.clearCommit()

	interrupt := n.interrupt
	n.interrupt = make(chan bool)
	close(interrupt)
}
