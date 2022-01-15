package sraft

import (
	"fmt"
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
	id              int
	matchIndex      int
	rollback        bool
	term            uint64
	vote            uint64
	peers           []*peer
	followers       []*follower
	commit          Commit
	log             Log // Manages all log entries that have been committed
	state           State
	isStarted       bool
	electionTimeout *time.Timer
	commitMut       *sync.Mutex
	termMut         *sync.Mutex
	errCh           chan error
	stopCh          chan bool
	stopWG          *sync.WaitGroup
}

func newNode(config *nodeConfig) *node {
	peerLen := len(config.peers)
	followers := make([]*follower, peerLen)
	for i := 0; i < peerLen; i++ {
		followers[i] = newFollower()
	}
	return &node{
		id:        config.id,
		peers:     config.peers,
		followers: followers,
		state:     Follower,
		log:       config.log,
		commit:    config.commit,
		stopCh:    make(chan bool),
		stopWG:    &sync.WaitGroup{},
		commitMut: &sync.Mutex{},
		termMut:   &sync.Mutex{},
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

// getFollower gets a follower by its index.
// If the follower is not found an error will be returned.
func (n *node) getFollower(peerIndex int) (*follower, error) {
	follower := n.followers[peerIndex]
	if follower == nil {
		return nil, errFollowerNotFound
	}
	return follower, nil
}

func (n *node) resetElectionTimeout() {
	if n.electionTimeout != nil {
		n.electionTimeout.Stop()
	}
	n.electionTimeout = time.AfterFunc(getRandElectionTimeout(), func() {
		err := n.advertiseCandidacy()
		if err != nil {
			n.errCh <- err
		}
	})
}

func (n *node) resetHeartbeatTimeout(followerIndex int) error {
	follower, err := n.getFollower(followerIndex)
	if err != nil {
		return err
	}
	if follower.heartbeatTimeout != nil {
		follower.heartbeatTimeout.Stop()
	}
	follower.heartbeatTimeout = time.AfterFunc(heartbeatTimeoutDur, func() {
		err := n.sendHeartbeatToFollower(followerIndex, -1)
		if err != nil {
			n.errCh <- err
		}
	})
	return nil
}

// clearCommit clears the current commit.
func (n *node) clearCommit() {
	n.commitMut.Lock()
	defer n.commitMut.Unlock()
	n.commit.Shift(n.commit.Length())
}

// stopHeartbeatTimeouts stops all heartbeat timeouts for followers.
func (n *node) stopHeartbeatTimeouts() {
	for _, follower := range n.followers {
		if follower.heartbeatTimeout != nil {
			follower.heartbeatTimeout.Stop()
		}
	}
}

// acceptVote accepts a vote from a peer.
func (n *node) acceptVote(vote *vote) error {
	n.termMut.Lock()
	defer n.termMut.Unlock()

	if vote.term < n.term {
		return fmt.Errorf("failed to accept vote: %w", errStaleTerm)
	}

	if n.state != Candidate {
		return fmt.Errorf("failed to accept vote: %w", errNotCandidate)
	}

	n.vote++
	if isMajority(len(n.peers)+1, int(n.vote)) {
		n.electionTimeout.Stop()

		n.state = Leader
		n.vote = 0

		commitIndex := n.commit.Length()
		for peerIndex, follower := range n.followers {
			follower.nextIndex = commitIndex
			n.sendHeartbeatToFollower(peerIndex, -1)
		}
	} else {
		n.resetElectionTimeout()
	}

	return nil
}

// voteForCandidate votes for a peer in the candidate state.
func (n *node) voteForCandidate(peerIndex int, election *election) error {
	peer, err := n.getPeer(peerIndex)
	if err != nil {
		return err
	}

	n.termMut.Lock()
	defer n.termMut.Unlock()

	if n.term >= election.term {
		return fmt.Errorf("failed to vote for candidate: %w", errStaleTerm)
	}

	if n.state == Leader {
		n.stopHeartbeatTimeouts()
	}

	n.state = Follower
	n.rollback = true

	newTerm := election.term
	n.term = newTerm

	n.resetElectionTimeout()

	commitIndex := n.log.Length()
	if int(election.commitIndex) >= commitIndex {
		go func() {
			peer.out.vote <- &vote{term: newTerm}
		}()
	}

	return nil
}

// advertiseCandidacy advertises this node as a candidate for leader election to its peers.
func (n *node) advertiseCandidacy() error {
	n.termMut.Lock()
	defer n.termMut.Unlock()

	if n.state == Leader {
		return errIsLeader
	}

	n.resetElectionTimeout()

	newTerm := n.term + 1
	n.term = newTerm

	n.vote = 1
	n.state = Candidate

	commitIndex := uint64(n.log.Length())
	for _, p := range n.peers {
		go func(peer *peer) {
			peer.out.election <- &election{term: newTerm, commitIndex: commitIndex}
		}(p)
	}

	return nil
}

// newHeartbeatForFollower contructs a new heartbeat for a follower at a given match index.
// If `followerMatchIndex` is less than 0, a heartbeat without any entries will be sent.
// Otherwise, all of the missing entries starting from `followerMatchIndex` will be sent.
func (n *node) newHeartbeatForFollower(followerMatchIndex int) *heartbeat {
	if followerMatchIndex >= 0 {
		n.commitMut.Lock()
		defer n.commitMut.Unlock()
	}

	var entries [][]byte
	commitIndex := n.log.Length()
	uCommitIndex := uint64(commitIndex)
	prevIndex := uCommitIndex

	if followerMatchIndex >= 0 {
		leaderMatchIndex := commitIndex + n.commit.Length()
		if followerMatchIndex < leaderMatchIndex {
			prevIndex = uint64(followerMatchIndex)
			size := leaderMatchIndex - followerMatchIndex

			var entry []byte
			entries = make([][]byte, size)
			for i := 0; i < size; i++ {
				index := i + followerMatchIndex
				if index < commitIndex {
					entry = n.log.Get(index)
				} else {
					entry = n.commit.Get(index - commitIndex)
				}
				entries[i] = entry
			}
		}
	}

	return &heartbeat{
		commitIndex: uCommitIndex,
		prevIndex:   prevIndex,
		entries:     entries,
		term:        n.term,
	}
}

// sendHeartbeatToFollower sends a heartbeat to a follower identified by `peerIndex`.
// The entries in the heartbeat are based upon the provided `followerMatchIndex`.
// If `followerMatchIndex` is less than 0, a heartbeat without any entries will be sent.
func (n *node) sendHeartbeatToFollower(peerIndex, followerMatchIndex int) error {
	peer, err := n.getPeer(peerIndex)
	if err != nil {
		return err
	}
	follower, err := n.getFollower(peerIndex)
	if err != nil {
		return err
	}

	n.resetHeartbeatTimeout(peerIndex)
	follower.hasRespondedToHeartbeat = false

	go func() {
		peer.out.heartbeat <- n.newHeartbeatForFollower(followerMatchIndex)
	}()

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
	_, err := n.getPeer(peerIndex)
	if err != nil {
		return err
	}
	follower, err := n.getFollower(peerIndex)
	if err != nil {
		return err
	}

	n.termMut.Lock()
	defer n.termMut.Unlock()

	if hb.term < n.term {
		return errStaleTerm
	}
	if n.state != Leader {
		return ErrNotLeader
	}

	follower.hasRespondedToHeartbeat = true

	followerMatchIndex := int(hb.matchIndex)
	if hb.success || followerMatchIndex < follower.nextIndex {
		follower.nextIndex = followerMatchIndex
	}

	if followerMatchIndex < n.matchIndex {
		err = n.sendHeartbeatToFollower(peerIndex, followerMatchIndex)
		if err != nil {
			return err
		}
	}

	if !hb.success {
		return nil
	}

	n.commitMut.Lock()
	defer n.commitMut.Unlock()

	// Check if a consensus has been reached. If so, shift commit entries into log.
	commitIndex := n.log.Length()
	n.matchIndex = commitIndex + n.commit.Length()
	followerUncommittedIndex := followerMatchIndex - commitIndex
	if followerUncommittedIndex > 0 {
		indexes := make([]int, len(n.followers)+1)
		indexes[0] = n.matchIndex
		for i := 0; i < len(n.followers); i++ {
			indexes[i+1] = n.followers[i].nextIndex
		}

		consensusIndex := getConsensusIndex(indexes)
		consensusUncommittedIndex := consensusIndex - commitIndex
		if consensusUncommittedIndex > 0 {
			n.log.Append(n.commit.Shift(consensusUncommittedIndex)...)
		}
	}

	return nil
}

// handleHeartbeatFromLeader handles the heartbeat received from a leader.
func (n *node) handleHeartbeatFromLeader(peerIndex int, hb *heartbeat) error {
	peer, err := n.getPeer(peerIndex)
	if err != nil {
		return err
	}

	n.termMut.Lock()
	defer n.termMut.Unlock()

	if hb.term < n.term {
		return fmt.Errorf("failed to handle heartbeat from leader: %w", errStaleTerm)
	}
	if n.state == Leader {
		return errIsLeader
	}

	n.resetElectionTimeout()

	prevIndex := int(hb.prevIndex)

	entriesLen := len(hb.entries)
	if entriesLen != 0 && n.rollback {
		n.rollback = false
		n.clearCommit()
	}

	n.commitMut.Lock()
	defer n.commitMut.Unlock()

	success := false
	commitIndex := n.log.Length()
	uncommittedIndex := n.commit.Length()
	followerMatchIndex := commitIndex + uncommittedIndex

	if entriesLen != 0 {
		startIndex := followerMatchIndex - prevIndex
		if startIndex >= 0 {
			newEntries := hb.entries[startIndex:]
			lenEntries := len(newEntries)
			if lenEntries != 0 {
				n.commit.Append(newEntries...)
				followerMatchIndex += lenEntries
				success = true
			}
		}
	}

	commitDiff := int(hb.commitIndex) - commitIndex
	if commitDiff > 0 {
		shiftIndex := commitDiff
		if uncommittedIndex < commitDiff {
			shiftIndex = uncommittedIndex
		}
		if shiftIndex != 0 {
			n.log.Append(n.commit.Shift(shiftIndex)...)
		}
	}

	uFollowerMatchIndex := uint64(followerMatchIndex)
	go func() {
		peer.out.heartbeat <- &heartbeat{
			matchIndex: uFollowerMatchIndex,
			term:       hb.term,
			success:    success,
		}
	}()
	return nil
}

func (n *node) handleHeartbeat(peerIndex int, hb *heartbeat) error {
	_, err := n.getPeer(peerIndex)
	if err != nil {
		return err
	}

	n.termMut.Lock()

	if hb.term < n.term {
		return fmt.Errorf("failed to handle heartbeat: %w", errStaleTerm)
	}

	if hb.term > n.term {
		n.term = hb.term
		if n.state == Leader {
			n.stopHeartbeatTimeouts()
			n.resetElectionTimeout()
		}
		n.state = Follower
		n.rollback = true
	}

	n.termMut.Unlock()

	if n.state == Leader {
		return n.handleHeartbeatFromFollower(peerIndex, hb)
	}
	return n.handleHeartbeatFromLeader(peerIndex, hb)
}

func (n *node) appendEntries(entry []byte) error {
	n.termMut.Lock()
	defer n.termMut.Unlock()

	if n.state != Leader {
		return ErrNotLeader
	}

	n.commitMut.Lock()
	defer n.commitMut.Unlock()

	n.commit.Append(entry)

	// Send new entries to followers immediately if the leader is
	// not waiting on a heartbeat response from follower.
	for i := 0; i < len(n.followers); i++ {
		follower := n.followers[i]
		if follower.hasRespondedToHeartbeat {
			err := n.sendHeartbeatToFollower(i, follower.nextIndex)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *node) start() <-chan error {
	if n.isStarted {
		return n.errCh
	}
	n.isStarted = true

	n.errCh = make(chan error, 100)
	n.stopCh = make(chan bool)
	n.stopWG.Add(len(n.peers))

	for i := 0; i < len(n.peers); i++ {
		var err error
		go func(index int) {
			peer := n.peers[index]
			for n.isStarted {
				select {
				case hb := <-peer.in.heartbeat:
					err = n.handleHeartbeat(index, hb)
				case v := <-peer.in.vote:
					err = n.acceptVote(v)
				case e := <-peer.in.election:
					err = n.voteForCandidate(index, e)
				case <-n.stopCh:
					break
				}
				if err != nil {
					fmt.Println(err)
					n.errCh <- err
					err = nil
				}
			}
			n.stopWG.Done()
		}(i)
	}

	n.resetElectionTimeout()
	return n.errCh
}

func (n *node) stop() {
	if !n.isStarted {
		return
	}
	n.isStarted = false

	n.stopHeartbeatTimeouts()

	close(n.stopCh)
	n.stopWG.Wait()
	close(n.errCh)

	n.clearCommit()
	n.state = Follower
	n.rollback = false
}
