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
	id               int
	term             uint64
	vote             uint64
	peers            []*peer
	followers        []*follower
	commit           Commit
	log              Log // Manages all log entries that have been committed
	state            int
	consensus        *consensusMan
	isStarted        bool
	electionDeadline int64
	errCh            chan error
	stopCh           chan bool
	stopWG           *sync.WaitGroup
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
		consensus: newConsensusMan(len(config.peers)),
		log:       config.log,
		commit:    config.commit,
		stopCh:    make(chan bool),
		stopWG:    &sync.WaitGroup{},
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

func (n *node) nextElectionDeadline() {
	n.electionDeadline = time.Now().Add(getRandElectionTimeout()).UnixMilli()
	fmt.Printf("%d deadline %d \n", n.id, n.electionDeadline)
}

// clearCommit clears the current commit.
func (n *node) clearCommit() {
	n.commit.Shift(n.commit.Length())
}

// resetFollowers sets all in memory follower data back to its initial state.
func (n *node) resetFollowers() {
	for _, follower := range n.followers {
		follower.lastMatchIndex = -1
		follower.hasRespondedToHeartbeat = true
	}
}

// acceptVote accepts a vote from a peer.
func (n *node) acceptVote(vote *vote) error {
	if n.state != Candidate {
		return errNotCandidate
	}

	if *vote.term < n.term {
		return errStaleTerm
	}

	n.nextElectionDeadline()

	n.vote++
	if isMajority(len(n.peers)+1, int(n.vote)) {
		fmt.Printf("%d new leader %d \n", n.id, time.Now().UnixMilli())

		n.state = Leader
		n.vote = 0

		for peerIndex := 0; peerIndex < len(n.peers); peerIndex++ {
			n.sendHeartbeatToFollower(peerIndex, -1)
		}
	}
	return nil
}

// voteForCandidate votes for a peer in the candidate state.
func (n *node) voteForCandidate(peerIndex int, election *election) error {
	if n.term >= *election.term {
		return errStaleTerm
	}

	peer, err := n.getPeer(peerIndex)
	if err != nil {
		return err
	}

	if n.state == Leader {
		n.resetFollowers()
	}
	n.term = *election.term
	n.state = Follower

	n.nextElectionDeadline()

	commitIndex := n.log.Length()
	if int(*election.commitIndex) >= commitIndex {
		go func() {
			peer.out.vote <- &vote{term: &n.term}
		}()
	}

	return nil
}

// advertiseCandidacy advertises this node as a candidate for leader election to its peers.
func (n *node) advertiseCandidacy() error {
	if n.state == Leader {
		return errIsLeader
	}

	fmt.Printf("%d advertise %d \n", n.id, time.Now().UnixMilli())
	n.nextElectionDeadline()

	n.term++

	n.vote = 1
	n.state = Candidate

	term := n.term
	commitIndex := uint64(n.log.Length())
	for _, p := range n.peers {
		go func(peer *peer) {
			peer.out.election <- &election{term: &term, commitIndex: &commitIndex}
		}(p)
	}

	return nil
}

// newHeartbeatForFollower contructs a new heartbeat for a follower at a given match index.
// If `followerMatchIndex` is less than 0, a heartbeat without any entries will be sent.
// Otherwise, all of the missing entries starting from `followerMatchIndex` will be sent.
func (n *node) newHeartbeatForFollower(followerMatchIndex int) *heartbeat {
	var entries [][]byte
	commitIndex := n.log.Length()
	uCommitIndex := uint64(commitIndex)
	prevIndex := uCommitIndex
	leaderMatchIndex := commitIndex + n.commit.Length()

	if followerMatchIndex >= 0 && followerMatchIndex < leaderMatchIndex {
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

	follower.nextHearbeatDeadline()
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

	if n.state != Leader {
		return errNotLeader
	}
	if hb.term < n.term {
		return errStaleTerm
	}

	commitIndex := n.log.Length()

	followerMatchIndex := int(hb.matchIndex)

	// Check if a consensus has been reached. If so, shift commit entries into log.
	uncommittedIndex := followerMatchIndex - commitIndex
	if uncommittedIndex > 0 {
		consensusIndex := n.consensus.check(peerIndex, uncommittedIndex)
		if consensusIndex > 0 {
			n.log.Append(n.commit.Shift(consensusIndex)...)
		}
	}

	follower, err := n.getFollower(peerIndex)
	if err != nil {
		return err
	}

	follower.lastMatchIndex = followerMatchIndex

	// Check if the follower is missing entries. If so, send heartbeat with missing entries ASAP.
	leaderMatchIndex := commitIndex + n.commit.Length()
	if followerMatchIndex < leaderMatchIndex {
		return n.sendHeartbeatToFollower(peerIndex, followerMatchIndex)
	}

	follower.hasRespondedToHeartbeat = true

	return nil
}

// handleHeartbeatFromLeader handles the heartbeat received from a leader.
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

	fmt.Printf("%d next %d \n", n.id, time.Now().UnixMilli())
	n.nextElectionDeadline()

	commitIndex := n.log.Length()
	uncommittedIndex := n.commit.Length()

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

	entriesLen := len(hb.entries)
	followerMatchIndex := commitIndex + uncommittedIndex
	if entriesLen != 0 {
		startIndex := followerMatchIndex - int(hb.prevIndex)
		if startIndex >= 0 {
			newEntries := hb.entries[startIndex:]
			followerMatchIndex += len(newEntries)
			n.commit.Append(newEntries...)
		}
	}

	go func() {
		uFollowerMatchIndex := uint64(followerMatchIndex)
		peer.out.heartbeat <- &heartbeat{
			matchIndex: uFollowerMatchIndex,
			term:       hb.term,
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

	if hb.term > n.term {
		n.term = hb.term
		if n.state == Leader {
			n.resetFollowers()
			n.nextElectionDeadline()
		}
		n.clearCommit()
		n.state = Follower
	}

	if n.state == Leader {
		return n.handleHeartbeatFromFollower(peerIndex, hb)
	}
	return n.handleHeartbeatFromLeader(peerIndex, hb)
}

func (n *node) appendEntries(entry []byte) {
	n.commit.Append(entry)

	// Send new entries to followers immediately if the leader is
	// not waiting on a heartbeat response from follower.
	for i := 0; i < len(n.followers); i++ {
		follower := n.followers[i]
		if follower.hasRespondedToHeartbeat {
			n.sendHeartbeatToFollower(i, follower.lastMatchIndex)
		}
	}
}

func (n *node) start() <-chan error {
	if n.isStarted {
		return n.errCh
	}
	n.isStarted = true

	n.stopCh = make(chan bool)
	n.stopWG.Add(len(n.peers) + 1)

	vtCh := make(chan struct {
		int
		*vote
	})
	hbCh := make(chan struct {
		int
		*heartbeat
	})
	elCh := make(chan struct {
		int
		*election
	})

	for i := 0; i < len(n.peers); i++ {
		go func(index int) {
			peer := n.peers[index]
			for n.isStarted {
				select {
				case hb := <-peer.in.heartbeat:
					hbCh <- struct {
						int
						*heartbeat
					}{index, hb}
				case v := <-peer.in.vote:
					vtCh <- struct {
						int
						*vote
					}{index, v}
				case e := <-peer.in.election:
					elCh <- struct {
						int
						*election
					}{index, e}
				case <-n.stopCh:
				}
			}
			n.stopWG.Done()
		}(i)
	}

	go func() {
		n.nextElectionDeadline()
		for n.isStarted {
			var err error
			t := time.NewTimer(heartbeatTimeoutDur)

			select {
			case h := <-hbCh:
				err = n.handleHeartbeat(h.int, h.heartbeat)
			case v := <-vtCh:
				err = n.acceptVote(v.vote)
			case e := <-elCh:
				err = n.voteForCandidate(e.int, e.election)
			case <-n.stopCh:
				return
			case <-t.C:
				if n.state == Leader {
					for i, follower := range n.followers {
						if follower.heartbeatDeadline < time.Now().UnixMilli() {
							shtfErr := n.sendHeartbeatToFollower(i, -1)
							n.errCh <- shtfErr
						}
					}
				} else if n.electionDeadline < time.Now().UnixMilli() {
					err = n.advertiseCandidacy()
				}
			}

			t.Stop()
			if err != nil {
				n.errCh <- err
			}
		}
		n.stopWG.Done()
	}()

	return n.errCh
}

func (n *node) stop() {
	if !n.isStarted {
		return
	}
	n.isStarted = false

	close(n.stopCh)
	n.stopWG.Wait()

	n.state = Follower
	n.clearCommit()
	n.resetFollowers()
}
