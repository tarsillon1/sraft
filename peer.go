package sraft

import (
	"time"
)

type peerIO struct {
	vote      chan *vote      // Channel for sending/receiving votes for a candidate.
	election  chan *election  // Channel for sending/receiving advertisements of candidicy. Accepts term number.
	heartbeat chan *heartbeat // Channel for sending/receiving heartbeats from leader.
}

type peer struct {
	in  *peerIO // Read messages from peer.
	out *peerIO // Write messages to peer.
}

func newPeer() *peer {
	return &peer{
		in: &peerIO{
			vote:      make(chan *vote),
			election:  make(chan *election),
			heartbeat: make(chan *heartbeat),
		},
		out: &peerIO{
			vote:      make(chan *vote),
			election:  make(chan *election),
			heartbeat: make(chan *heartbeat),
		},
	}
}

// follower is a helper struct used by a leader for caching information regarding a follower.
type follower struct {
	nextIndex               int         // An estimate of the next index that should be sent to the follower.
	hasRespondedToHeartbeat bool        // True if the follower has responded to the last heartbeat sent from the leader.
	heartbeatTimeout        *time.Timer // Timer for tracking when heartbeats should be sent to follower.
}

func newFollower() *follower {
	return &follower{
		hasRespondedToHeartbeat: true,
	}
}
