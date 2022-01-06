package sraft

type peerIO struct {
	vote      chan bool       // Channel for sending/receiving votes for a candidate.
	election  chan uint64     // Channel for sending/receiving advertisements of candidicy. Accepts term number.
	heartbeat chan *heartbeat // Channel for sending/receiving heartbeats from leader.
}

type peer struct {
	in  *peerIO // Read messages from peer.
	out *peerIO // Write messages to peer.
}

func newPeer() *peer {
	return &peer{
		in: &peerIO{
			vote:      make(chan bool),
			election:  make(chan uint64),
			heartbeat: make(chan *heartbeat),
		},
		out: &peerIO{
			vote:      make(chan bool),
			election:  make(chan uint64),
			heartbeat: make(chan *heartbeat),
		},
	}
}
