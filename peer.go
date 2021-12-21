package sraft

type heartbeat struct {
	index   uint64
	term    uint64
	entries [][]byte
}

type peerIn struct {
	vote      <-chan bool       // Channel for receiving votes for a candidate.
	election  <-chan uint64     // Channel for receiving advertisements of candidicy. Accepts term number.
	heartbeat <-chan *heartbeat // Channel for receiving heartbeats from leader.
}

type peerOut struct {
	vote      chan bool       // Channel for sending votes for a candidate.
	election  chan uint64     // Channel for sending advertisements of candidicy. Accepts term number.
	heartbeat chan *heartbeat // Channel for sending heartbeats from leader.
}

type peer struct {
	in  peerIn  // Read messages from peer.
	out peerOut // Write messages to peer.
}
