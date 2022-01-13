package sraft

// Enumerator for node states.
type State = byte

const (
	Follower  State = 0 // Follower dentoes that the node is following a leader node.
	Candidate State = 1 // Candidate dentoes that the node is a candidate to become a leader node.
	Leader    State = 2 // Leader dentoes that the node is a leader.
)
