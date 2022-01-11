package sraft

const (
	Follower  = 0 // Follower dentoes that the node is following a leader node.
	Candidate = 1 // Candidate dentoes that the node is a candidate to become a leader node.
	Leader    = 2 // Leader dentoes that the node is a leader.
)
