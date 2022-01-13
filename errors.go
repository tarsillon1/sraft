package sraft

import "errors"

var (
	ErrEntryTooLarge = errors.New("entry must be less than 256 bytes")
	ErrNotLeader     = errors.New("operation cannot be executed by a node in follower or candidate state")
)

var (
	errInterrupt        = errors.New("operation was interrupted")
	errIsLeader         = errors.New("operation cannot be executed by a node in leader state")
	errPeerNotFound     = errors.New("peer not found")
	errFollowerNotFound = errors.New("follower not found")
	errStaleTerm        = errors.New("operation not permitted as received message from peer contained a stale term")
	errNotCandidate     = errors.New("operation cannot be executed by a node in follower or leader state")
)
