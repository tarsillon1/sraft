package sraft

import "errors"

var (
	ErrEntryTooLarge = errors.New("entry must be less than 256 bytes")
	ErrNotLeader     = errors.New("not in leader state")
)

var (
	errInterrupt        = errors.New("operation was interrupted")
	errIsLeader         = errors.New("operation cannot be executed by a node in leader state")
	errPeerNotFound     = errors.New("peer not found")
	errFollowerNotFound = errors.New("follower not found")
	errStaleTerm        = errors.New("received message had stale term")
	errNotCandidate     = errors.New("not in candidate state")
)
