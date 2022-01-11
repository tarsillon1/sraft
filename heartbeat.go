package sraft

import "encoding/binary"

type heartbeat struct {
	commitIndex uint64   // The index of the highest log entry known to be committed.
	matchIndex  uint64   // The index of the highest log entry known to be replicated.
	prevIndex   uint64   // The index that the entries should be appended after.
	term        uint64   // Term of the sender.
	entries     [][]byte // Entries from the sender to append to receiver's commit log.
}

const (
	tagHeartbeat = 3
)

const (
	typeHeartbeatMatchOnly  = 0
	typeHeartbeatCommitOnly = 1
	typeHeartbeatCommitPrev = 2
)

const (
	heartbeatHeaderBytes   = 17
	heartbeatMaxEntryBytes = 256
)

func marshalHeartbeatPacket(hb *heartbeat) ([]byte, error) {
	entriesLen := 0
	for _, entry := range hb.entries {
		l := len(entry)
		if l > heartbeatMaxEntryBytes {
			return nil, ErrEntryTooLarge
		}
		entriesLen += l + 1
	}

	b := make([]byte, entriesLen+17)

	b[0] = tagHeartbeat

	var termIndex int

	if hb.commitIndex != 0 && hb.prevIndex != 0 {
		b[1] = typeHeartbeatCommitPrev
		binary.LittleEndian.PutUint64(b[2:10], hb.matchIndex)
		binary.LittleEndian.PutUint64(b[10:18], hb.prevIndex)
		termIndex = 18
	} else if hb.prevIndex == 0 {
		b[1] = typeHeartbeatCommitOnly
		binary.LittleEndian.PutUint64(b[2:10], hb.commitIndex)
		termIndex = 10
	} else {
		b[1] = typeHeartbeatMatchOnly
		binary.LittleEndian.PutUint64(b[2:10], hb.matchIndex)
		termIndex = 10
	}

	entriesIndex := termIndex + 8
	binary.LittleEndian.PutUint64(b[termIndex:entriesIndex], hb.term)

	for _, entry := range hb.entries {
		l := len(entry)
		b[entriesIndex] = byte(l - 1)

		entriesIndex++
		endIndex := l + entriesIndex

		sec := b[entriesIndex:endIndex]
		for i := 0; i < l; i++ {
			sec[i] = entry[i]
		}
		entriesIndex = endIndex
	}

	return b, nil
}
