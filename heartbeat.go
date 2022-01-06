package sraft

import "encoding/binary"

type heartbeat struct {
	index   *uint64  // The index that the entries should be appended after.
	term    uint64   // Term of the sender.
	entries [][]byte // Entries from the sender to append to receiver's commit log.
}

const (
	tagHeartbeat = 3
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

	binary.LittleEndian.PutUint64(b[1:9], *hb.index)
	binary.LittleEndian.PutUint64(b[9:17], hb.term)

	startIndex := 17
	for _, entry := range hb.entries {
		l := len(entry)
		b[startIndex] = byte(l - 1)

		startIndex++
		endIndex := l + startIndex

		sec := b[startIndex:endIndex]
		for i := 0; i < l; i++ {
			sec[i] = entry[i]
		}
		startIndex = endIndex
	}

	return b, nil
}
