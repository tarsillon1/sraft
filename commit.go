package sraft

// Commit is the contract for a commit implementation.
// A commit implementation keeps track of the uncommitted log entries that the system has yet to reach consensus on.
type Commit interface {
	// Length the total number of entries in the commit.
	Length() int

	// Append entries to the end of the commit.
	Append(entries ...[]byte)

	// Shift removes the given number of elements from the start of the slice.
	// Assume the given shift number is always positive.
	Shift(start int) [][]byte

	// Get an entry at a specified index.
	Get(i int) []byte
}

// CommitInMemory uses an in memory slice to store commit entries.
type CommitInMemory struct {
	entries [][]byte
}

// NewCommitInMemory creates a new commit that stores entries in memory.
func NewCommitInMemory() *CommitInMemory {
	return &CommitInMemory{make([][]byte, 0)}
}

// Length is the number of entries stored in memory.
func (c *CommitInMemory) Length() int {
	return len(c.entries)
}

// Get an in memory entry at a specified index.
func (c *CommitInMemory) Get(i int) []byte {
	return c.entries[i]
}

// Append entries to the end of the in memory slice.
func (c *CommitInMemory) Append(entries ...[]byte) {
	c.entries = append(c.entries, entries...)
}

// Shift removes a given number of elements from the start of the in memory slice.
func (c *CommitInMemory) Shift(start int) [][]byte {
	slice := c.entries[:start]
	c.entries = c.entries[start:]
	return slice
}
