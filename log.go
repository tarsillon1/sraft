package sraft

// Log is the contract for a log implementation.
// A log implemention stores committed log entries the system has reached consensus on local to the node.
type Log interface {
	// Length gets the total number of entries in the log.
	Length() int

	// Append adds entries to the end of the log.
	Append(entry ...[]byte)

	// Get an entry at a specified index.
	Get(i int) []byte
}
