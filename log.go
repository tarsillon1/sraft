package sraft

type Log interface {
	Add(entry []byte)
	Length() int
}
