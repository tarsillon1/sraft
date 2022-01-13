package sraft

type election struct {
	term        uint64
	commitIndex uint64
}
