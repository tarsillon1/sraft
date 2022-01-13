package sraft

import (
	"math"
	"sort"
)

// calcFloorOfHalf calculates the int floor of half the provided value.
func calcFloorOfHalf(len int) int {
	return int(math.Floor(float64(len) / 2))
}

// isMajority checks if a integer is greater than half another integer.
func isMajority(len, num int) bool {
	return num > calcFloorOfHalf(len)
}

func getConsensusIndex(indexes []int) int {
	lenIndexes := len(indexes)
	sort.Ints(indexes)
	foh := calcFloorOfHalf(lenIndexes) + 1
	return indexes[lenIndexes-foh]
}
