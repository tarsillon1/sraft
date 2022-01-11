package sraft

import (
	"testing"
	"time"
)

func TestNewNode(t *testing.T) {
	peers := make([]*peer, 0)
	n := newNode(&nodeConfig{
		peers:  peers,
		commit: NewCommitInMemory(),
		log:    NewCommitInMemory(),
	})

	loopCloseChan := make(chan bool)
	go func() {
		n.start()
		close(loopCloseChan)
	}()

	n.stop()
	<-loopCloseChan
}

func createNodes(l int) []*node {
	nodeConfigs := make([]*nodeConfig, l)
	for x := 0; x < l; x++ {
		peers := make([]*peer, 0)
		nodeConfigs[x] = &nodeConfig{
			id:     x,
			peers:  peers,
			log:    NewCommitInMemory(),
			commit: NewCommitInMemory(),
		}
	}

	for x := 0; x < l; x++ {
		for y := x + 1; y < l; y++ {
			peerX := newPeer()
			peerY := newPeer()
			nodeConfigs[x].peers = append(nodeConfigs[x].peers, peerX)
			nodeConfigs[y].peers = append(nodeConfigs[y].peers, peerY)
			peerX.out = peerY.in
			peerY.out = peerX.in
		}
	}

	nodes := make([]*node, l)
	for x := 0; x < l; x++ {
		nodes[x] = newNode(nodeConfigs[x])
	}
	return nodes
}

func startAll(nodes []*node) {
	for _, node := range nodes {
		go node.start()
	}
}

func stopAll(nodes []*node) {
	for _, node := range nodes {
		node.stop()
	}
}

func getLeader(nodes []*node) *node {
	for _, node := range nodes {
		if node.state == Leader {
			return node
		}
	}
	return nil
}

func getLeaderOrFail(t *testing.T, nodes []*node) *node {
	var leader *node
	leaderCount := 0
	for _, node := range nodes {
		if node.state == Leader {
			leader = node
			leaderCount++
		}
	}

	if leaderCount != 1 {
		t.Fatalf("incorrect number of leaders: expected 1, got %d", leaderCount)
	}

	return leader
}

func checkLogLength(t *testing.T, nodes []*node, expectedLen int) {
	for _, node := range nodes {
		l := node.log.Length()
		if l != expectedLen {
			t.Fatalf("wrong log length: expected %d, got %d", expectedLen, l)
		}
	}
}

func checkLogAtIndex(t *testing.T, nodes []*node, i int, b []byte) {
	for _, node := range nodes {
		log := node.log.Get(i)
		if string(log) != string(b) {
			t.Fatalf("wrong log: expected %s, got %s", string(b), string(log))
		}
	}
}

func TestLeaderElection(t *testing.T) {
	nodes := createNodes(5)
	startAll(nodes)

	time.Sleep(time.Second)

	getLeaderOrFail(t, nodes)
	stopAll(nodes)
}

func TestLeaderRelection(t *testing.T) {
	nodes := createNodes(5)
	startAll(nodes)

	time.Sleep(time.Second)

	leader := getLeaderOrFail(t, nodes)
	leader.stop()

	time.Sleep(time.Second)

	newLeader := getLeaderOrFail(t, nodes)

	if newLeader == leader {
		t.Fatal("leader was stopped but is still in leader state")
	}

	stopAll(nodes)
}

func TestAppendLog(t *testing.T) {
	nodes := createNodes(5)
	startAll(nodes)

	time.Sleep(time.Second)

	leader := getLeaderOrFail(t, nodes)

	log1 := []byte("hello world")
	leader.appendEntries(log1)

	time.Sleep(time.Second)

	checkLogLength(t, nodes, 1)
	checkLogAtIndex(t, nodes, 0, log1)

	log2 := []byte("hello world 1")
	leader.appendEntries(log2)

	log3 := []byte("hello world 2")
	leader.appendEntries([]byte(log3))

	log4 := []byte("hello world 3")
	leader.appendEntries(log4)

	time.Sleep(time.Second)

	checkLogLength(t, nodes, 4)
	checkLogAtIndex(t, nodes, 1, log2)
	checkLogAtIndex(t, nodes, 2, log3)
	checkLogAtIndex(t, nodes, 3, log4)

	stopAll(nodes)
}

type benchmarkLog struct {
	*CommitInMemory
	benchmark chan int
}

func (b *benchmarkLog) Append(e ...[]byte) {
	b.CommitInMemory.Append(e...)
	b.benchmark <- len(e)
}

func newBenchamarkLog() *benchmarkLog {
	commitInMemory := NewCommitInMemory()
	return &benchmarkLog{
		benchmark:      make(chan int),
		CommitInMemory: commitInMemory,
	}
}

func BenchmarkConsensus(b *testing.B) {
	b.StopTimer()

	log := newBenchamarkLog()

	nodes := createNodes(5)
	startAll(nodes)

	time.Sleep(time.Second)

	leader := getLeader(nodes)
	leader.log = log

	b.StartTimer()

	entry := []byte("hello world")
	for n := 0; n < b.N; n++ {
		leader.appendEntries(entry)

		<-log.benchmark
	}
}
