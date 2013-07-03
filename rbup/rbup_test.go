package rbup

import (
	"testing"
	"crypto/rand"
)

func TestRollingSum(t *testing.T) {
	data := make([]byte, 280)
	rand.Read(data)

	rs := NewRolling(data[:256])
	for i, c := range data[256:] {
		rs.WriteByte(c)
		rs.Sum32()
		t.Logf("sum to %v: %v, ratio=%v", i, rs.Sum32(), float64(rs.Sum32()) / float64(1 << 32))
	}
}

func TestSplit(t *testing.T) {
	data := bytes.Repeat([]byte("four score and seven years ago"), 3000)

	ch := make(chan Chunk)

	go Split(bytes.NewBuffer(data), ch)
	rs := NewRolling(data[:256])
	for i, c := range data[256:] {
		rs.WriteByte(c)
		rs.Sum32()
		t.Logf("sum to %v: %v, ratio=%v", i, rs.Sum32(), float64(rs.Sum32()) / float64(1 << 32))
	}
}


