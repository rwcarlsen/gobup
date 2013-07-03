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


