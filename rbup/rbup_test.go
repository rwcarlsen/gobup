package rbup

import (
	"testing"
)

func TestRollingSum(t *testing.T) {
	data := []byte("four score and seven years ago I started eating much food and it was so delicious")

	rs := NewRolling(data[:32])
	for i, c := range data[32:] {
		rs.WriteByte(c)
		rs.Sum32()
		t.Logf("sum to %v: %v", i, rs.Sum32())
	}
}


