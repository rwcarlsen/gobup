package rolling

import (
	"crypto/rand"
	"io"
	"testing"
)

func TestRandom(t *testing.T) {
	window := 256
	blockSize := 1024 * 4

	rs := New(window, blockSize)
	data := make([]byte, 1)
	for i := 0; i < 10000; i++ {
		_, err := io.ReadFull(rand.Reader, data)
		if err != nil {
			t.Fatal(err)
		}
		rs.WriteByte(data[0])

		if rs.OnSplit() {
			t.Logf("sum at %v<target: %v, ratio=%v", i, rs.Sum(), float64(rs.Sum())/float64(1<<32))
		}
	}
}

func TestRollingSum(t *testing.T) {
	data1 := []byte("hello my name is joe and I work in a button factory")
	data2 := []byte("hello my name is joe and I eat in a button factory")
	window := 8

	rs := New(window, 8*1024)
	sums1 := []uint32{}
	for _, c := range data1 {
		rs.WriteByte(c)
		sums1 = append(sums1, rs.Sum())
	}

	rs = New(window, 8*1024)
	sums2 := []uint32{}
	for _, c := range data2 {
		rs.WriteByte(c)
		sums2 = append(sums2, rs.Sum())
	}

	for i := 0; i < 27; i++ {
		if sums1[i] != sums2[i] {
			t.Errorf("pre sums %v don't match, %v != %v", i, sums1[i], sums2[i])
		}
	}

	for i := 1; i < 14; i++ {
		i1 := len(sums1) - i
		i2 := len(sums2) - i
		if sums1[i1] != sums2[i2] {
			t.Errorf("post sums %v don't match, %v != %v", i-1, sums1[i1], sums2[i2])
		}
	}
}
