package rbup

import (
	"bytes"
	"testing"
)

func TestRollingSum(t *testing.T) {
	seed := []byte("four score and seven years ago")
	data := bytes.Repeat(seed, int(window) / len(seed) + 1)

	rs := NewRolling(data[:window])
	for i, c := range data[window:] {
		rs.WriteByte(c)
		rs.Sum32()
		t.Logf("sum to %v: %v, ratio=%v", i, rs.Sum32(), float64(rs.Sum32())/float64(1<<32))
	}
}

func TestSplit(t *testing.T) {
	data := bytes.Repeat([]byte("three score and seven years ago I was eating much food and then the tree ran away from the spoon and the little hog rolled around in the mud and then the cheese kept eating much food and many zoo visits later I ate fifteen boxes of swiss and havarti cheese because they are two of my least unfavorite types of live sustenance.000000!@#$%^&*()"), 50000)

	ch := make(chan Chunk)

	go Split(bytes.NewBuffer(data), ch)

	tot := 0
	n := 0
	for chunk := range ch {
		t.Logf("len(chunk)=%v", len(chunk.Data))
		tot += len(chunk.Data)
		n++
	}

	t.Logf("avg blocksize of %v bytes", tot/n)
	t.Logf("len(data)=%v", len(data))
	t.Logf("target=%v", target)
}

func TestArchive(t *testing.T) {
	data := bytes.Repeat([]byte("four score and seven years ago I was eating much food and then the tree ran away from the spoon and the little hog rolled around in the mud"), 50000)

	ch := make(chan Chunk)
	go func() {
		if err := Split(bytes.NewBuffer(data), ch); err != nil {
			t.Fatal(err)
		}
	}()

	if err := Archive(ch, "test-dir", "test-file"); err != nil {
		t.Fatal(err)
	}
}
