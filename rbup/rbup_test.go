package rbup

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestRollingSum(t *testing.T) {
	data := make([]byte, 280)
	rand.Read(data)

	rs := NewRolling(data[:256])
	for i, c := range data[256:] {
		rs.WriteByte(c)
		rs.Sum32()
		t.Logf("sum to %v: %v, ratio=%v", i, rs.Sum32(), float64(rs.Sum32())/float64(1<<32))
	}
}

func TestSplit(t *testing.T) {
	data := bytes.Repeat([]byte("four score and seven years ago I was eating much food and then the tree ran away from the spoon and the little hog rolled around in the mud"), 3000)

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
}

func TestArchive(t *testing.T) {
	data := bytes.Repeat([]byte("four score and seven years ago I was eating much food and then the tree ran away from the spoon and the little hog rolled around in the mud"), 3000)

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
