package rbup

import (
	"bytes"
	"testing"
)

func TestRollingSum(t *testing.T) {
	seed := []byte("four score and seven years ago I was eating cheese from #$%^?!")
	data := bytes.Repeat(seed, window / len(seed) + 1)

	rs := NewRolling(data[:window])
	for i, c := range data[window:] {
		rs.WriteByte(c)
		rs.Sum32()
		t.Logf("sum to %v: %v, ratio=%v", i, rs.Sum32(), float64(rs.Sum32())/float64(1<<32))
	}
}

func TestSplit(t *testing.T) {
	seed := []byte("three score and seven years ago I was eating much food and then the tree ran away from the spoon and the little hog rolled around in the mud and then the cheese kept eating much food and many zoo visits")
	data := bytes.Repeat(seed, blockSize * 25 / len(seed))

	ch := make(chan Chunk)

	go Split(bytes.NewBuffer(data), ch)

	n, tot := 0, 0
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
	seed := []byte("three score and seven years ago I was eating much food and then the tree ran away from the spoon and the little hog rolled around in the mud and then the cheese kept eating much food and many zoo visits")
	data := bytes.Repeat(seed, blockSize * 25 / len(seed))

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
