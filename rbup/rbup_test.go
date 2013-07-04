package rbup

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

func TestRollingSum(t *testing.T) {
	rs := NewRolling(window)
	for i := 0; i < 100000; i++ {
		io.CopyN(rs, rand.Reader, 1)
		if rs.Sum32() < target {
			t.Logf("sum at %v<target: %v, ratio=%v", i, rs.Sum32(), float64(rs.Sum32())/float64(1<<32))
		}
	}
}

func TestSplit(t *testing.T) {
	seed := []byte("three score and seven years ago I was eating much food and then\n the tree ran away from the spoon and the little hog rolled around in the mud and then the cheese kept eating much food and many zoo visits")
	data := bytes.Repeat(seed, blockSize*25/len(seed))

	ch := make(chan []byte)

	go Split(bytes.NewBuffer(data), ch)

	n, tot := 0, 0
	for chunk := range ch {
		t.Logf("len(chunk)=%v", len(chunk))
		tot += len(chunk)
		n++
	}

	t.Logf("avg blocksize of %v bytes", tot/n)
	t.Logf("len(data)=%v", len(data))
	t.Logf("target=%v", target)
}

func TestArchive(t *testing.T) {
	seed := []byte("three score and seven years ago I was eating much food and then\n the tree ran away from the spoon and the little hog rolled around in the mud and then the cheese kept eating much food and many zoo visits")
	data := bytes.Repeat(seed, blockSize*25/len(seed))

	ch := make(chan []byte)
	go func() {
		if err := Split(bytes.NewBuffer(data), ch); err != nil {
			t.Fatal(err)
		}
	}()

	if err := Archive(ch, "test-dir", "test-file"); err != nil {
		t.Fatal(err)
	}
}
