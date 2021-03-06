package rbup

import (
	"bytes"
	"testing"

	"github.com/rwcarlsen/gobup/rollsum"
)

type testHandler []int

func (h *testHandler) Close() error { return nil }
func (h *testHandler) Write(chunk []byte) (int, error) {
	*h = append(*h, len(chunk))
	return len(chunk), nil
}

func TestSplit(t *testing.T) {
	seed := []byte("three score and seven years ago I was eating much food and then\n the tree ran away from the spoon and the little hog rolled around in the mud and then the cheese kept eating much food and many zoo visits")
	data := bytes.Repeat(seed, int(rollsum.DefaultSplitAvg)*25/len(seed))

	expected := []int{
		204624,
		78891,
		1582,
		22427,
		23443,
		13687,
		6697,
		25475,
		36980,
		300698,
		42005,
		32088,
		40381,
		58038,
		14146,
		5782,
	}

	th := &testHandler{}

	err := Split(bytes.NewBuffer(data), rollsum.New(), th)
	if err != nil {
		t.Fatal(err)
	}

	for i, n := range *th {
		if n != expected[i] {
			t.Errorf("block %v length: expected %v, got %v", i+1, expected[i], n)
		}
	}
}

func TestArchive(t *testing.T) {
	seed := []byte("three score and seven years ago I was eating much food and then\n the tree ran away from the spoon and the little hog rolled around in the mud and then the cheese kept eating much food and many zoo visits")
	data := bytes.Repeat(seed, int(rollsum.DefaultSplitAvg)*25/len(seed))

	a, err := NewArchiver("test-file", "test-dir")
	if err != nil {
		t.Fatal(err)
	}

	err = Split(bytes.NewBuffer(data), rollsum.New(), a)
	if err != nil {
		t.Fatal(err)
	}
}
