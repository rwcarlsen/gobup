// Package rbup provides primitives for splitting and retrieving data in a
// space-efficient manner using a rolling checksum algorithm.
package rbup

import (
	"bufio"
	"io"
	"math"

	"github.com/rwcarlsen/gobup/rolling"
)

// configuration params
var (
	Window           = 256
	BlockSize uint32 = 1024 * 32
)

var target = math.MaxUint32 / BlockSize

// Handler is an interface for receiving a set of split file chunks from
// the Split function.
type Handler interface {
	io.WriteCloser
}

// Split splits the data in r into several chunks that are passed to h for
// handling.  The process is aborted returning an error if h.Write returns
// an error.
func Split(r io.Reader, h Handler) (err error) {
	defer func() {
		if err2 := h.Close(); err == nil {
			err = err2
		}
	}()

	data := make([]byte, 0, BlockSize*2)
	rh := rolling.New(Window)
	buf := bufio.NewReader(r)
	for {
		c, err := buf.ReadByte()
		if err != nil {
			break
		}
		data = append(data, c)

		rh.WriteByte(c)
		if rh.Sum32() < target && len(data) >= Window {
			if _, err := h.Write(data); err != nil {
				return err
			}
			data = data[:0]
		}
	}

	if len(data) > 0 {
		if _, err := h.Write(data); err != nil {
			return err
		}
	}

	return nil
}
