// Package rbup provides primitives for splitting and retrieving data in a
// space-efficient manner using a rolling checksum algorithm.
package rbup

import (
	"bufio"
	"io"
)

const minchunk = 64

// Handler is an interface for receiving a set of split file chunks from
// the Split function.
type Handler interface {
	io.Writer
}

// Split splits the data in r into several chunks that are passed to h for
// handling.  The process is aborted returning an error if h.Write returns
// an error.
func Split(r io.Reader, rs *rollsum.RollSum, h Handler) error {
	data := make([]byte, 0, avgBlock*4)
	buf := bufio.NewReader(r)
	for {
		c, err := buf.ReadByte()
		if err != nil {
			break
		}
		data = append(data, c)

		rs.WriteByte(c)
		if rs.OnSplit() && len(data) >= minchunk {
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
