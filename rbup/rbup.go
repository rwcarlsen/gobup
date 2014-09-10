// Package rbup provides primitives for splitting and retrieving data in a
// space-efficient manner using a rolling checksum algorithm.
package rbup

import (
	"bufio"
	"io"
)

const winsize = rollsum.DefaultWindow

// Handler is an interface for receiving a set of split file chunks from
// the Split function.
type Handler interface {
	io.Writer
}

// Split splits the data in r into several chunks that are passed to h for
// handling.  The process is aborted returning an error if h.Write returns
// an error.
func Split(r io.Reader, h Handler, rs RollSum) error {
	data := make([]byte, 0, avgBlock*4)
	rh := rollsum.New(winsize, avgBlock)
	buf := bufio.NewReader(r)
	for {
		c, err := buf.ReadByte()
		if err != nil {
			break
		}
		data = append(data, c)

		rh.WriteByte(c)
		if rh.OnSplit() && len(data) >= winsize {
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
