// Package rbup provides primitives for splitting and retrieving data in a
// space-efficient manner using a rolling checksum algorithm.
package rbup

import (
	"bufio"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"

	"github.com/rwcarlsen/gobup/rolling"
)

// configuration params
const (
	window    = 256
	blockSize = 1024 * 32
)

const target = math.MaxUint32 / blockSize

// Index is a structure for storing an ordered sequence of objects/files
// representing a single backed-up file.
type Index struct {
	Name    string
	Objects []string
}

// Archiver implements the Handler interface for storing split files in a
// directory.
type Archiver struct {
	Name  string
	Dst   string
	index *Index
	h     hash.Hash
}

func NewArchiver(name, dst string) (*Archiver, error) {
	if err := os.MkdirAll(dst, 0760); err != nil {
		return nil, err
	}
	return &Archiver{
		Name:  name,
		Dst:   dst,
		index: &Index{Name: name},
		h:     sha1.New(),
	}, nil
}

// Close writes the index of split chunks to the archive's dst directory.
func (a *Archiver) Close() error {
	data, err := json.MarshalIndent(a.index, "", "\t")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filepath.Join(a.Dst, a.Name+".idx"), data, 0660)
}

// Write stores chunk in a hash-named file in the archive's dst directory.
func (a *Archiver) Write(chunk []byte) (n int, err error) {
	a.h.Reset()
	a.h.Write(chunk)

	fname := fmt.Sprintf("sha1-%x.dat", a.h.Sum(nil))
	a.index.Objects = append(a.index.Objects, fname)

	if _, err := os.Stat(filepath.Join(a.Dst, fname)); err == nil {
		return len(chunk), nil // chunk file already exists
	}

	f, err := os.Create(filepath.Join(a.Dst, fname))
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return f.Write(chunk)
}

// Handler is an interface for receiving a set of split file chunks from
// the Split function.
type Handler interface {
	io.WriteCloser
}

// Combine reconstitutes a split file with the given index and file chunks
// stored in the dst directory.
func Combine(index io.Reader, dst string) (io.Reader, error) {
	indx := &Index{}
	dec := json.NewDecoder(index)
	if err := dec.Decode(indx); err != nil {
		return nil, err
	}
	return &reader{dst: dst, indx: indx}, nil
}

// reader is a special io.Reader that gradually returns bytes from all the
// objects for a split file chunk by chunk.
type reader struct {
	dst      string
	indx     *Index
	buf      []byte
	objIndex int
}

func (r *reader) Read(data []byte) (n int, err error) {
	if r.objIndex == len(r.indx.Objects) {
		return 0, io.EOF
	}

	if len(r.buf) == 0 {
		fpath := filepath.Join(r.dst, r.indx.Objects[r.objIndex])
		r.buf, err = ioutil.ReadFile(fpath)
		if err != nil {
			return 0, err
		}
		r.objIndex++
	}

	n = copy(data, r.buf)
	r.buf = r.buf[n:]
	return n, nil
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

	data := make([]byte, 0)
	rh := rolling.New(window)
	buf := bufio.NewReader(r)
	for {
		c, err := buf.ReadByte()
		if err != nil {
			break
		}
		data = append(data, c)
		if rh.WriteByte(c); rh.Sum32() < target {
			if _, err := h.Write(data); err != nil {
				return err
			}
			data = make([]byte, 0)
		}
	}

	if len(data) > 0 {
		if _, err := h.Write(data); err != nil {
			return err
		}
	}
	return nil
}
