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

type Index struct {
	Name    string
	Objects []string
}

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

func (a *Archiver) Close() error {
	data, err := json.MarshalIndent(a.index, "", "\t")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filepath.Join(a.Dst, a.Name+".idx"), data, 0660)
}

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

type Handler interface {
	io.WriteCloser
}

func Combine(index io.Reader, dst string) (io.Reader, error) {
	indx := &Index{}
	dec := json.NewDecoder(index)
	if err := dec.Decode(indx); err != nil {
		return nil, err
	}
	return &Reader{dst: dst, indx: indx}, nil
}

type Reader struct {
	dst      string
	indx     *Index
	buf      []byte
	objIndex int
}

func (r *Reader) Read(data []byte) (n int, err error) {
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
