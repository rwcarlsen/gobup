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

const (
	window    = 256
	blockSize = 1024 * 32
	target    = math.MaxUint32 / blockSize
)

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
