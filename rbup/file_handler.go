package rbup

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

// Index is a structure for storing an ordered sequence of objects/files
// representing a single backed-up file.
type Index struct {
	Name    string
	Objects []string
}

// Archiver implements the Handler interface for storing split files in a
// directory.  A single-file index is created containing the list of all chunks
// that constitute a particular file.
type Archiver struct {
	Name  string
	Dst   string
	index []string
	h     hash.Hash
}

func NewArchiver(name, dst string) (*Archiver, error) {
	if err := os.MkdirAll(dst, 0760); err != nil {
		return nil, err
	}
	return &Archiver{
		Name: name,
		Dst:  dst,
		h:    sha1.New(),
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
	a.index = append(a.index, fname)

	if _, err := os.Stat(filepath.Join(a.Dst, fname)); err == nil {
		// chunk file already exists - shortcut (not an error)
		return len(chunk), nil
	}

	f, err := os.Create(filepath.Join(a.Dst, fname))
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return f.Write(chunk)
}

// Combine reconstitutes a split file with the given index filename
// stored in the dst directory.
func Combine(index string, dst string) (io.Reader, error) {
	data, err := ioutil.ReadFile(filepath.Join(dst, index))
	if err != nil {
		return nil, err
	}
	idx := []string{}
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, err
	}
	return &reader{dst: dst, indx: idx}, nil
}

// reader is a special io.Reader that gradually returns bytes from all the
// objects for a split file chunk by chunk.
type reader struct {
	dst      string
	indx     []string
	buf      []byte
	objIndex int
}

func (r *reader) Read(data []byte) (n int, err error) {
	if r.objIndex == len(r.indx) {
		return 0, io.EOF
	}

	if len(r.buf) == 0 {
		fpath := filepath.Join(r.dst, r.indx[r.objIndex])
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
