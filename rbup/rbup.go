package rbup

import (
	"bufio"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
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

func Archive(ch chan []byte, dst, name string) error {
	if err := os.MkdirAll(dst, 0760); err != nil {
		return err
	}

	h := sha1.New()
	index := &Index{Name: name}
	for chunk := range ch {
		h.Reset()
		h.Write(chunk)

		fname := fmt.Sprintf("sha1-%x.dat", h.Sum(nil))
		index.Objects = append(index.Objects, fname)

		if _, err := os.Stat(filepath.Join(dst, fname)); err == nil {
			continue
		}

		f, err := os.Create(filepath.Join(dst, fname))
		if err != nil {
			close(ch)
			return err
		}

		if _, err := f.Write(chunk); err != nil {
			close(ch)
			return err
		}

		f.Close()
	}

	data, err := json.MarshalIndent(index, "", "\t")
	if err != nil {
		return err
	}

	return ioutil.WriteFile(filepath.Join(dst, name+".idx"), data, 0660)
}

func Split(r io.Reader, ch chan []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("rbup: channel closed unexpectedly")
		}
	}()
	defer close(ch)

	data := make([]byte, 0)
	h := NewRolling(window)
	buf := bufio.NewReader(r)
	for {
		c, err := buf.ReadByte()
		if err != nil {
			break
		}
		data = append(data, c)
		if h.WriteByte(c); h.Sum32() < target {
			ch <- data
			data = make([]byte, 0)
		}
	}

	if len(data) > 0 {
		ch <- data
	}
	return nil
}
