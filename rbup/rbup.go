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
	m = 1 << 16
)

type RollingSum struct {
	a      uint
	b      uint
	window []byte
	size   uint
}

func NewRolling(init []byte) *RollingSum {
	rs := &RollingSum{
		size:   uint(len(init)),
		window: init,
	}

	for i, c := range init {
		rs.a += uint(c)
		rs.b += (rs.size - uint(i)) * uint(c)
	}
	rs.a %= m
	rs.b %= m

	return rs
}

func (rs *RollingSum) Write(data []byte) (n int, err error) {
	for _, c := range data {
		rs.WriteByte(c)
	}
	return len(data), nil
}

func (rs *RollingSum) WriteByte(c byte) error {
	rs.a = (rs.a - uint(rs.window[0]) + uint(c)) % m
	rs.b = (rs.b - rs.size*uint(rs.window[0]) + rs.a) % m

	rs.window = append(rs.window, c)
	rs.window = rs.window[1:]
	return nil
}

func (rs *RollingSum) Sum32() uint32 {
	return uint32(rs.a) + uint32(rs.b) * m
}

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

	data := make([]byte, window)
	n, err := io.ReadFull(r, data)
	if err == io.ErrUnexpectedEOF {
		ch <- data[:n]
		return nil
	} else if err != nil {
		return err
	}

	h := NewRolling(data)
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
