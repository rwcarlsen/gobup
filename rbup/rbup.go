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

type Chunk struct {
	RollSum uint32
	Data    []byte
}

type RollingSum struct {
	a      uint
	b      uint
	window []byte
	size   uint
	m      uint
}

func NewRolling(init []byte) *RollingSum {
	rs := &RollingSum{
		size:   uint(len(init)),
		window: init,
		m:      1 << 16,
	}

	for i, c := range init {
		rs.a += uint(c)
		rs.b += (rs.size - uint(i)) * uint(c)
	}
	rs.a %= rs.m
	rs.b %= rs.m

	return rs
}

func (rs *RollingSum) Write(data []byte) (n int, err error) {
	for _, c := range data {
		rs.WriteByte(c)
	}
	return len(data), nil
}

func (rs *RollingSum) WriteByte(c byte) error {
	rs.a = (rs.a - uint(rs.window[0]) + uint(c)) % rs.m
	rs.b = (rs.b - rs.size*uint(rs.window[0]) + rs.a) % rs.m

	rs.window = append(rs.window, c)
	rs.window = rs.window[1:]
	return nil
}

func (rs *RollingSum) Sum32() uint32 {
	return uint32(rs.a) + (uint32(rs.b) << 16)
}

const (
	window    = 128
	blockSize = 1024 * 8
	target    = math.MaxUint32 / blockSize
)

type Index struct {
	Name    string
	Objects []string
}

func Archive(ch chan Chunk, dst, name string) error {
	if err := os.MkdirAll(dst, 0760); err != nil {
		return err
	}

	h := sha1.New()
	index := &Index{Name: name}
	for chunk := range ch {
		h.Reset()
		h.Write(chunk.Data)

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

		if _, err := f.Write(chunk.Data); err != nil {
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

func Split(r io.Reader, ch chan Chunk) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("rbup: channel closed unexpectedly")
		}
	}()
	defer close(ch)

	data := make([]byte, window)
	n, err := io.ReadFull(r, data)
	if err == io.ErrUnexpectedEOF {
		ch <- Chunk{NewRolling(data[:n]).Sum32(), data[:n]}
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
			ch <- Chunk{h.Sum32(), data}
			data = []byte{}
		}
	}

	if len(data) > 0 {
		ch <- Chunk{h.Sum32(), data}
	}
	return nil
}
