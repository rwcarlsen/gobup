// Package rolling implements a 32 bit rolling checksum similar to rsync's
// algorithm.
package rolling

import (
	"encoding/binary"
)

const m = 1 << 16

type RollingSum struct {
	a      uint
	b      uint
	window []byte
	size   int
}

func New(window int) *RollingSum {
	return &RollingSum{size: window}
}

func (rs *RollingSum) Write(data []byte) (n int, err error) {
	for _, c := range data {
		rs.WriteByte(c)
	}
	return len(data), nil
}

func (rs *RollingSum) WriteByte(c byte) error {
	if len(rs.window) == rs.size {
		rs.a += -uint(rs.window[0]) + uint(c)
		rs.b += -uint(rs.size)*uint(rs.window[0]) + rs.a
	} else if len(rs.window) > 0 {
		rs.a += uint(c)
		rs.b += uint(c) * uint(len(rs.window))
	} else {
		rs.a = uint(c)
		rs.b = uint(rs.size) * uint(c)
	}
	rs.a %= m
	rs.b %= m

	rs.window = append(rs.window, c)
	if len(rs.window) > rs.size {
		rs.window = rs.window[1:]
	}
	return nil
}

func (rs *RollingSum) Sum32() uint32 {
	return uint32(rs.a) + uint32(rs.b)*m
}

func (rs *RollingSum) Size() int {
	return 4
}

func (rs *RollingSum) BlockSize() int {
	return 1
}

func (rs *RollingSum) Reset() {
	rs.window = make([]byte, 0)
	rs.a, rs.b = 0, 0
}

func (rs *RollingSum) Sum(b []byte) []byte {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, rs.Sum32())
	return append(b, data...)
}
