// Package rolling implements a 32 bit rolling checksum similar to rsync's
// algorithm.
package rolling

import (
	"encoding/binary"
)

const m = 1 << 16

type RollingSum struct {
	a      uint16
	b      uint16
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
		rs.a += -uint16(rs.window[0]) + uint16(c)
		rs.b += -uint16(rs.size)*uint16(rs.window[0]) + rs.a
	} else if len(rs.window) > 0 {
		rs.a += uint16(c)
		rs.b += uint16(c) * uint16(len(rs.window))
	} else {
		rs.a = uint16(c)
		rs.b = uint16(rs.size) * uint16(c)
	}

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
