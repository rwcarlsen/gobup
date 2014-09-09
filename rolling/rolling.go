// Package rolling implements a 32 bit rolling checksum similar to rsync's
// algorithm.
package rolling

import "math"

const (
	DefaultWindow = 64
	DefaultSplit  = 1024 * 8
)

type Rollsum struct {
	a      uint16
	b      uint16
	window []byte
	size   int
	i      int
	target uint32
}

func New(window, splitlen int) *Rollsum {
	return &Rollsum{
		window: make([]byte, window),
		size:   window,
		target: math.MaxUint32 / uint32(splitlen),
	}
}

func (rs *Rollsum) OnSplit() bool {
	return rs.Sum() < rs.target
}

func (rs *Rollsum) WriteByte(c byte) error {
	rs.a += -uint16(rs.window[rs.i]) + uint16(c)
	rs.b += -uint16(rs.size)*uint16(rs.window[rs.i]) + rs.a

	rs.window[rs.i] = c
	if rs.i++; rs.i == rs.size {
		rs.i = 0
	}

	return nil
}

func (rs *Rollsum) Sum() uint32 {
	return uint32(rs.a) | (uint32(rs.b) << 16)
}

func (rs *Rollsum) Size() int {
	return 4
}

func (rs *Rollsum) BlockSize() int {
	return 1
}

func (rs *Rollsum) Reset() {
	rs.window = make([]byte, rs.size)
	rs.a, rs.b = 0, 0
}
