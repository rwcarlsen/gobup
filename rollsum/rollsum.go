// Package rolling implements a 32 bit rolling checksum similar to rsync's
// algorithm.
package rolling

import "math"

const window = 64
const avgsplit = 8 * 1024
const charOffset = 31

type Rollsum struct {
	s1, s2  uint32
	window  []byte
	winsize int
	i       int
	target  uint32
}

func New() *Rollsum { return NewCustom(window, avgsplit) }

func NewCustom(window, splitlen int) *Rollsum {
	return &Rollsum{
		s1:      uint32(window * charOffset),
		s2:      uint32(window * (window - 1) * charOffset),
		window:  make([]byte, window),
		winsize: window,
		target:  math.MaxUint32 / uint32(splitlen),
	}
}

func (rs *Rollsum) OnSplit() bool { return rs.Sum() < rs.target }

func (rs *Rollsum) Sum() uint32 { return (rs.s1 << 16) | (rs.s2 & 0xffff) }

func (rs *Rollsum) WriteByte(ch byte) error {
	drop := rs.window[rs.i]
	rs.s1 += uint32(ch) - uint32(drop)
	rs.s2 += rs.s1 - uint32(rs.winsize)*uint32(drop+charOffset)

	rs.window[rs.i] = ch
	rs.i = (rs.i + 1) % rs.winsize

	return nil
}
