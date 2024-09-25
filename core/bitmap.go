/*
 bitmap.go

 GNU GENERAL PUBLIC LICENSE
 Version 3, 29 June 2007
 Copyright (C) 2024 Jack Ng <jack.ng.ca@gmail.com>

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <https://www.gnu.org/licenses/> */

package core

import (
	"fmt"
	"math/bits"
	"unsafe"
)

type Bitmap interface {
	Init(groupId uint32, data []uint8)
	GetData(offset int, length int) []uint8
	FreeBits() int
	TotalBits() int

	AllocBits(int, int, bool) ([]uint32, int)
	ClearBits(ptrs []uint32)
	CheckBit(ptr uint32) bool
}

func checkBit(groupId uint32, bitmap []uint8, inodeptr uint32) bool {
	idx, g, _ := EntAddr(inodeptr).GetAddr()
	if g != groupId {
		panic("Inner error:Wrong group id")
	}
	return bitmap[idx/8]&(1<<(idx%8)) != 0
}

func DumpBitmap(bitmap []uint8) {
	for i, v := range bitmap {
		if v != 0 {
			fmt.Printf("BM_VAL %d:%x\n", i, v)
		}
	}
}

func clearBits(bitmap []uint8, from, to uint32) {
	startByte := from / 8
	startBit := from % 8
	endByte := to / 8
	endBit := to % 8

	if startByte == endByte {
		mask := uint8((1<<(endBit-startBit))-1) << startBit
		bitmap[startByte] &^= mask
	} else {
		startMask := byte(0xFF) << startBit
		bitmap[startByte] &^= startMask

		endMask := byte((1 << (endBit)) - 1)
		bitmap[endByte] &^= endMask

		for i := startByte + 1; i < endByte; i++ {
			bitmap[i] = 0
		}
	}
}

func batchClearBits(groupId uint32, bitmap []uint8, ptrs []uint32) int {
	c := 0
	for _, p := range ptrs {
		idx, g, isBig := EntAddr(p).GetAddr()
		if g != groupId {
			panic("Inner error:Wrong group id")
		}
		startByte := idx / 8
		startBit := idx % 8
		if isBig > 0 {
			clearBits(bitmap, idx, idx+64)
			c += 64
		} else {
			mask := byte(1 << startBit)
			bitmap[startByte] &^= mask
			c++
		}
	}
	return c
}

type BitmapBase struct {
	bits     []uint8
	freeBits int
	GroupId  uint32
	lastPos  int
}

func (b *BitmapBase) GetData(offset int, length int) []uint8 {
	if offset < 0 {
		return b.bits
	}
	return b.bits[offset : offset+length]
}

func (b *BitmapBase) FreeBits() int {
	return b.freeBits
}

func (b *BitmapBase) CheckBit(ptr uint32) bool {
	return checkBit(b.GroupId, b.bits, ptr)
}

func (b *BitmapBase) ClearBits(ptrs []uint32) {
	b.freeBits += batchClearBits(b.GroupId, b.bits, ptrs)
}

func (b *BitmapBase) TotalBits() int {
	return len(b.bits) * 8
}

func (b *BitmapBase) CountFreeBits() int {
	count := 0
	for _, b := range b.bits {
		count += bits.OnesCount8(b)
	}
	return len(b.bits)*8 - count
}

func (b *BitmapBase) Init(groupId uint32, data []uint8) {
	b.GroupId = groupId
	b.bits = data
	b.lastPos = 0
	b.freeBits = b.CountFreeBits()
}

func (b *BitmapBase) trySet64Bits(pos int, of int) bool {
	if pos+8 >= len(b.bits) {
		return false
	}

	mask := (uint8(1) << of) - 1
	for i := 0; i <= 8; i++ {
		byteValue := b.bits[pos+i]
		if i == 0 {
			if byteValue>>of != 0 {
				return false
			}
		} else if i == 8 {
			if byteValue&mask != 0 {
				return false
			}
		} else {
			if byteValue != 0 {
				return false
			}
		}
	}

	for i := 0; i <= 8; i++ {
		if i == 0 {
			b.bits[pos+i] |= ^((uint8(1) << (of)) - 1)
		} else if i == 8 {
			b.bits[pos+i] |= mask
		} else {
			b.bits[pos+i] = 0xff
		}
	}

	return true
}

func (b *BitmapBase) AllocBits(numBits int, hlimit int, bigAlloc bool) ([]uint32, int) {
	var allocatedPositions []uint32
	cnt := 0
	bml := len(b.bits)
	bpos := b.lastPos
	for pos := bpos; pos < bml; pos++ {
		b.lastPos = pos
		for {
			of := bits.TrailingZeros8(^b.bits[pos])
			if of == 8 {
				break //try next byte
			}
			if bigAlloc && numBits-cnt >= 64 {
				if b.trySet64Bits(pos, of) {
					cnt += 64
					b.freeBits -= 64
					addr := MakeEntAddr(uint32(pos*8+of), b.GroupId, true)
					allocatedPositions = append(allocatedPositions, addr)
					if cnt >= numBits || len(allocatedPositions) >= hlimit {
						return allocatedPositions, cnt
					}
					break
				}
			}
			b.bits[pos] |= (1 << of)
			cnt++
			b.freeBits--
			addr := MakeEntAddr(uint32(pos*8+of), b.GroupId, false)
			allocatedPositions = append(allocatedPositions, addr)
			if cnt >= numBits || len(allocatedPositions) >= hlimit {
				return allocatedPositions, cnt
			}
		}
	}
	if bpos != 0 && b.freeBits > 0 {
		b.lastPos = 0
		lst, n := b.AllocBits(numBits-cnt, hlimit, bigAlloc)
		return append(allocatedPositions, lst...), cnt + n
	}
	return allocatedPositions, cnt
}

// Little Endian Only !!!
type Bitmap64 struct {
	buffer   []uint8
	bits     []uint64
	freeBits int
	GroupId  uint32
	lastPos  int
	bool
}

func (b *Bitmap64) Init(groupId uint32, data []uint8) {
	if len(data)%8 != 0 {
		panic("Bitmap length must be a multiple of 8")
	}
	b.GroupId = groupId
	b.buffer = data
	b.bits = unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(data))), len(data)/8)
	b.lastPos = 0
	b.freeBits = b.CountFreeBits()
}

func (b *Bitmap64) FreeBits() int {
	return b.freeBits
}

func (b *Bitmap64) CheckBit(ptr uint32) bool {
	return checkBit(b.GroupId, b.buffer, ptr)
}

func (b *Bitmap64) ClearBits(ptrs []uint32) {
	b.freeBits += batchClearBits(b.GroupId, b.buffer, ptrs)
}

func (b *Bitmap64) TotalBits() int {
	return len(b.bits) * 64
}

func (b *Bitmap64) GetData(offset int, length int) []uint8 {
	if offset < 0 {
		return b.buffer
	}
	return b.buffer[offset : offset+length]
}

func (b *Bitmap64) CountFreeBits() int {
	count := 0
	for _, b := range b.bits {
		count += bits.OnesCount64(b)
	}
	return len(b.bits)*64 - count
}

func (b *Bitmap64) AllocBits(numBits int, hlimit int, bigAlloc bool) ([]uint32, int) {
	var allocatedPositions []uint32
	cnt := 0
	bml := len(b.bits)
	bpos := b.lastPos
	for pos := bpos; pos < bml; pos++ {
		b.lastPos = pos
		for {
			of := bits.TrailingZeros64(^b.bits[pos])
			if of == 64 {
				break //try next 8 bytes
			}
			if bigAlloc && numBits-cnt >= 64 && b.bits[pos]>>of == 0 && pos < bml-1 {
				mask := (uint64(1) << of) - 1
				if b.bits[pos+1]&mask == 0 { //check next of bits
					b.bits[pos] |= ^((uint64(1) << (of)) - 1)
					b.bits[pos+1] |= mask
					cnt += 64
					b.freeBits -= 64
					addr := MakeEntAddr(uint32(pos<<6+of), b.GroupId, true)
					allocatedPositions = append(allocatedPositions, addr)
					if cnt >= numBits || len(allocatedPositions) >= hlimit {
						return allocatedPositions, cnt
					}
					break
				}
			}
			b.bits[pos] |= (1 << of)
			cnt++
			b.freeBits--
			addr := MakeEntAddr(uint32(pos<<6+of), b.GroupId, false)
			allocatedPositions = append(allocatedPositions, addr)
			if cnt >= numBits || len(allocatedPositions) >= hlimit {
				return allocatedPositions, cnt
			}
		}
	}
	if bpos != 0 && b.freeBits > 0 {
		b.lastPos = 0
		lst, n := b.AllocBits(numBits-cnt, hlimit, bigAlloc)
		return append(allocatedPositions, lst...), cnt + n
	}
	return allocatedPositions, cnt
}
