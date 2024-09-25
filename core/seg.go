/*
 seg.go

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

type Seg struct {
	offset int
	length int
}

func mergeSeg(addr []uint32) ([]Seg, int) {
	segs := []Seg{}
	lastof := -1
	lastLen := 1
	bits := 0
	for _, e := range addr {
		idx, _, isBig := EntAddr(e).GetAddr()
		bits++
		if isBig > 0 {
			bits += 63
		}
		of := int(idx / 8)
		if lastof < 0 { //begin new span
			lastof = of
			if isBig > 0 {
				lastLen = 8
			}
			continue
		}
		if lastof+lastLen > of {
			if isBig > 0 {
				lastLen += 8
			}
			continue
		} else if lastof+lastLen == of {
			if isBig > 0 {
				lastLen += 8
			} else {
				lastLen++
			}
		} else {
			segs = append(segs, Seg{offset: lastof, length: lastLen})
			lastof = of
			lastLen = 1
		}
	}
	if lastof >= 0 {
		segs = append(segs, Seg{offset: lastof, length: lastLen})
	}
	return segs, bits
}
