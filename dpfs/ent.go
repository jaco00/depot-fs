/*
 ent.go

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

package dpfs

type EntAddr uint32

func (b EntAddr) IsBigBlock() uint32 {
	return (uint32(b) & 0x80000000) >> 31
}

// return idx,group,isbig
func (b EntAddr) GetAddr() (uint32, uint32, uint32) {
	idx := uint32(b) & 0x000FFFFF
	b = b >> 20
	group := uint32(b & 0x7FF)
	return idx, group, uint32(b >> 11)
	//return (uint32(b) & 0x80000000) >> 31, (uint32(b) & 0x7FF00000) >> 20, pos
}

func MakeEntAddr(idx, group uint32, isBigBlock bool) uint32 {
	addr := group<<20 | idx
	if isBigBlock {
		addr = addr | 0x80000000
	}
	return addr
}
