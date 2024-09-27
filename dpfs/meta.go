/*
 meta.go

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

import (
	"errors"
	"fmt"
	"hash/crc64"
)

const (
	SuperBlockMagic      = 0x55AA
	DefaultBlockSize     = 8192
	MaxBlockSize         = 128 * 4096
	MaxBlockGroupNum     = 1024
	DefaultInodesRatio   = 4
	DefaultBlocksInGroup = 1024 * 1024
)

const (
	AttrBigAlloc = 0
)

// File system meta
type SuperBlock struct {
	BlockSize     uint32
	TotalGroups   uint32
	BlocksInGroup uint32
	InodesRatio   uint32
	ShardId       uint16
	Attr          uint16 //bit 0 BigAlloc
	Magic         uint32
	Crc           uint64
}

func (s *SuperBlock) EnableBigAlloc() {
	s.Attr |= (1 << AttrBigAlloc)
}

func (s *SuperBlock) IsBigAllocEnabled() bool {
	return s.Attr&(1<<AttrBigAlloc) != 0
}

func (s *SuperBlock) Checksum() uint64 {
	data := fmt.Sprintf("%d_%d_%d_%d_%d_%d_%x",
		s.BlockSize,
		s.TotalGroups,
		s.BlocksInGroup,
		s.InodesRatio,
		s.ShardId,
		s.Attr,
		s.Magic)
	crcTable := crc64.MakeTable(crc64.ECMA)
	return crc64.Checksum([]byte(data), crcTable)
}

func (s *SuperBlock) Sign() {
	s.Magic = SuperBlockMagic
	s.Crc = s.Checksum()
	return
}

func (s *SuperBlock) Verify() error {
	if s.Magic != SuperBlockMagic {
		return errors.New("Bad magic")
	}
	if s.BlockSize%4096 != 0 {
		return errors.New("Invalid BlockSize; must be a multiple of 4K")
	}
	if s.BlocksInGroup%1024 != 0 {
		return errors.New("Invalid BlocksInGroup ; must be a multiple of 1024")
	}
	if s.InodesRatio%DefaultInodesRatio != 0 {
		return errors.New("Invalid InodesRatio; must be a multiple of DefaultInodesRatio")
	}
	if (s.BlocksInGroup/s.InodesRatio)%64 != 0 {
		return errors.New("Invalid InodesRatio; (BlocksInGroup/InodesRatio) must be a multiple of 64")
	}
	if s.Crc != s.Checksum() {
		return errors.New("Bad Crc")
	}
	return nil
}

func (s *SuperBlock) TotalBlocks() int64 {
	return int64(s.TotalGroups) * int64(s.BlocksInGroup)
}

func (s *SuperBlock) TotalInodes() int64 {
	return int64(s.TotalGroups) * int64(s.BlocksInGroup/s.InodesRatio)
}

func (s *SuperBlock) TotalSpace() int64 {
	return s.TotalBlocks() * int64(s.BlockSize)
}
