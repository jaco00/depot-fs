/*
 GlobalMeta.go

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
	"encoding/binary"
	"errors"
	"hash/crc64"
)

const (
	MaxGlobalMetaSize = 2048
	MaxGlobalMetaDup  = 10
)

var crcTable = crc64.MakeTable(crc64.ISO)

type GlobalMeta struct {
	Version   uint32
	Timestamp uint64
	Crc       uint64
	DataLen   uint32
	Data      [MaxGlobalMetaSize]byte
}

func calculateCRC(meta *GlobalMeta) uint64 {
	crc := crc64.New(crcTable)
	_ = binary.Write(crc, binary.LittleEndian, meta.Version)
	_ = binary.Write(crc, binary.LittleEndian, meta.DataLen)
	crc.Write(meta.Data[:meta.DataLen])
	return crc.Sum64()
}

func NewGlobalMeta(version uint32, data []byte) (*GlobalMeta, error) {
	if len(data) > MaxGlobalMetaSize {
		return nil, errors.New("data exceeds max allowed size")
	}
	meta := &GlobalMeta{
		Version: version,
		DataLen: uint32(len(data)),
	}
	copy(meta.Data[:], data)
	meta.Crc = calculateCRC(meta)
	return meta, nil
}

func (meta *GlobalMeta) ExtractData() (uint32, []byte, error) {
	if meta.DataLen > MaxGlobalMetaSize {
		return meta.Version, nil, nil
	}
	if meta.Crc != calculateCRC(meta) {
		return meta.Version, nil, errors.New("crc mismatch")
	}
	return meta.Version, meta.Data[:meta.DataLen], nil
}
