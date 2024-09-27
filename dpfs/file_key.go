/*
 file_key.go

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
	"encoding/hex"
	"errors"
	"fmt"
)

type FileKey struct {
	Shard    uint16
	Inodeptr uint32
	Seq      uint32
	Stamp    uint32
}

const keyLength = 28

func (k *FileKey) ToString() string {
	return fmt.Sprintf("%04x%08x%08x%08x", k.Shard, k.Inodeptr, k.Seq, k.Stamp)
}

func (k *FileKey) ParseKey(key string) error {
	if len(key) != 28 {
		return errors.New("invalid key length")
	}
	bytes, err := hex.DecodeString(string(key))
	if err != nil {
		return err
	}
	k.Shard = binary.BigEndian.Uint16(bytes[:2])
	k.Inodeptr = binary.BigEndian.Uint32(bytes[2:6])
	k.Seq = binary.BigEndian.Uint32(bytes[6:10])
	k.Stamp = binary.BigEndian.Uint32(bytes[10:14])

	return nil
}
