/*
 seek_test.go

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

package dpfs_test

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/jaco00/depot-fs/dpfs"
)

var (
	fn       = "test.io"
	meta     = make([]byte, 10)
	metaSize = 32
)

func doSeek(t *testing.T, fs *dpfs.FileSystem, totalSize int64, batchLimit int64, pos int64) error {
	rdp, err := dpfs.NewRandomDataProvider(int64(batchLimit), totalSize, true, true)
	if err != nil {
		return err
	}
	key, _, _, _, err := dpfs.WriteFile(fs, rdp, fn, meta, false)
	if err != nil {
		t.Errorf("test size:%d, write file failed :%s", totalSize, err)
		return err
	}
	f, err := fs.OpenFile(key)
	if err != nil {
		return err
	}
	data := make([]byte, batchLimit)
	var offset int64 = 0
	if pos > int64(f.Inode.FileSize) {
		pos = int64(f.Inode.FileSize)
	}
	for offset < pos {
		if offset+batchLimit > pos {
			batchLimit = pos - offset
		}
		r, err := f.Read(data[:batchLimit])
		if err != nil {
			fmt.Printf("read file failed: %s [off:%d,pos:%d,need:%d]\n", err, offset, pos, batchLimit)
			return err
		}
		if r == 0 {
			return errors.New("read nil")
		}
		offset += int64(r)
	}

	off1 := f.GetOffset()
	off2, err := f.SeekPos(pos)
	if err != nil {
		t.Errorf("SeedPos failed:%s  offset:%v %v", err, off1, off2)
		return err
	}
	if off1 != off2 {
		t.Errorf("Seek to end test failed(read), total size :%d,meta:%d, pos:%d,  %v!=%v", totalSize, f.Inode.MetaSize, pos, off1, off2)
	}

	return nil
}

func TestSeekToPos(t *testing.T) {
	// 创建一个临时目录
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(testDir)

	var group uint32 = 32
	fs, err := dpfs.MakeFileSystem(group, 2*256*1024, testDir, "", "", 0, true)
	if err != nil {
		t.Fatalf("Failed to create file system: %v", err)
	}

	testSuits := [][]int64{
		{4, 1024, 4},
		{4, 1024, 3},
		{256, 1024, 256},
		{256, 1024, 250},
		{4000, 1024, 400},
		{4000, 1024, 4000},
		{4090, 1024, 4080},
		{8192, 1024, 8192},
		{8192, 2048, 8190},
		{8192, 8192, 9000},
		{8193, 8192, 0},
		{8193, 8192, 8192},
		{8193, 8192, 8193},
		{8192*2 - 32, 8192, 8192*2 - 32}, //align with block boundary
		{8192*28 + 100, 8000, 8192 * 28},
		{8192*29 + 200, 3000, 81900},
		{8192 * 18, 8192 * 64, 9000},
		{8192 * 180, 8192 * 64, 8192*180 - 1},
		{8192 * 500, 8192 * 100, 8192*499 + 1},
		{8192*500 - 32, 8192 * 100, 8192*500 - 32}, //align with block boundary
		{8192*500 + 50, 8192 * 100, 8192 * 500},
		{8192*500 + 50, 5000, 8192*500 + 50},
	}

	for _, s := range testSuits {
		t.Run(fmt.Sprintf("Size:%d, Pos:%d", s[0], s[2]), func(t *testing.T) {
			if err := doSeek(t, fs, s[0], int64(s[1]), int64(s[2])); err != nil {
				t.Errorf("Seek test failed:%s, size:%d, pos:%d", err, s[0], s[2])
			}
		})
	}
}
