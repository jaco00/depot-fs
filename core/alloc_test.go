/*
 alloc_test.go

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
	"os"
	"testing"
)

const testDir = "./testdata"

func doAlloc(t *testing.T, fs *FileSystem, need int) {
	_, fb := fs.StatBlocks(-1)
	blks, cnt, err := fs.allocBlocks(need, need, true)
	if err != nil {
		t.Errorf("alloc blocks failed:%v", err)
		return
	}
	_, fb2 := fs.StatBlocks(-1)
	t.Logf("total blocks:%d, need blocks:%d, alloced:%d, blocks:%d, free blocks:%d\n", fb, need, cnt, len(blks), fb2)
	if fb2+int64(need) != fb {
		t.Errorf("alloc blocks failed. %d(alloc)+%d(free)!=%d(total)", need, fb2, fb)
		return
	}
}

func TestAlloc(t *testing.T) {
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(testDir)

	fs, err := MakeFileSystem(8, DefaultBlocksInGroup, testDir, "", "", 0, true)
	if err != nil {
		t.Fatalf("Failed to create file system: %v", err)
	}

	fb := fs.Smeta.TotalBlocks()
	testSuits := []int{
		int(float32(fb) * 0.1),
		int(float32(fb) * 0.5),
		int(float32(fb) * 0.2),
	}
	for _, s := range testSuits {
		t.Run(fmt.Sprintf("AllocTest:%d blocks", s), func(t *testing.T) {
			doAlloc(t, fs, s)
		})
	}
	return
}
