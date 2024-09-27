/*
 wd_test.go

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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jaco00/depot-fs/dpfs"
	"github.com/sirupsen/logrus"
)

func TestWD(t *testing.T) {
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(testDir)

	fs, err := dpfs.MakeFileSystem(dpfs.MaxBlockGroupNum, dpfs.DefaultBlocksInGroup, testDir, "", "", 0, true)
	if err != nil {
		t.Fatalf("Failed to create file system: %v", err)
	}

	testSuits := [][]int64{
		{4, 1024},
		{4000, 1024},
		{4000, 1000},
		{8192, 1000},
		{8192, 2048},
		{8192, 8192},
		{8192 + 1, 8192},
		{9000, 1024},
		{9000, 1001},
		{8192 * 8, 4096},
		{8192*28 + 100, 8000},
		{8192*29 + 200, 3000},
		{8192 * 18, 8192 * 64},
		{8192 * 180, 8192 * 64},
		{8192 * 500, 8192 * 100},
		{8192*500 + 50, 8192 * 100},
		{8192*500 + 50, 5000},
	}

	for _, s := range testSuits {
		t.Run(fmt.Sprintf("Size:%d@BatchLimit:%d", s[0], s[1]), func(t *testing.T) {
			if err := doWD(t, fs, s[0], int(s[1]), nil); err != nil {
				t.Errorf("Failed on size %d and batchlimit %d: %v", s[0], s[1], err)
			}
		})
	}
}

func doWD(t *testing.T, fs *dpfs.FileSystem, totalSize int64, batchLimit int, data []byte) error {
	start := time.Now()
	_, fb := fs.StatBlocks(-1)
	_, fi := fs.StatInodes(-1)
	rdp, err := dpfs.NewRandomDataProvider(int64(batchLimit), totalSize, true, true)
	if err != nil {
		return err
	}
	key, wtn, _, _, err := dpfs.WriteFile(fs, rdp, "test.file", nil, false)

	duration1 := time.Since(start)
	if err != nil {
		logrus.Errorf("test size:%d, write file failed :%s", totalSize, err)
		return err
	}

	start = time.Now()
	if err := fs.DeleteFile(key); err != nil {
		logrus.Errorf("delete file failed :%s \n", err)
		return err
	}
	_, fb_ := fs.StatBlocks(-1)
	_, fi_ := fs.StatInodes(-1)
	duration2 := time.Since(start)
	if fi != fi_ {
		t.Errorf("Wrong free Inodes count(before,after) :%d!=%d", fi, fi_)
	} else if fb_ != fb {
		t.Errorf("Wrong free Blocks count(before,after) :%d!=%d", fb, fb_)
	} else {
		t.Logf("Ok [size:%d,wcost:%s dcost:%s]\n", wtn, duration1, duration2)
	}
	return nil
}
