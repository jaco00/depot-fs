/*
 rw_test.go

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
	"bytes"
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"os"
	"testing"
	"time"

	"github.com/jaco00/depot-fs/dpfs"
	"github.com/sirupsen/logrus"
)

const testDir = "./testdata"

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomFileName() string {
	length := mrand.Intn(30) + 1 // Generates a number between 1 and 30

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[mrand.Intn(len(charset))]
	}
	return string(b)
}

func randomBytes() []byte {
	length := mrand.Intn(2048) + 1
	b := make([]byte, length)
	_, err := rand.Read(b)
	if err != nil {
		logrus.Errorf("rand.Read failed :%s", err)
		return nil
	}
	return b
}

func doRW(t *testing.T, fs *dpfs.FileSystem, totalSize int64, batchLimit int) error {
	start := time.Now()
	rdp, err := dpfs.NewRandomDataProvider(int64(batchLimit), totalSize, true, true)
	if err != nil {
		return err
	}
	fn := randomFileName()
	meta := randomBytes()
	inodeptr, wtn, crc1, _, err := dpfs.WriteFile(fs, rdp, fn, meta, false)
	duration1 := time.Since(start)
	if err != nil {
		t.Errorf("test size:%d, write file failed :%s", totalSize, err)
		return err
	}

	dc, err := dpfs.NewNullDataConsumer(true)
	if err != nil {
		return err
	}
	start = time.Now()
	rdn, crc2, _, err2 := dpfs.ReadFile(fs, inodeptr, dc, int64(batchLimit), false)
	duration2 := time.Since(start)
	if err2 != nil {
		t.Errorf("test size:%d,read file failed :%s", totalSize, err2)
		return err
	}
	if fn != dc.Name {
		t.Errorf("Wrong file name [%s!=%s]", fn, dc.Name)
		return nil
	}
	if !bytes.Equal(meta, dc.Meta) {
		t.Errorf("Failed (wrong meta) [len:%d,%d]\n", len(meta), len(dc.Meta))
		return nil
	}
	if totalSize != wtn || totalSize != rdn {
		t.Errorf("Failed (wrong length) [size:%d,%d,%d]\n", totalSize, wtn, rdn)
		return nil
	}
	if crc1 != crc2 {
		t.Errorf("Failed (wrong crc) [size:%d,crc:%d!=%d]\n", totalSize, crc1, crc2)
	} else {
		t.Logf("Ok [size:%d,wcost:%s rcost:%s]\n", totalSize, duration1, duration2)
	}
	return nil
}

func TestRW(t *testing.T) {
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
		{4, 1024},
		{256, 1024},
		{4000, 4096},
		{4000, 1024},
		{4000, 1000},
		{8192, 1000},
		{8192, 2048},
		{8192, 8192},
		{8192 + 1, 8192},
		{9000, 1024},
		{9000, 1001},
		{8192 * 8, 4096},
		{8192 * 18, 100},
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
			if err := doRW(t, fs, s[0], int(s[1])); err != nil {
				t.Errorf("Failed on size %d and batchlimit %d: %v", s[0], s[1], err)
			}
		})
	}
}
