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

package core_test

import (
	"depotFS/core"
	"errors"
	"fmt"
	"os"
	"testing"
)

func TestSeekWrite(t *testing.T) {
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(testDir)

	var group uint32 = 32
	fs, err := core.MakeFileSystem(group, 2*256*1024, testDir, "", "", 0, true)
	if err != nil {
		t.Fatalf("Failed to create file system: %v", err)
	}

	testSuits := [][]int64{
		{8192, 1024, 8192},
		{8192 * 20, 8192 * 10, 8192*50 + 10},
		{8192*2 - 32, 8192 - 32, 5000},
		{8192*500 - 32, 8192*200 - 32, 8192 * 500},
	}

	for _, s := range testSuits {
		t.Run(fmt.Sprintf("Init Size:%d,Seek pos:%d,Append Size:%d", s[0], s[1], s[2]), func(t *testing.T) {
			if err := doSeekAndWrite(fs, s[0], s[1], s[2]); err != nil {
				t.Errorf("Seek_Write test failed:%s, Init Size:%d, Seek pos:%d, Append Size:%d", err, s[0], s[2], s[3])
			}
		})
	}
}

func initFile(fs *core.FileSystem, fileSize int64) (*core.Vfile, string, error) {
	fn := "test.io"
	meta := make([]byte, 10)
	f, key, err := fs.CreateFile(fn, meta)
	if err != nil {
		fmt.Printf("open file err:%s\n", err)
		return nil, "", err
	}
	buffer := make([]byte, fileSize)
	for i := int64(0); i < fileSize; i++ {
		buffer[i] = byte(i % 256)
	}

	wtn, e := f.Write(buffer)
	if e != nil {
		return f, key, e
	}
	if wtn != len(buffer) {
		fmt.Printf("Write test file failed, bad size!  %d!=%d\n", wtn, fileSize)
		return f, key, errors.New("bad size")
	}
	return f, key, nil
}

func seekAndAppend(f *core.Vfile, pos int64, writeLen int64) error {
	_, err := f.SeekPos(pos)
	if err != nil {
		fmt.Printf("Seek failed: %s\n", err)
		return err
	}
	//write new data
	buffer2 := make([]byte, writeLen)
	for i := 0; i < len(buffer2); i++ {
		buffer2[i] = byte(i%256) + 20
	}
	wtn, err := f.Write(buffer2)
	if err != nil {
		return err
	}
	if wtn != len(buffer2) {
		fmt.Printf("Write test file failed2, bad size!  %d!=%d\n", wtn, len(buffer2))
		return errors.New("bad size")
	}
	return nil
}

func checkFile(fs *core.FileSystem, key string, fileSize int64, pos int64, writeLen int64) error {
	f2, err := fs.OpenFile(key)
	if err != nil {
		fmt.Printf("Open file failed:%s\n", err)
		return err
	}
	f2.SeekPos(0)
	nfSize := pos + writeLen
	if nfSize < fileSize {
		nfSize = fileSize
	}
	ans := make([]byte, nfSize)
	rdn, err := f2.Read(ans)
	if err != nil {
		fmt.Printf("Read error:%s\n", err)
		return err
	}
	if rdn != len(ans) {
		fmt.Printf("Read size error %d!=%d\n", rdn, len(ans))
	}
	for i, v := range ans {
		if i < int(pos) || i >= int(pos+writeLen) {
			if v != byte(i%256) {
				fmt.Printf("File data checking failed!\n")
				return nil
			}
		} else {
			if v != byte((i-int(pos))%256+20) {
				fmt.Printf("i:%d pos:%d  f2:%d  \n", i, pos, pos+nfSize)
				fmt.Printf("File data checking failed!  pos:%d %d!=%d\n", i, v, (i-int(pos))%256+20)
				return errors.New("File data checking failed")
			}
		}
	}
	return nil
}

func doSeekAndWrite(fs *core.FileSystem, fileSize int64, pos int64, writeLen int64) error {
	f, key, err := initFile(fs, fileSize)
	if err != nil {
		return err
	}
	//write new data
	if err := seekAndAppend(f, pos, writeLen); err != nil {
		return nil
	}
	//check file
	if err := checkFile(fs, key, fileSize, pos, writeLen); err != nil {
		return err
	}
	return nil
}
