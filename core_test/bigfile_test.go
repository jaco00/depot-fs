//go:build full

/*
 bigfile_test.go

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
	"fmt"
	"os"
	"testing"
)

func TestBigFile(t *testing.T) {
	var batchSize int = 1024 * 1024
	var fileSize int64 = 2 * 1024 * 1024 * 1024

	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(testDir)
	var group uint32 = 32
	fs, err := core.MakeFileSystem(group, 2*256*1024, testDir, "", "", 0, true)
	if err != nil {
		t.Fatalf("Failed to create file system: %v", err)
	}
	fmt.Printf("Runing big file testing...\n")
	if err := doRW(t, fs, fileSize, batchSize); err != nil {
		t.Errorf("Failed on big file test: %v", err)
	}
	return
}
