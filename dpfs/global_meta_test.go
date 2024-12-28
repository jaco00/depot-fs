/*
 global_meta_test.go

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
	"bytes"
	"fmt"
	"testing"
)

func TestNewGlobalMeta(t *testing.T) {
	testData := []byte("this is a test data")

	meta, err := NewGlobalMeta(1, testData)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if meta == nil {
		t.Fatal("expected non-nil meta")
	}

	if meta.Version != 1 {
		t.Errorf("expected version 1, got %d", meta.Version)
	}
	if meta.DataLen != uint32(len(testData)) {
		t.Errorf("expected DataLen %d, got %d", len(testData), meta.DataLen)
	}

	if !bytes.Equal(meta.Data[:len(testData)], testData) {
		t.Errorf("expected data %v, got %v", testData, meta.Data[:len(testData)])
	}
}

func TestNewGlobalMeta_DataExceedsMaxSize(t *testing.T) {
	testData := make([]byte, MaxGlobalMetaSize+1)
	meta, err := NewGlobalMeta(1, testData)
	if err == nil {
		t.Fatal("expected error for data size exceeding max size")
	}
	if meta != nil {
		t.Fatal("expected nil meta when error occurs")
	}
}

func TestExtractData_Success(t *testing.T) {
	testData := []byte("this is another test data")
	meta, _ := NewGlobalMeta(2, testData)
	version, data, err := meta.ExtractData()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if version != 2 {
		t.Errorf("expected version 2, got %d", version)
	}
	if !bytes.Equal(data, testData) {
		t.Errorf("expected data %v, got %v", testData, data)
	}
}

func TestExtractData_CRCMismatch(t *testing.T) {
	testData := []byte("this is a test data")
	meta, _ := NewGlobalMeta(1, testData)

	meta.Data[0] = 'X'

	_, _, err := meta.ExtractData()

	if err == nil {
		t.Fatal("expected error due to CRC mismatch")
	}

	if err.Error() != "crc mismatch" {
		t.Errorf("expected 'crc mismatch' error, got %v", err)
	}
}
func TestNullMeta(t *testing.T) {
	meta, _ := NewGlobalMeta(0, nil)
	_, data, err := meta.ExtractData()
	fmt.Printf("Extract null meta :%v\n", data)
	if err != nil {
		t.Fatal("null meta verify failed")
	}
}
