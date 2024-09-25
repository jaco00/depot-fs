/*
 bitmap_test.go

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
	"crypto/rand"
	"fmt"
	"math/big"
	"reflect"
	"testing"
	"time"
)

var (
	size = 100 * 1024 * 1024
)

func allocBits(t *testing.T, name string, bm Bitmap, cnt, batchSize int, checkBig, checkMm, needRet bool) []uint32 {
	start := time.Now()
	bigAlloc := 0
	alloc := 0
	var r []uint32
	for i := 0; i < cnt; i++ {
		lst, n := bm.AllocBits(batchSize, batchSize, true)
		alloc += n
		if n != batchSize {
			if checkMm {
				t.Errorf("Bad alloc need:%d alloc:%d ptr:%d", batchSize, n, len(lst))
			}
			if n == 0 {
				break
			}
		}
		if checkBig {
			bigAlloc += countBig(lst)
		}
		if needRet {
			r = append(r, lst...)
		}
	}
	elapsed := time.Since(start)
	ba := ""
	if checkBig {
		ba = fmt.Sprintf("<BigAlloc:%d(%d)>", bigAlloc, bigAlloc*64)
	}
	fmt.Printf("SequentialAlloc [%s] took:%s, need:%d, alloc:%d%s, batch:%d\n", name, elapsed, cnt*batchSize, alloc, ba, batchSize)
	return r
}

func alternatingAlloc(t *testing.T, name string, bm Bitmap, totalSize, batchSize int) {
	alloc := 0
	bigAlloc := 0
	start := time.Now()
	for {
		lst, n := bm.AllocBits(1, 1, false)
		if n != 1 {
			t.Errorf("Alloc failed")
			break
		}
		alloc++
		if alloc >= totalSize {
			break
		}
		lst, n = bm.AllocBits(batchSize, batchSize, true)
		bigAlloc += countBig(lst)
		alloc += n
		if alloc >= totalSize {
			break
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("AlternatingAlloc [%s] took:%s, limit:%d, alloc:%d<BigAlloc:%d(%d)>, batch:%d\n", name, elapsed, totalSize, alloc, bigAlloc, bigAlloc*64, batchSize)
}

func TestRandomAlloc(t *testing.T) {
	ma := make([]uint8, size/8)
	mb := make([]uint8, size/8)
	_, err := rand.Read(ma)
	if err != nil {
		t.Errorf("gen random data failed:%v", err)
		return
	}
	copy(mb, ma)

	bm1 := BitmapBase{}
	bm2 := Bitmap64{}
	bm1.Init(1, ma)
	bm2.Init(1, mb)

	batchSize := 100 * 1024
	allocBits(t, "Random perf(byte)", &bm1, 10, batchSize, true, false, false)
	allocBits(t, "Random perf(ui64)", &bm2, 10, batchSize, true, false, false)
}

func TestBitmapAlloc(t *testing.T) {
	batchSize := 10 * 1024
	cnt := size / batchSize
	bm1 := BitmapBase{}
	bm2 := Bitmap64{}
	bm1.Init(1, make([]uint8, size/8))
	bm2.Init(1, make([]uint8, size/8))
	allocBits(t, "Base alloc(byte)", &bm1, cnt, batchSize, false, true, false)
	allocBits(t, "Base alloc(ui64)", &bm2, cnt, batchSize, false, true, false)

}

func TestBitmapBigAlloc(t *testing.T) {
	batchSize := 10 * 1024
	cnt := size / batchSize
	bm1 := BitmapBase{}
	bm2 := Bitmap64{}
	bm1.Init(1, make([]uint8, size/8))
	bm2.Init(1, make([]uint8, size/8))

	bm1.AllocBits(1, 1, false)
	bm2.AllocBits(1, 1, false)

	allocBits(t, "BigAlloc(byte)", &bm1, cnt, batchSize, true, false, false)
	allocBits(t, "BigAlloc(ui64)", &bm2, cnt, batchSize, true, false, false)
}

func countBig(lst []uint32) int {
	bigAlloc := 0
	for _, i := range lst {
		_, _, isBig := EntAddr(i).GetAddr()
		if isBig > 0 {
			bigAlloc++
		}
	}
	return bigAlloc
}

func setRandomBits(slice []byte, numBits int) {
	sliceLen := len(slice) * 8

	for i := 0; i < numBits; i++ {
		pos, _ := rand.Int(rand.Reader, big.NewInt(int64(sliceLen)))
		byteIndex := pos.Int64() / 8
		bitOffset := pos.Int64() % 8
		slice[byteIndex] |= 1 << bitOffset
	}
}

func TestFuzzingRamdom(t *testing.T) {
	d1 := make([]uint8, size/8)
	d2 := make([]uint8, size/8)
	_, err := rand.Read(d1)
	if err != nil {
		t.Errorf("gen random data failed:%v", err)
		return
	}
	copy(d2, d1)

	bm1 := BitmapBase{}
	bm2 := Bitmap64{}
	bm1.Init(0, d1)
	bm2.Init(0, d2)
	batchSize := 100 * 1024
	lst1 := allocBits(t, "Test Fuzzing Ramdom(byte)", &bm1, 1, batchSize, true, false, true)
	lst2 := allocBits(t, "Test Fuzzing Ramdom(ui64)", &bm2, 1, batchSize, true, false, true)
	if !reflect.DeepEqual(lst1, lst2) {
		t.Error("The allocated pos is inconsistent.")
	}
	if !reflect.DeepEqual(d1, d2) {
		t.Error("The allocated bitmap is inconsistent.")
	}
}

func TestFuzzingSparseArray(t *testing.T) {
	batchSize := 10 * 1024
	bm1 := BitmapBase{}
	bm2 := Bitmap64{}
	d1 := make([]uint8, size/8)
	d2 := make([]uint8, size/8)
	setRandomBits(d1, 10*1024)

	copy(d2, d1)

	bm1.Init(1, d1)
	bm2.Init(1, d2)

	lst1 := allocBits(t, "Test Fuzzing Sparse Array(byte)", &bm1, 1, batchSize, true, false, true)
	lst2 := allocBits(t, "Test Fuzzing Sparse Array(ui64)", &bm2, 1, batchSize, true, false, true)
	if !reflect.DeepEqual(lst1, lst2) {
		t.Error("The allocated pos is inconsistent.")
	}
	if !reflect.DeepEqual(d1, d2) {
		t.Error("The allocated bitmap is inconsistent.")
	}
}

func TestBitmapBigAlloc2(t *testing.T) {
	batchSize := 1024
	bm1 := BitmapBase{}
	bm2 := Bitmap64{}
	bm1.Init(1, make([]uint8, size/8))
	bm2.Init(1, make([]uint8, size/8))
	alternatingAlloc(t, "byte", &bm1, size, batchSize)
	alternatingAlloc(t, "ui64", &bm2, size, batchSize)
}
