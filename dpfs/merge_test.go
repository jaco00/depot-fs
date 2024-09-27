/*
 test_merge.go

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
	"reflect"
	"testing"
)

func TestMerge(t *testing.T) {
	testCases := []struct {
		input          []uint32
		expectedOutput []Seg
		expectedCount  int
	}{
		{
			input:          []uint32{8 | 0x80000000},
			expectedOutput: []Seg{{offset: 1, length: 8}},
			expectedCount:  64,
		},
		{
			input:          []uint32{9 | 0x80000000},
			expectedOutput: []Seg{{offset: 1, length: 8}},
			expectedCount:  64,
		},
		{
			input:          []uint32{3, 4, 7, 8, 15},
			expectedOutput: []Seg{{offset: 0, length: 2}},
			expectedCount:  5,
		},
		{
			input:          []uint32{1, 2, 3, 4 | 0x80000000},
			expectedOutput: []Seg{{offset: 0, length: 9}},
			expectedCount:  67,
		},
	}

	for _, v := range testCases {
		seg, cnt := mergeSeg(v.input)
		if !reflect.DeepEqual(seg, v.expectedOutput) || cnt != v.expectedCount {
			t.Errorf("Merge Seg test: For input %v, expected output %v with count %d, but got output %v with count %d",
				v.input, v.expectedOutput, v.expectedCount, seg, cnt)
		}
	}
}
