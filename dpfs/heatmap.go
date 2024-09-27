/*
 heatmap.go

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
	"fmt"
	"math/bits"
)

const (
	DefaultHMWidth = 128
)

func MakeHeatMap(bitmap []uint8, height int, calc func(bitmap []uint8) float32) *HeatMap {
	defCalc := func(bitmap []uint8) float32 {
		ret := 0
		for _, i := range bitmap {
			ret += bits.OnesCount8(i)
		}
		return float32(ret) / (float32(len(bitmap)) * 8.0)
	}
	m := HeatMap{
		bitmap: bitmap,
		height: height,
		width:  DefaultHMWidth,
		calc:   defCalc,
	}
	if calc != nil {
		m.calc = calc
	}
	return &m
}

type HeatMap struct {
	bitmap []uint8
	width  int
	height int
	calc   func(bitmap []uint8) float32
}

func (h *HeatMap) Draw() {
	totalCell := h.width * h.height
	cellSize := len(h.bitmap) / totalCell
	//fmt.Printf("total data: %d,  graph size :%d, pot size: %d\n", len(bitmap), totalBlk, blkSize)
	for i := 0; i < h.height; i++ {
		for j := 0; j < h.width; j++ {
			v := h.calc(h.bitmap[(i*h.width+j)*cellSize : (i*h.width+j)*cellSize+cellSize])
			if v < 0.0001 { //
				fmt.Printf("█")
			} else if v < 0.2 { //green
				fmt.Printf("\033[92m█\033[0m")
			} else if v < 0.6 { //yellow
				fmt.Printf("\033[38;5;226m█\033[0m")
			} else if v < 0.85 { //orange
				fmt.Printf("\033[38;5;214m█\033[0m")
			} else { //red
				fmt.Printf("\033[31m█\033[0m")
			}
		}
		fmt.Println("")
	}
}
