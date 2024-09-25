/*
 cache.go

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
	"container/list"
)

const (
	BlockCacheSize = 128
)

type CacheLayer struct {
	capacity int
	cache    map[uint32]*list.Element
	list     *list.List
}

type CachedBlock struct {
	blockPtr uint32
	data     any
}

func NewCacheLayer(capacity int) *CacheLayer {
	return &CacheLayer{
		capacity: capacity,
		cache:    make(map[uint32]*list.Element),
		list:     list.New(),
	}
}

func (c *CacheLayer) Get(blockPtr uint32) (any, bool) {
	if elem, found := c.cache[blockPtr]; found {
		c.list.MoveToFront(elem)
		return elem.Value.(*CachedBlock).data, true
	}
	return nil, false
}
func (c *CacheLayer) Put(blockPtr uint32, data any) {
	if elem, found := c.cache[blockPtr]; found {
		elem.Value.(*CachedBlock).data = data
		c.list.MoveToFront(elem)
		return
	}

	if c.list.Len() == c.capacity {
		backElem := c.list.Back()
		if backElem != nil {
			c.list.Remove(backElem)
			delete(c.cache, backElem.Value.(*CachedBlock).blockPtr)
		}
	}

	newElem := c.list.PushFront(&CachedBlock{blockPtr: blockPtr, data: data})
	c.cache[blockPtr] = newElem
}

type BlockCache struct {
	lv1, lv2, lv3 *CacheLayer
}

func NewBlockCache() *BlockCache {
	return &BlockCache{
		lv1: NewCacheLayer(BlockCacheSize),
		lv2: NewCacheLayer(BlockCacheSize),
		lv3: NewCacheLayer(BlockCacheSize),
	}
}

func (m *BlockCache) Get(level int, blockPtr uint32) (any, bool) {
	switch level {
	case SingleIndirectLv:
		return m.lv1.Get(blockPtr)
	case DoubleIndirectLv:
		return m.lv2.Get(blockPtr)
	case TripleIndirectLv:
		return m.lv3.Get(blockPtr)
	default:
		return nil, false
	}
}

func (m *BlockCache) Put(level int, blockPtr uint32, data any) {
	switch level {
	case SingleIndirectLv:
		m.lv1.Put(blockPtr, data)
	case DoubleIndirectLv:
		m.lv2.Put(blockPtr, data)
	case TripleIndirectLv:
		m.lv3.Put(blockPtr, data)
	}
}
