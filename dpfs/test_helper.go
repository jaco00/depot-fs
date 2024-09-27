/*
 test_helper.go

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
	"crypto/rand"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"
)

type DataConsumer interface {
	Consume(data []byte) error
	OnMeta(name, key string, meta []byte) error
	Close() (uint32, error)
}

type DataProvider interface {
	Provide() ([]byte, error)
	Close() (uint32, error)
}

type FileDataProvider struct {
	data    []byte
	file    *os.File
	hash    hash.Hash32
	needCrc bool
}

type RamdomDataProvider struct {
	data         []byte
	totalSize    int64
	offset       int64
	genEachBatch bool
	hash         hash.Hash32
}

type FileDataConsumer struct {
	path string
	name string
	meta []byte
	file *os.File
	hash hash.Hash32
}

type NullDataConsumer struct {
	hash hash.Hash32
	Name string
	Meta []byte
}

func NewNullDataConsumer(needCrc bool) (*NullDataConsumer, error) {
	c := NullDataConsumer{}
	if needCrc {
		c.hash = crc32.New(crc32.MakeTable(crc32.IEEE))
	}
	return &c, nil
}

func (c *NullDataConsumer) Consume(data []byte) error {
	if c.hash != nil {
		c.hash.Write(data)
	}
	return nil
}

func (c *NullDataConsumer) OnMeta(name, key string, meta []byte) error {
	c.Name = name
	c.Meta = meta
	return nil
}

func (c *NullDataConsumer) Close() (uint32, error) {
	if c.hash != nil {
		return c.hash.Sum32(), nil
	} else {
		return 0, nil
	}
}

func NewFileDataConsumer(filePath, name string, needCrc bool) (*FileDataConsumer, error) {
	f := FileDataConsumer{
		path: filePath,
		name: name,
	}

	if needCrc {
		f.hash = crc32.New(crc32.MakeTable(crc32.IEEE))
	}
	return &f, nil
}

func (c *FileDataConsumer) OnMeta(name, key string, meta []byte) error {
	filePath := c.path
	if c.name != "" {
		filePath = filepath.Join(filePath, c.name)
	} else if name != "" {
		filePath = filepath.Join(filePath, name)
	} else {
		filePath = filepath.Join(filePath, key)
	}
	c.name = name
	c.meta = meta
	destDir := filepath.Dir(filePath)
	os.MkdirAll(destDir, os.ModePerm)
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	c.file = file
	return nil
}

func (f *FileDataConsumer) Consume(data []byte) error {
	if f.hash != nil {
		f.hash.Write(data)
	}
	_, err := f.file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write data to file: %v", err)
	}
	return nil
}

func (f *FileDataConsumer) Close() (uint32, error) {
	if f.file != nil {
		err := f.file.Close()
		f.file = nil
		if err != nil {
			return 0, fmt.Errorf("error closing file: %v", err)
		}
	}
	if f.hash != nil {
		return f.hash.Sum32(), nil
	} else {
		return 0, nil
	}
}

func NewRandomDataProvider(batchSize int64, totalSize int64, genEachBatch, needCrc bool) (*RamdomDataProvider, error) {
	r := RamdomDataProvider{
		totalSize:    totalSize,
		data:         make([]byte, batchSize),
		genEachBatch: genEachBatch,
	}
	if needCrc {
		r.hash = crc32.New(crc32.MakeTable(crc32.IEEE))
	}
	_, err := rand.Read(r.data)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func (r *RamdomDataProvider) Provide() ([]byte, error) {
	if r.offset >= r.totalSize {
		return nil, io.EOF
	}
	if r.genEachBatch && r.offset != 0 {
		_, err := rand.Read(r.data)
		if err != nil {
			return nil, err
		}
	}
	n := int64(len(r.data))
	if r.offset+int64(n) > r.totalSize {
		n = r.totalSize - r.offset
	}
	r.offset += n
	if r.hash != nil {
		r.hash.Write(r.data[:n])
	}
	return r.data[:n], nil
	//todo del
	/*
		slice := make([]byte, n)
		for i := range slice {
			slice[i] = 0x55
		}
		return slice, nil
	*/
}

func (r *RamdomDataProvider) Close() (uint32, error) {
	if r.hash != nil {
		return r.hash.Sum32(), nil
	} else {
		return 0, nil
	}
}

func NewFileDataProvider(filePath string, batchSize int64, needCrc bool) (*FileDataProvider, error) {
	r := FileDataProvider{
		data:    make([]byte, batchSize),
		needCrc: needCrc,
		hash:    crc32.New(crc32.MakeTable(crc32.IEEE)),
	}
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	r.file = file
	return &r, nil
}

func (f *FileDataProvider) Provide() ([]byte, error) {
	n, err := f.file.Read(f.data)
	if err != nil {
		return nil, err
	}
	if f.needCrc {
		f.hash.Write(f.data[:n])
	}
	return f.data[:n], nil
}

func (f *FileDataProvider) Close() (uint32, error) {
	if f.needCrc {
		return f.hash.Sum32(), nil
	} else {
		return 0, nil
	}
}

func WriteFile(fs *FileSystem, dp DataProvider, name string, meta []byte, echo bool) (string, int64, uint32, *Vfile, error) {
	var wtn int64 = 0
	f, key, err := fs.CreateFile(name, meta)
	if err != nil {
		fmt.Printf("Create file err:%s\n", err)
		return "", 0, 0, nil, err
	}
	start := time.Now()
	for {
		data, e := dp.Provide()
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
		w, e := f.Write(data)
		if e != nil {
			err = e
			break
		}
		if w == 0 || w != len(data) {
			err = fmt.Errorf("write error: incorrect data length, expected %d bytes but got %d bytes", len(data), w)
			break
		}
		wtn += int64(w)
	}
	elapsed := time.Since(start)
	if echo {
		fmt.Printf("File written: [Name: %s, Size: %s, Time: %.3f s]\n", name, FormatBytes(wtn), elapsed.Seconds())
	}
	sum, _ := dp.Close()
	return key, wtn, sum, f, err
}

func ReadFile(fs *FileSystem, key string, dc DataConsumer, batchLimit int64, echo bool) (int64, uint32, *Vfile, error) {
	f, err := fs.OpenFile(key)
	if err != nil {
		return 0, 0, nil, err
	}
	if err := dc.OnMeta(f.Meta.Name, key, f.Meta.ExtMetas); err != nil {
		return 0, 0, nil, err
	}
	data := make([]byte, batchLimit)
	var offset int64 = 0
	defer dc.Close()
	start := time.Now()
	for offset < int64(f.Inode.FileSize) {
		if offset+batchLimit > int64(f.Inode.FileSize) {
			batchLimit = int64(f.Inode.FileSize) - offset
		}
		r, err := f.Read(data[:batchLimit])
		if err != nil {
			return 0, 0, f, err
		}
		if r == 0 {
			return 0, 0, f, errors.New("read nil")
		}
		offset += int64(r)
		if err := dc.Consume(data[:r]); err != nil {
			return 0, 0, f, err
		}
	}
	elapsed := time.Since(start)
	if echo {
		fmt.Printf("File read: [Name: %s, Size: %s, Time: %.3f s]\n", f.Meta.Name, FormatBytes(offset), elapsed.Seconds())
	}
	sum, err := dc.Close()
	return int64(offset), sum, f, err
}
