/*
 volume.go

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
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/bits"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	InodeBitmapOffset int64 = 0
	BlockBitmapOffset int64 = 0
	InodeOffset       int64 = 0
	BlockOffset       int64 = 0
	BlockPointers     int   = 0
)

func align(value, alignment int64) int64 {
	if alignment == 0 || (alignment&(alignment-1)) != 0 {
		panic("alignment must be a power of 2")
	}
	return (value + (alignment - 1)) & ^(alignment - 1)
}

// smeta+gmeta+inodeBitmap+blockBitmap+inode+blocks
type Volume struct {
	Status int
	Id     int
	Fn     string
	file   *os.File
}

func (v *Volume) GetSize() int64 {
	if v.file == nil {
		return 0
	}
	fileInfo, err := v.file.Stat()
	if err != nil {
		logrus.Warnf("Error getting file info:%s\n", err)
		return -1
	}
	return fileInfo.Size()
}

type VolumeFiles struct {
	root    string
	pattern string
	tpl     string
	smeta   SuperBlock
	//vols    int
	volumes []Volume
	groups  []BlockGroup
}

func countBits(data []byte) int {
	count := 0
	for _, b := range data {
		count += bits.OnesCount8(b)
	}
	return count
}

func SortFileNameAscend(files []os.FileInfo) {
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})
}

func (v *VolumeFiles) FindLastVolumeIdx() uint32 {
	var idx uint32 = 0
	for i, e := range v.volumes {
		if e.Status > 0 {
			idx = uint32(i)
		}
	}
	return idx
}

func (v *VolumeFiles) loadMeta(files []string) error {
	if len(files) == 0 {
		return nil //use setup values
	}
	for _, f := range files {
		file, err := os.OpenFile(f, os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		smeta := SuperBlock{}
		if err := binary.Read(file, binary.LittleEndian, &smeta); err != nil {
			file.Close()
			return err
		}
		if err := smeta.Verify(); err != nil {
			logrus.Errorf("Super block error :%s", err)
			file.Close()
		} else {
			v.smeta = smeta
			return nil
		}
	}
	return errors.New("Super block not found")
}

func (v *VolumeFiles) initParas() {
	InodeBitmapOffset = int64(binary.Size(SuperBlock{}) + binary.Size(BlockGroupDescriptor{}))
	//BlockBitmapOffset = InodeBitmapOffset + int64(len(v.groups[0].inodeBitmap))
	v.smeta.TotalInodes()
	BlockBitmapOffset = InodeBitmapOffset + int64(v.smeta.BlocksInGroup/v.smeta.InodesRatio)/8
	InodeOffset = BlockBitmapOffset + int64(v.groups[0].blockBitmap.TotalBits()/8)
	inodecap := int64(binary.Size(Inode{})) * int64(v.smeta.BlocksInGroup/v.smeta.InodesRatio)
	BlockOffset = InodeOffset + inodecap

	BlockPointers = int(v.smeta.BlockSize) / 4
}

func (v *VolumeFiles) initGroups() {
	v.volumes = make([]Volume, v.smeta.TotalGroups)
	for i := 1; i <= len(v.volumes); i++ {
		v.volumes[i-1].Id = i
		v.volumes[i-1].Fn = fmt.Sprintf(v.tpl, i)
	}
	v.groups = make([]BlockGroup, v.smeta.TotalGroups)
	ninode := v.smeta.BlocksInGroup / v.smeta.InodesRatio
	for i := range v.groups { //fill data later
		v.groups[i] = BlockGroup{
			//blockBitmap: make([]uint8, v.smeta.BlocksInGroup/8),
			gmeta: BlockGroupDescriptor{
				GroupId: uint32(i + 1),
			},
		}
		v.groups[i].blockBitmap.Init(uint32(i+1), make([]uint8, v.smeta.BlocksInGroup/8))
		v.groups[i].inodeBitmap.Init(uint32(i+1), make([]uint8, ninode/8))
	}
}

func (v *VolumeFiles) scanFiles() (int, error) {
	pattern := regexp.MustCompile(v.pattern)

	d, err := os.Open(v.root)
	if err != nil {
		return 0, err
	}
	defer d.Close()
	files, err := d.Readdir(-1)
	if err != nil {
		log.Fatal(err)
	}

	SortFileNameAscend(files)
	gfs := []string{}
	for _, file := range files {
		if !file.IsDir() && pattern.MatchString(file.Name()) {
			gfs = append(gfs, filepath.Join(v.root, file.Name()))
		}
	}

	if err := v.loadMeta(gfs); err != nil {
		return 0, err
	}
	v.initGroups()
	v.initParas()

	start := time.Now()
	for _, file := range gfs {
		if err := v.initVolume(file); err != nil {
			return 0, err
		}
	}
	duration := time.Since(start)
	logrus.Infof("load %d volume files, cost:%s", len(gfs), duration)
	return len(gfs), nil
}

func (v *VolumeFiles) initVolume(fn string) error {
	//vv := &v.volumes[idx]
	file, err := os.OpenFile(fn, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	smeta := SuperBlock{}
	if err := binary.Read(file, binary.LittleEndian, &smeta); err != nil {
		return err
	}
	if smeta.Crc != v.smeta.Crc {
		logrus.Errorf("Bad super block in file :%s", fn)
		return errors.New("Bad super block found")
	}

	meta := BlockGroupDescriptor{}
	if err := binary.Read(file, binary.LittleEndian, &meta); err != nil {
		return err
	}
	//re gen meta
	bitsI := make([]uint8, v.groups[meta.GroupId-1].inodeBitmap.TotalBits()/8)
	if _, err := file.Read(bitsI); err != nil {
		return err
	}
	v.groups[meta.GroupId-1].inodeBitmap.Init(meta.GroupId, bitsI)

	bitsB := make([]uint8, v.groups[meta.GroupId-1].blockBitmap.TotalBits()/8)
	if _, err := file.Read(bitsB); err != nil {
		return err
	}
	v.groups[meta.GroupId-1].blockBitmap.Init(meta.GroupId, bitsB)

	totalBlocks := v.groups[meta.GroupId-1].blockBitmap.TotalBits()
	freeBlocks := v.groups[meta.GroupId-1].blockBitmap.FreeBits()

	totalInodes := v.groups[meta.GroupId-1].inodeBitmap.TotalBits()
	freeInodes := v.groups[meta.GroupId-1].inodeBitmap.FreeBits()

	logrus.Debugf("load group file [grpid:%d, used blocks:%d/%d, used inodes:%d/%d]",
		meta.GroupId,
		totalBlocks-int(freeBlocks), totalBlocks,
		totalInodes-int(freeInodes), totalInodes,
	)
	v.groups[meta.GroupId-1].gmeta = meta
	v.volumes[meta.GroupId-1].Status = 1
	v.volumes[meta.GroupId-1].file = file
	return nil
}

func (v *VolumeFiles) checkReady(idx uint32, g *BlockGroup) error { //todo fix
	vv := &v.volumes[idx]
	var err error
	if vv.Status == 0 {
		//init file
		vv.file, err = os.Create(filepath.Join(v.root, vv.Fn))
		if err != nil {
			return err
		}
		v.smeta.Sign()
		if err := binary.Write(vv.file, binary.LittleEndian, v.smeta); err != nil {
			return err
		}
		if err := binary.Write(vv.file, binary.LittleEndian, g.gmeta); err != nil {
			return err
		}

		dataI := g.inodeBitmap.GetData(-1, 0)
		if _, err := vv.file.Write(dataI); err != nil {
			return err
		}

		dataB := g.blockBitmap.GetData(-1, 0)
		if _, err := vv.file.Write(dataB); err != nil {
			return err
		}

		zeroBytes := make([]byte, InodeSize*int(8*len(dataI)))
		if _, err := vv.file.Write(zeroBytes); err != nil {
			return err
		}

		vv.Status = 1
		if err := vv.file.Sync(); err != nil {
			return err
		}
	} else {
		if vv.file == nil {
			vv.file, err = os.Open(filepath.Join(v.root, vv.Fn))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *VolumeFiles) Init(root, pattern, tpl string, smeta SuperBlock, groups []BlockGroup) error {
	v.root = root
	v.groups = groups
	if pattern == "" {
		v.pattern = DefaultVfPattern
	} else {
		v.pattern = pattern
	}
	if tpl == "" {
		v.tpl = DefaultVfTpl
	} else {
		v.tpl = tpl
	}
	v.smeta = smeta //set default
	//scan file
	if n, err := v.scanFiles(); err != nil {
		return err
	} else if n > 0 {
		logrus.Debugf("Overwrite the original superblock by reading parameters from the specified file")
	}
	return nil
}
