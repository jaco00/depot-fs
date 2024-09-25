/*
 fs.go

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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/sirupsen/logrus"
)

/*
  block 8kb
  block 8GB  bit map 1048576  16384*8byte  Node ratio 2:1   ,node 8192*8byte
  block group number limit 1000
  total size 8000GB
  memcory cache size 1000(8192*8+16384*8+64)MB  187MB
*/

const (
	CacheCapacity    = 256
	MaxBlocksPtr     = 8
	DefaultVfPattern = `^vol.\d{6}$`
	DefaultVfTpl     = `vol.%06d`
	DirectBlocks     = 8
)

const (
	BlocksOverlimit = 10
)

const (
	SingleIndirectLv = 1
	DoubleIndirectLv = 2
	TripleIndirectLv = 3
)

const (
	MaxFileMetaSize = 2048
	FileMetaAlign   = 16
)

var (
	InodeSize = binary.Size(Inode{})
)

var FNF = errors.New("File not found")

var BAD_UID = errors.New("Bad UID for file")
var BAD_GID = errors.New("Bad GID") //bad group id

type BlockGroupDescriptor struct {
	GroupId uint32
}

type BlockGroup struct {
	gmeta       BlockGroupDescriptor
	inodeBitmap Bitmap64
	blockBitmap Bitmap64
}

type FileSystem struct {
	Smeta          SuperBlock
	curBlockGroups uint32 //index
	blockGroups    []BlockGroup
	device         *VolumeFiles
	ibCache        *BlockCache
}

type FileMeta struct {
	Name     string
	ExtMetas []byte
}

func (m *FileMeta) ToBytes() ([]byte, error) {
	var buf bytes.Buffer

	nameLen := int32(len(m.Name))
	if err := binary.Write(&buf, binary.LittleEndian, nameLen); err != nil {
		return nil, err
	}

	if nameLen > 0 {
		if _, err := buf.WriteString(m.Name); err != nil {
			return nil, err
		}
	}

	extMetasLen := int32(len(m.ExtMetas))
	if err := binary.Write(&buf, binary.LittleEndian, extMetasLen); err != nil {
		return nil, err
	}
	if extMetasLen > 0 {
		if _, err := buf.Write(m.ExtMetas); err != nil {
			return nil, err
		}
	}

	//padding
	padding := FileMetaAlign - buf.Len()%FileMetaAlign
	if padding != FileMetaAlign {
		padText := bytes.Repeat([]byte{byte(0)}, padding)
		if _, err := buf.Write(padText); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

type FileSnap struct {
	Key    string
	Inode  uint32
	Name   string
	Meta   []byte
	Size   int64
	CTime  uint64
	MTime  uint64
	FileId uint64
}

func (m *FileMeta) FromBytes(data []byte) error {
	m.Name = ""
	m.ExtMetas = nil
	buf := bytes.NewReader(data)

	var nameLen int32
	if err := binary.Read(buf, binary.LittleEndian, &nameLen); err != nil {
		return err
	}
	if nameLen >= MaxFileMetaSize {
		return fmt.Errorf("Bad File name len: %d\n", nameLen)
	}
	if nameLen > 0 {
		name := make([]byte, nameLen)
		if _, err := buf.Read(name); err != nil {
			return err
		}
		m.Name = string(name)
	}

	var extMetasLen int32
	if err := binary.Read(buf, binary.LittleEndian, &extMetasLen); err != nil {
		return err
	}
	if extMetasLen > 0 {
		m.ExtMetas = make([]byte, extMetasLen)
		if _, err := buf.Read(m.ExtMetas); err != nil {
			return err
		}
	}

	return nil
}

type Inode struct {
	Seq            uint32
	Attr           uint16
	MetaSize       uint16               //4
	Blocks         uint32               //8
	FileSize       uint64               //16 file size in bytes
	CTime          uint64               //24 creation time
	MTime          uint64               //32 creation time
	DirectPointers [DirectBlocks]uint32 //96
	SingleIndirect uint32               //100
	DoubleIndirect uint32               //104
	TripleIndirect uint32               //108
}

func (i *Inode) DataSize() uint64 {
	return uint64(i.MetaSize) + i.FileSize
}

// MakeFileSystem initializes and creates a new file system instance.
// It sets up the underlying structure based on the specified parameters,
// allowing for efficient file management and storage operations.
//
// Parameters:
//   - groupNum (uint32): The number of data files in the file system.
//     Suggested values are between 16 and 256.
//   - blocksInGroup (uint32): The number of blocks per allocation group.
//     Use 0 for default value (1M).
//   - root (string): The root directory for the data file.
//   - pattern (string): A regular expression used to identify data files
//     during initialization. It can be left empty to use the default value.
//   - tpl (string): A template string for generating underlying data file names.
//     It can be left empty to use the default value.
//   - shardId (uint16): Used in distributed systems as part of the unique
//     ID generation for files.
//   - enableBigAlloc (bool): A flag indicating whether to enable large
//     allocation for improved performance.
//
// Returns:
//   - *FileSystem: A pointer to the newly created FileSystem instance.
//   - error: An error if the creation fails. If successful, the file system
//     is ready for use.

func MakeFileSystem(groupNum, blocksInGroup uint32, root, pattern, tpl string, shardId uint16, enableBigAlloc bool) (*FileSystem, error) {
	if blocksInGroup == 0 {
		blocksInGroup = DefaultBlocksInGroup
	}
	fs := FileSystem{
		Smeta: SuperBlock{
			BlockSize:     DefaultBlockSize,
			TotalGroups:   groupNum,
			BlocksInGroup: blocksInGroup,
			InodesRatio:   DefaultInodesRatio,
			ShardId:       shardId,
		},
		device:  &VolumeFiles{},
		ibCache: NewBlockCache(),
	}
	if enableBigAlloc {
		fs.Smeta.EnableBigAlloc()
	}
	if err := fs.device.Init(root, pattern, tpl, fs.Smeta, fs.blockGroups); err != nil {
		return nil, err
	}
	fs.Smeta = fs.device.smeta
	fs.blockGroups = fs.device.groups
	fs.curBlockGroups = 0
	logrus.Debugf("set current group idx:%d", fs.curBlockGroups)
	logrus.Infof(
		"Init file system <Total space: %d GB, Block: %d, Blocksize: %d, Group: %d, INodeSize: %d, TotalInodes: %d>",
		fs.Smeta.TotalSpace()/(1024*1024*1024),
		fs.Smeta.TotalBlocks(),
		fs.Smeta.BlockSize,
		fs.Smeta.TotalGroups,
		binary.Size(Inode{}),
		fs.Smeta.TotalInodes(),
	)
	return &fs, nil
}

func (f *FileSystem) GetVolumeInfo(idx int) *Volume {
	if idx < 0 || idx >= int(f.Smeta.TotalGroups) {
		return nil
	}
	return &f.device.volumes[idx]
}

func (f *FileSystem) GetBlockBitmap(idx int) []uint8 {
	if idx < 0 || idx >= int(f.Smeta.TotalGroups) {
		return nil
	}
	return f.blockGroups[idx].blockBitmap.GetData(-1, 0)
}

func (f *FileSystem) GetInodeBitmap(idx int) []uint8 {
	if idx < 0 || idx >= int(f.Smeta.TotalGroups) {
		return nil
	}
	return f.blockGroups[idx].inodeBitmap.GetData(-1, 0)
}

func (f *FileSystem) DrawBlockBm(lim int) {
	if lim > int(f.Smeta.TotalGroups) {
		lim = int(f.Smeta.TotalGroups)
	}
	for i := 0; i < lim; i++ {
		group := &f.blockGroups[i]
		g := MakeHeatMap(group.blockBitmap.GetData(-1, 0), 1, nil)
		g.Draw()
	}
}

func (fs *FileSystem) isValidInode(inodeptr uint32) bool {
	_, group, _ := EntAddr(inodeptr).GetAddr()
	if group == 0 || group > fs.Smeta.TotalGroups {
		return false
	}
	bg := &fs.blockGroups[group-1]
	return bg.inodeBitmap.CheckBit(inodeptr)
}

func (fs *FileSystem) freeInode(inodeptr uint32) error {
	idx, group, _ := EntAddr(inodeptr).GetAddr()
	if group == 0 || group > fs.Smeta.TotalGroups {
		return BAD_UID
	}
	bg := &fs.blockGroups[group-1]
	bg.inodeBitmap.ClearBits([]uint32{inodeptr})
	data := bg.inodeBitmap.GetData(int(idx/8), 1)
	_, err := fs.device.volumes[group-1].file.WriteAt(data, int64(idx/8)+InodeBitmapOffset)
	return err
}

func (fs *FileSystem) allocInode() (uint32, error) {
	cur := fs.curBlockGroups
	for i := 0; i < int(fs.Smeta.TotalGroups); i++ {
		if fs.blockGroups[cur].inodeBitmap.FreeBits() > 0 {
			lst, _ := fs.blockGroups[cur].inodeBitmap.AllocBits(1, 1, false)
			if len(lst) > 0 {
				idx, _, _ := EntAddr(lst[0]).GetAddr()
				data := fs.blockGroups[cur].inodeBitmap.GetData(int(idx/8), 1)
				fs.device.volumes[cur].file.WriteAt(data, int64(idx/8)+InodeBitmapOffset)
				return lst[0], nil
			}
		}
		cur = (cur + 1) % fs.Smeta.TotalGroups
	}
	return 0, fmt.Errorf("No free inodes")
}

func (fs *FileSystem) StatBlocks(idx int) (int64, int64) {
	var c int64 = 0
	if idx >= 0 && idx < int(fs.Smeta.TotalGroups) {
		return int64(fs.Smeta.BlocksInGroup), int64(fs.blockGroups[idx].blockBitmap.FreeBits())
	}
	for i := 0; i < int(fs.Smeta.TotalGroups); i++ {
		c += int64(fs.blockGroups[i].blockBitmap.FreeBits())
	}
	return fs.Smeta.TotalBlocks(), c //total,free
}

func (fs *FileSystem) StatInodes(idx int) (int64, int64) {
	var c int64 = 0
	if idx >= 0 && idx < int(fs.Smeta.TotalGroups) {
		return int64(fs.Smeta.BlocksInGroup / fs.Smeta.InodesRatio), int64(fs.blockGroups[idx].inodeBitmap.FreeBits())
	}
	for i := 0; i < int(fs.Smeta.TotalGroups); i++ {
		c += int64(fs.blockGroups[i].inodeBitmap.FreeBits())
	}
	return fs.Smeta.TotalInodes(), c //total,free
}

func (fs *FileSystem) haveFreeBlocks(numBlocks int) bool {
	idx := fs.curBlockGroups
	cnt := 0
	for {
		group := &fs.blockGroups[idx]
		numBlocks -= int(group.blockBitmap.FreeBits())
		if numBlocks <= 0 {
			return true
		}
		cnt++
		idx = (idx + 1) % fs.Smeta.TotalGroups
		if cnt >= int(fs.Smeta.TotalGroups) {
			break
		}
	}
	return false
}

func (fs *FileSystem) syncInode(p uint32, node *Inode) error {
	idx, group, _ := EntAddr(p).GetAddr()
	if err := fs.device.checkReady(group-1, &fs.blockGroups[group-1]); err != nil {
		return err
	}
	offset := InodeOffset + int64(idx*uint32(InodeSize))
	logrus.Debugf("sync inode [%d] to [%s:%d]", p, fs.device.volumes[group-1].Fn, offset)
	if _, err := fs.device.volumes[group-1].file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	if err := binary.Write(fs.device.volumes[group-1].file, binary.LittleEndian, node); err != nil {
		return err
	}
	return nil
}

func (fs *FileSystem) readInode(p uint32) (*Inode, error) {
	idx, group, _ := EntAddr(p).GetAddr()
	if group <= 0 || group > fs.Smeta.TotalGroups {
		return nil, errors.New("Bad group id")
	}

	if err := fs.device.checkReady(group-1, &fs.blockGroups[group-1]); err != nil {
		return nil, err
	}
	offset := InodeOffset + int64(idx*uint32(InodeSize))
	if _, err := fs.device.volumes[group-1].file.Seek(offset, io.SeekStart); err != nil {
		logrus.Errorf("read inode failed(bad offset): %s", err)
		return nil, err
	}
	inode := Inode{}
	if err := binary.Read(fs.device.volumes[group-1].file, binary.LittleEndian, &inode); err != nil {
		logrus.Errorf("read inode failed: %s", err)
		return nil, err
	}
	return &inode, nil
}

func (fs *FileSystem) syncBlockAlloc(idx uint32, blks []uint32) error {
	if err := fs.device.checkReady(idx, &fs.blockGroups[idx]); err != nil {
		return err
	}
	segs, _ := mergeSeg(blks)
	for _, s := range segs {
		//data := fs.blockGroups[idx].blockBitmap[s.offset : s.offset+s.length]
		data := fs.blockGroups[idx].blockBitmap.GetData(s.offset, s.length)
		fs.device.volumes[idx].file.WriteAt(data, int64(s.offset)+BlockBitmapOffset)
	}
	return nil
}

func (fs *FileSystem) allocOneBlock() (uint32, error) {
	blks, _, err := fs.allocBlocks(1, 1, false)
	if err != nil {
		return 0, err
	}
	if len(blks) == 0 {
		return 0, errors.New("alloc block failed")
	}
	return blks[0], nil
}

func (fs *FileSystem) allocBlocks(numBlocks int, hlimit int, bigAlloc bool) ([]uint32, int, error) {
	if !fs.Smeta.IsBigAllocEnabled() {
		bigAlloc = false
	}
	//logrus.Debugf("alloc block: %d\n", numBlocks)
	if !fs.haveFreeBlocks(numBlocks) {
		return nil, 0, errors.New("Not enough free blocks")
	}
	allocatedBlocks := []uint32{}
	need := numBlocks

	idx := fs.curBlockGroups
	cnt := 0
	for {
		group := &fs.blockGroups[idx]
		limit := hlimit - len(allocatedBlocks)
		if limit == 0 {
			break
		}
		if group.blockBitmap.FreeBits() > 0 {
			blks, cnt := group.blockBitmap.AllocBits(numBlocks, limit, bigAlloc)
			numBlocks -= cnt
			allocatedBlocks = append(allocatedBlocks, blks...)
			if err := fs.syncBlockAlloc(idx, blks); err != nil {
				return allocatedBlocks, need - numBlocks, err
			}
			if numBlocks <= 0 || len(allocatedBlocks) == hlimit {
				break
			}
		}
		cnt++
		idx = (idx + 1) % fs.Smeta.TotalGroups
		fs.curBlockGroups = idx
		if cnt >= int(fs.Smeta.TotalGroups) {
			break
		}
	}
	if numBlocks > 0 && len(allocatedBlocks) < hlimit {
		return allocatedBlocks, need - numBlocks, errors.New("Not enough free blocks")
	}
	return allocatedBlocks, need - numBlocks, nil
}

func (fs *FileSystem) readBlock(blkptr uint32, offset int, data []byte) (int, int, error) {
	idx, group, isBig := EntAddr(blkptr).GetAddr()
	blksize := int(fs.Smeta.BlockSize)
	if isBig > 0 {
		blksize = 64 * int(fs.Smeta.BlockSize)
	}
	size := blksize - offset
	if size > len(data) {
		size = len(data)
	}
	left := blksize - size - offset
	if err := fs.device.checkReady(group-1, &fs.blockGroups[group-1]); err != nil {
		return 0, left, err
	}
	pos := BlockOffset + int64(idx)*int64(fs.Smeta.BlockSize) + int64(offset)
	if _, err := fs.device.volumes[group-1].file.Seek(pos, io.SeekStart); err != nil {
		logrus.Errorf("read block failed(bad offset): %s", err)
		return 0, left, err
	}
	rdn, err := fs.device.volumes[group-1].file.ReadAt(data[:size], pos)

	if err != nil {
		if err != io.EOF {
			logrus.Errorf("read block failed. [offset:%d,len:%d,err:%s]", offset, rdn, err)
		}
		return rdn, left, err
	}
	return rdn, left, nil
}

func (fs *FileSystem) writePointer(block uint32, blockptrs []uint32, offset int) error {
	data := make([]byte, 4*len(blockptrs))
	for i, ptr := range blockptrs {
		binary.LittleEndian.PutUint32(data[i*4:], uint32(ptr))
	}
	_, _, err := fs.writeBlock(block, data, offset*4) //need to merge first and cache
	return err
}

func (fs *FileSystem) writePointerWithCache(block uint32, blockptrs []uint32, offset int, lv int) error {
	if err := fs.writePointer(block, blockptrs, offset); err != nil {
		return err
	}
	if data, ok := fs.ibCache.Get(lv, block); ok {
		if offset+len(blockptrs) <= len(data.([]uint32)) {
			copy(data.([]uint32)[offset:], blockptrs)
		}
	} else if len(blockptrs) == BlockPointers {
		fs.ibCache.Put(lv, block, blockptrs)
	}
	return nil
}

func (fs *FileSystem) readPointer(blkptr uint32, blockptrs []uint32, offset int) error {
	data := make([]byte, 4*len(blockptrs))
	rdn, _, err := fs.readBlock(blkptr, 4*offset, data)
	if err != nil {
		return err
	}
	if rdn != len(data) {
		return errors.New("bad blockptrs length")
	}
	for i := 0; i < len(blockptrs); i++ {
		blockptrs[i] = binary.LittleEndian.Uint32(data[i*4:])
	}
	return nil
}

func (fs *FileSystem) readPointerWithCache(blkptr uint32, blockptrs []uint32, offset int, lv int) error {
	if data, ok := fs.ibCache.Get(lv, blkptr); ok {
		if offset+len(blockptrs) <= len(data.([]uint32)) {
			copy(blockptrs, data.([]uint32)[offset:])
			return nil
		} else {
			logrus.Warnf("read pointer from cache failed, bad offset [block:%d, lv:%d,offset:%d,len:%d]",
				blkptr, lv, offset, len(data.([]uint32)))
			return errors.New("Bad pointer offset")
		}
	}
	if err := fs.readPointer(blkptr, blockptrs, offset); err != nil {
		return err
	}
	if offset == 0 && len(blockptrs) == int(BlockPointers) {
		fs.ibCache.Put(lv, blkptr, blockptrs)
	}
	return nil
}

func (fs *FileSystem) writeBlock(blkptr uint32, data []byte, offset int) (int, int, error) {
	idx, group, isBig := EntAddr(blkptr).GetAddr()
	size := int(fs.Smeta.BlockSize)
	if isBig > 0 {
		size = 64 * int(fs.Smeta.BlockSize)
	}
	broff := 0
	if offset >= size {
		return 0, 0, errors.New("bad offset")
	}
	if len(data) >= size-offset {
		size -= offset
	} else {
		size = len(data)
		broff = offset + size
	}
	pos := BlockOffset + int64(offset) + int64(idx)*int64(fs.Smeta.BlockSize)
	if _, err := fs.device.volumes[group-1].file.Seek(pos, io.SeekStart); err != nil {
		logrus.Errorf("read block failed(bad offset): %s", err)
		return 0, 0, err
	}
	wtn, err := fs.device.volumes[group-1].file.WriteAt(data[:size], pos)
	if err != nil {
		return 0, 0, err
	}
	return wtn, broff, nil
}

func (fs *FileSystem) inode2snap(ptr uint32) (FileSnap, error) {
	node, err := fs.readInode(ptr)
	if err != nil {
		logrus.Errorf("Read inode error:%s", err)
		return FileSnap{}, err
	}
	snap := FileSnap{
		Key:   fs.inode2Uid(ptr, node),
		Inode: ptr,
		Name:  "",
		Meta:  nil,
		Size:  int64(node.FileSize),
		CTime: node.CTime,
		MTime: node.MTime,
	}
	meta, err := fs.loadMeta(node)
	if err != nil {
		return snap, err
	}
	snap.Name = meta.Name
	snap.Meta = meta.ExtMetas
	return snap, nil
}

func (fs *FileSystem) calcOffset(dataSize uint64, inodeptr uint32) int {
	_, _, isBig := EntAddr(inodeptr).GetAddr()
	if isBig > 0 {
		return int(dataSize%uint64(fs.Smeta.BlockSize)) + 63*int(fs.Smeta.BlockSize)
	} else {
		return int(dataSize % uint64(fs.Smeta.BlockSize))
	}
}

func (fs *FileSystem) GetFileList() ([]FileSnap, error) {
	var list []FileSnap
	for g := 0; g < int(fs.Smeta.TotalGroups); g++ {
		if fs.device.volumes[g].Status > 0 {
			gp := &fs.blockGroups[g]
			bm := gp.inodeBitmap.GetData(-1, 0)
			for i := 0; i < len(bm); i++ {
				if bm[i] == 0 {
					continue
				}
				for bitIndex := 0; bitIndex < 8; bitIndex++ {
					if (bm[i] & (1 << bitIndex)) > 0 {
						ptr := MakeEntAddr(uint32(i*8+bitIndex), uint32(g)+1, false)
						snap, err := fs.inode2snap(ptr)
						if err != nil {
							return list, err
						}
						list = append(list, snap)
					}
				}
			}
		}
	}
	return list, nil
}

func (fs *FileSystem) listInodes() { //for debug
	fmt.Printf("%-20s  %-10s  %-25s  %-20s\n", "inode(group:idx)", "filesize", "date", "fileid")
	for g := 0; g < int(fs.Smeta.TotalGroups); g++ {
		if fs.device.volumes[g].Status > 0 {
			gp := &fs.blockGroups[g]
			bm := gp.inodeBitmap.GetData(-1, 0)
			for i := 0; i < len(bm); i++ {
				if bm[i] == 0 {
					continue
				}
				for bitIndex := 0; bitIndex < 8; bitIndex++ {

					// Check if the block is free
					if (bm[i] & (1 << bitIndex)) > 0 {
						ptr := MakeEntAddr(uint32(i*8+bitIndex), uint32(g)+1, false)
						node, err := fs.readInode(ptr)
						if err != nil {
							fmt.Printf("Error:%s\n", err)
						} else {
							fmt.Printf("%-20s  %-10s  %-25s  %-20d\n",
								fmt.Sprintf("%d(%d:%d)", ptr, g+1, i),
								FormatBytes(int64(node.FileSize)),
								time.Unix(int64(node.CTime), 0).Local().Format("2006-01-02 15:04:05 MST"),
								node.Seq,
							)
						}
					}
				}
			}
		}
	}
}

func pow(base, exp int) int {
	var result int = 1
	for exp > 0 {
		result *= base
		exp--
	}
	return result
}

func (fs *FileSystem) releaseIndirectBlocks(blockptr uint32, depth int, blocks int) error {
	if blockptr == 0 {
		return nil
	}
	var pos = 0
	if depth == 1 {
		pos = blocks
	} else {
		pos = (blocks + pow(BlockPointers, depth-1) - 1) / (pow(BlockPointers, depth-1))
	}
	logrus.Debugf("release indirect blocks [%d,depth:%d,blocks:%d,pos:%d]", blockptr, depth, blocks, pos)
	if pos >= BlockPointers {
		pos = BlockPointers
	}
	blockptrs := make([]uint32, pos)
	err := fs.readPointer(blockptr, blockptrs, 0)
	if err != nil {
		return err
	}

	if depth == 1 {
		if err := fs.releaseDataBlock(blockptrs); err != nil {
			return err
		}
	} else {
		for _, ptr := range blockptrs {
			err := fs.releaseIndirectBlocks(ptr, depth-1, blocks)
			if err != nil {
				return err
			}
			blocks -= pow(BlockPointers, depth-1)
		}
	}
	return fs.releaseDataBlock([]uint32{blockptr})
}

func (fs *FileSystem) releaseDataBlock(blockptrs []uint32) error {
	sort.Slice(blockptrs, func(i, j int) bool {
		return (blockptrs[i] & 0x7fffffff) < (blockptrs[j] & 0x7fffffff)
	})
	groups := make(map[uint32][]uint32)
	for _, v := range blockptrs {
		_, group, _ := EntAddr(v).GetAddr()
		groups[group] = append(groups[group], v)
	}
	for g, v := range groups {
		if g < 1 || g > fs.Smeta.TotalGroups {
			return BAD_GID
		}
		if err := fs.device.checkReady(g-1, &fs.blockGroups[g-1]); err != nil {
			return err
		}
		fs.blockGroups[g-1].blockBitmap.ClearBits(v)
		segs, _ := mergeSeg(v)
		for _, s := range segs {
			//data := fs.blockGroups[g-1].blockBitmap[s.offset : s.offset+s.length]
			data := fs.blockGroups[g-1].blockBitmap.GetData(s.offset, s.length)
			fs.device.volumes[g-1].file.WriteAt(data, int64(s.offset)+BlockBitmapOffset)
		}
	}
	return nil
}

// DeleteFile removes a file from the file system using the specified unique ID (uid).
// It frees any allocated resources associated with the file and updates the file system
// structure accordingly.
//
// Parameters:
//   - uid (string): The unique identifier of the file to be deleted.
//
// Returns:
//   - error: An error if the file could not be deleted (e.g., if the file does not
//     exist or if there are permission issues). If successful, the file is removed
//     from the file system.
func (fs *FileSystem) DeleteFile(uid string) error {
	key := FileKey{}
	if err := key.ParseKey(uid); err != nil {
		return err
	}
	if !fs.isValidInode(key.Inodeptr) {
		return FNF
	}

	inode, err := fs.readInode(key.Inodeptr)
	if err != nil {
		return FNF
	}
	if fs.inode2Uid(key.Inodeptr, inode) != uid {
		return FNF
	}
	logrus.Debugf("delete file [uid:%s,inode:%d,size:%d,blocks:%d]", uid, key.Inodeptr, inode.FileSize, inode.Blocks)
	for i := 0; i < DirectBlocks && i < int(inode.Blocks); i++ {
		if inode.DirectPointers[i] != 0 {
			if err := fs.releaseDataBlock([]uint32{inode.DirectPointers[i]}); err != nil {
				return err
			}
			inode.DirectPointers[i] = 0
		}
	}
	blocks := int(inode.Blocks - DirectBlocks)
	if inode.SingleIndirect > 0 && blocks > 0 {
		n := pow(BlockPointers, SingleIndirectLv)
		batch := blocks
		if blocks > n {
			batch = n
		}
		if err := fs.releaseIndirectBlocks(inode.SingleIndirect, SingleIndirectLv, batch); err != nil {
			return err
		}
		blocks -= batch
		inode.SingleIndirect = 0
	}
	if inode.DoubleIndirect > 0 && blocks > 0 {
		n := pow(BlockPointers, DoubleIndirectLv)
		batch := blocks
		if blocks > n {
			batch = n
		}
		if err := fs.releaseIndirectBlocks(inode.DoubleIndirect, DoubleIndirectLv, batch); err != nil {
			return err
		}
		blocks -= batch
		inode.DoubleIndirect = 0
	}
	if inode.TripleIndirect > 0 && blocks > 0 {
		if err := fs.releaseIndirectBlocks(inode.TripleIndirect, 3, blocks); err != nil {
			return err
		}
	}
	return fs.freeInode(key.Inodeptr)
}

func (fs *FileSystem) inode2Uid(inodeptr uint32, inode *Inode) string {
	k := FileKey{
		Shard:    fs.Smeta.ShardId,
		Inodeptr: inodeptr,
		Seq:      inode.Seq,
		Stamp:    uint32(inode.CTime),
	}
	return k.ToString()
}

// CreateFile creates a new file in the file system with the specified name
// and metadata. It initializes a Vfile instance to represent the new file.
// The encoded length of both the name and the meta must be less than the
// size of a block in the depot file system.
// If successful, it returns a pointer to the created Vfile, its unique ID,
// and a nil error. If the file creation fails, an error will be returned.
//
// Parameters:
//   - name: The name of the file to be created. This should be a valid file name
//     that adheres to the file system's naming conventions.
//   - meta: A byte slice containing metadata associated with the file. This
//     could include information such as file type, permissions, or custom data.
//
// Returns:
//   - (*Vfile): A pointer to the newly created Vfile instance representing
//     the file in the file system.
//   - string: The unique ID assigned to the created file.
//   - error: Any error that occurred during the file creation process. If
//     successful, error will be nil.
func (fs *FileSystem) CreateFile(name string, meta []byte) (*Vfile, string, error) {
	vf := Vfile{
		fs:   fs,
		Meta: new(FileMeta),
	}
	if len(meta) > MaxFileMetaSize {
		return nil, "", errors.New("meta overlimit")
	}
	vf.Meta.ExtMetas = meta
	vf.Meta.Name = name
	mbuff, err := vf.Meta.ToBytes()
	if len(mbuff) >= int(fs.Smeta.BlockSize) {
		return nil, "", errors.New("File meta overlimit")
	}

	inodeptr, err := fs.allocInode()
	if err != nil {
		return nil, "", err
	}
	vf.Inodeptr = inodeptr
	oldnode, err := fs.readInode(inodeptr)
	if err != nil {
		return nil, "", err
	}
	inode := Inode{
		Seq:   oldnode.Seq + 1,
		CTime: uint64(time.Now().Unix()),
	}

	uid := fs.inode2Uid(inodeptr, &inode)

	inode.MetaSize = uint16(len(mbuff))
	inode.Blocks = 1
	n, err := fs.allocOneBlock()
	if err != nil {
		return nil, uid, err
	}
	if _, _, err := fs.writeBlock(n, mbuff, 0); err != nil {
		return nil, uid, err
	}
	inode.DirectPointers[0] = n
	vf.Inode = &inode
	vf.offset.blkRemOffset = len(mbuff)
	if err := vf.fs.syncInode(vf.Inodeptr, vf.Inode); err != nil {
		return nil, uid, err
	}
	return &vf, uid, nil
}

func (fs *FileSystem) loadMeta(node *Inode) (FileMeta, error) {
	meta := FileMeta{}
	data := make([]byte, node.MetaSize)
	if _, _, err := fs.readBlock(node.DirectPointers[0], 0, data); err != nil {
		return meta, err
	}
	err := meta.FromBytes(data)
	return meta, err
}

// OpenFile opens a file in the file system using the specified unique ID (uid).
// It retrieves the corresponding Vfile instance, allowing for file operations such
// as reading, writing, and seeking.
//
// Parameters:
//   - uid (string): The unique identifier of the file to be opened.
//
// Returns:
//   - *Vfile: A pointer to the opened Vfile instance, which provides access to
//     the file's content and operations.
//   - error: An error if the file could not be opened (e.g., if the file does
//     not exist or if there are permission issues).
func (fs *FileSystem) OpenFile(uid string) (*Vfile, error) {
	key := FileKey{}
	if err := key.ParseKey(uid); err != nil {
		return nil, err
	}
	vf := Vfile{
		fs:   fs,
		Meta: new(FileMeta),
	}

	inode, err := fs.readInode(key.Inodeptr)
	if err != nil {
		return nil, FNF
	}

	if fs.inode2Uid(key.Inodeptr, inode) != uid {
		return nil, FNF
	}

	vf.Inodeptr = key.Inodeptr
	vf.Inode = inode
	if inode.MetaSize > uint16(fs.Smeta.BlockSize) {
		return nil, errors.New("Bad meta size")
	}
	vf.offset.blkRemOffset = int(inode.MetaSize)

	meta, err := fs.loadMeta(inode)
	if err != nil {
		return nil, err
	}
	vf.Meta = &meta
	logrus.Debugf("Open file [inode:%d , size:%d,name:%s,block:%d,indirect<%d,%d,%d> blocks:%v]",
		key.Inodeptr, vf.Inode.FileSize, vf.Meta.Name, vf.Inode.Blocks,
		vf.Inode.SingleIndirect, vf.Inode.DoubleIndirect, vf.Inode.TripleIndirect, vf.Inode.DirectPointers)

	return &vf, nil
}

type VfileOffset struct {
	offset       int64
	blockIdx     uint32
	blkRemOffset int
}

type Vfile struct {
	FileId   uint64
	Meta     *FileMeta
	fs       *FileSystem
	Inodeptr uint32 //todo rename Inodeptr to InodeId
	Inode    *Inode
	offset   VfileOffset
}

func (vf *Vfile) readFromIndirect(blockptr uint32, blockIndex uint32, data []byte, depth int) (int, error) {
	blkIdx := blockIndex / uint32(pow(BlockPointers, depth-1))
	blockptrs := make([]uint32, BlockPointers)
	err := vf.fs.readPointerWithCache(blockptr, blockptrs, 0, depth)
	if err != nil {
		return 0, err
	}
	totalRdn := 0
	if depth == 1 {
		for i := blkIdx; i < uint32(BlockPointers); i++ {
			rdn, left, err := vf.fs.readBlock(blockptrs[i], vf.offset.blkRemOffset, data[totalRdn:])
			if err != nil {
				return rdn, err
			}
			if left == 0 {
				vf.offset.blockIdx++
				vf.offset.blkRemOffset = 0
			} else {
				vf.offset.blkRemOffset += rdn
			}
			totalRdn += rdn
			if totalRdn >= len(data) {
				break
			}
		}
		return totalRdn, nil
	}
	for i := blkIdx; i < uint32(BlockPointers); i++ {
		if blockptrs[i] == 0 {
			return 0, errors.New("read from unallocated block")
		}
		var offset uint32 = 0
		if i == blkIdx {
			offset = blockIndex % uint32(pow(BlockPointers, depth-1))
		}
		rdn, err := vf.readFromIndirect(blockptrs[i], offset, data[totalRdn:], depth-1)
		if err != nil {
			return totalRdn, err
		}
		totalRdn += rdn
		if totalRdn >= len(data) {
			break
		}
	}
	return totalRdn, nil
}

func (vf *Vfile) aliginBlock(size int) int {
	return (size + int(vf.fs.Smeta.BlockSize) - 1) / int(vf.fs.Smeta.BlockSize)
}

func (vf *Vfile) readIndirectBlocks(blockIndex uint32, data []byte) (int, error) {
	levels := []struct {
		blkptr    *uint32
		indirects int
	}{
		{&vf.Inode.SingleIndirect, SingleIndirectLv},
		{&vf.Inode.DoubleIndirect, DoubleIndirectLv},
		{&vf.Inode.TripleIndirect, TripleIndirectLv},
	}
	for _, level := range levels {
		if blockIndex < uint32(pow(BlockPointers, level.indirects)) {
			if *level.blkptr == 0 {
				return 0, errors.New("bad indirect block id")
			}
			return vf.readFromIndirect(*level.blkptr, blockIndex, data, level.indirects)
		}
		if level.indirects != TripleIndirectLv {
			blockIndex -= uint32(pow(BlockPointers, level.indirects))
		}
	}
	return 0, errors.New("system full")
}

func (vf *Vfile) escapeBlock(ptr uint32, depth int, pos int64) (bool, error) {
	blockptrs := make([]uint32, BlockPointers)
	err := vf.fs.readPointer(ptr, blockptrs, 0)
	if err != nil {
		return false, err
	}
	if depth == 1 {
		for _, v := range blockptrs {
			if v == 0 {
				return false, io.EOF
			}
			_, _, isBig := EntAddr(v).GetAddr()
			blksize := int64(vf.fs.Smeta.BlockSize)
			if isBig > 0 {
				blksize = 64 * int64(vf.fs.Smeta.BlockSize)
			}
			vf.offset.blockIdx++
			if vf.offset.offset+blksize > pos {
				vf.offset.blkRemOffset = int(pos - vf.offset.offset)
				vf.offset.offset += int64(vf.offset.blkRemOffset)
				return true, nil
			} else if vf.offset.offset+blksize == pos {
				vf.offset.blockIdx++ //move to next block boundary
				vf.offset.offset = pos
				return true, nil
			} else {
				vf.offset.offset += blksize
			}
		}

	} else {
		for _, v := range blockptrs {
			ok, err := vf.escapeBlock(v, depth-1, pos)
			if err != nil || ok {
				return ok, err
			}
		}
	}
	return false, nil
}

// SeekPos sets the current position of the Vfile to the specified offset.
// It returns the new file offset and any error encountered during the operation.
// This method allows random access to the file, enabling reading or writing
// from a specific position.
// For better performance when frequently seeking, it is recommended to use the GetOffset method to retrieve the actual address of the offset after seeking.
// Note that after calling GetOffset, you should use the Seek method to set the file pointer to the actual position.
//
// Parameters:
//   - pos: The new position (offset) in the file, in bytes. This can be
//     any valid offset within the file's size.
//
// Returns:
//   - VfileOffset: The new position of the file after seeking.
//   - error: Any error that occurred during the seek operation. If successful,
//     error will be nil.
func (vf *Vfile) SeekPos(pos int64) (VfileOffset, error) {
	if pos >= int64(vf.Inode.FileSize) {
		vf.offset.offset = int64(vf.Inode.FileSize)
		vf.offset.blockIdx = vf.Inode.Blocks - 1
		vf.offset.blkRemOffset = int(vf.Inode.DataSize() % uint64(vf.fs.Smeta.BlockSize))
	}
	vf.offset.blkRemOffset = int(vf.Inode.MetaSize)
	vf.offset.offset = 0
	vf.offset.blockIdx = 0
	for i := vf.offset.blockIdx; i < DirectBlocks; i++ {
		if vf.Inode.DirectPointers[i] == 0 {
			return vf.offset, nil
		}
		vf.offset.blockIdx = i
		_, _, isBig := EntAddr(vf.Inode.DirectPointers[i]).GetAddr()
		blksize := int64(vf.fs.Smeta.BlockSize)
		if isBig > 0 {
			blksize = 64 * int64(vf.fs.Smeta.BlockSize)
		}
		if vf.offset.blkRemOffset != 0 { //first block
			blksize -= int64(vf.offset.blkRemOffset)
		}
		if vf.offset.offset+blksize > pos {
			vf.offset.blkRemOffset += int(pos - vf.offset.offset)
			vf.offset.offset = pos
			return vf.offset, nil
		} else if vf.offset.offset+blksize == pos {
			vf.offset.blkRemOffset = 0
			vf.offset.blockIdx += 1 //base 0
			vf.offset.offset = pos
			return vf.offset, nil
		} else {
			vf.offset.offset += blksize
			vf.offset.blkRemOffset = 0
		}
	}
	//seek indirect
	levels := []struct {
		blkptr    uint32
		indirects int
	}{
		{vf.Inode.SingleIndirect, SingleIndirectLv},
		{vf.Inode.DoubleIndirect, DoubleIndirectLv},
		{vf.Inode.TripleIndirect, TripleIndirectLv},
	}
	for _, level := range levels {
		ok, err := vf.escapeBlock(level.blkptr, level.indirects, pos)
		if err != nil || ok {
			return vf.offset, err
		}
	}
	return vf.offset, errors.New("system error")
}

// GetOffset retrieves the current offset of the Vfile.
// It returns the current position (offset) in the file.
// This method is useful for tracking the current read/write position
// within the file without modifying it.
func (vf *Vfile) GetOffset() VfileOffset {
	return vf.offset
}

// Seek sets the current offset of the Vfile to the specified value.
// It updates the file's position for subsequent read or write operations.
// This method allows you to move the file pointer to any valid position
// within the file, facilitating random access.
//
// Parameters:
//   - off: The new offset to set, represented as a VfileOffset value.
//     This value is typically obtained by calling the GetOffset method.
func (vf *Vfile) Seek(off VfileOffset) {
	vf.offset = off
}

// Read reads data from the Vfile into the provided byte slice.
// It returns the number of bytes read and any error encountered.
// The method reads up to len(data) bytes, or fewer if the end of the file is reached.
//
// Parameters:
// - data: A byte slice into which the file's data will be read.
//
// Returns:
// - int: The number of bytes actually read.
// - error: Any error that occurred during the read operation. If successful, error will be nil.
func (vf *Vfile) Read(data []byte) (int, error) {
	if uint64(vf.offset.offset) >= vf.Inode.FileSize {
		return 0, io.EOF
	}
	if uint64(vf.offset.offset+int64(len(data))) > vf.Inode.FileSize {
		data = data[:vf.Inode.FileSize-uint64(vf.offset.offset)]
	}

	rdn := 0
	for i := vf.offset.blockIdx; i < DirectBlocks; i++ {
		if vf.Inode.DirectPointers[i] == 0 {
			break
		}
		rd, left, err := vf.fs.readBlock(vf.Inode.DirectPointers[i], vf.offset.blkRemOffset, data[rdn:])
		vf.offset.blockIdx = i
		if left == 0 {
			vf.offset.blockIdx++
			vf.offset.blkRemOffset = 0
		} else {
			vf.offset.blkRemOffset += rd
		}
		if err != nil {
			return rd, err
		}
		rdn += rd
		if rdn == len(data) {
			break
		}
	}
	for rdn < len(data) {
		blockIdx := vf.offset.blockIdx - DirectBlocks
		rd, err := vf.readIndirectBlocks(blockIdx, data[rdn:])
		if err != nil {
			return rdn, err
		}
		if rd == 0 {
			logrus.Errorf("read indirect blocks return zero length")
			break
		}
		rdn += rd
		logrus.Debugf("read indirect blocks, len:%d,total:%d, err:%v\n", rd, rdn, err)
	}
	vf.offset.offset += int64(rdn)
	return rdn, nil
}

// Write writes the provided byte slice to the Vfile.
// It returns the number of bytes written and any error encountered.
// The method writes up to len(data) bytes, potentially overwriting existing content in the file.
//
// Parameters:
// - data: A byte slice containing the data to be written to the file.
//
// Returns:
// - int: The number of bytes successfully written to the file.
// - error: Any error that occurred during the write operation. If successful, error will be nil.
func (vf *Vfile) Write(data []byte) (int, error) {
	if vf.Inode == nil {
		return 0, errors.New("Invalid inode")
	}
	totalWtn := 0
	for vf.offset.blockIdx < DirectBlocks { //overwrite
		if vf.Inode.DirectPointers[vf.offset.blockIdx] != 0 {
			wtn, broff, err := vf.fs.writeBlock(vf.Inode.DirectPointers[vf.offset.blockIdx], data, vf.offset.blkRemOffset)
			if err != nil {
				return totalWtn, err
			}
			totalWtn += wtn
			data = data[wtn:]
			vf.Inode.FileSize += uint64(wtn)
			vf.offset.offset += int64(wtn)
			vf.offset.blkRemOffset = broff
			if broff == 0 {
				vf.offset.blockIdx++
			}
			if err := vf.fs.syncInode(vf.Inodeptr, vf.Inode); err != nil {
				return 0, err
			}
		} else {
			allocNum := vf.aliginBlock(len(data))
			nb, batch, err := vf.fs.allocBlocks(allocNum, int(DirectBlocks-vf.offset.blockIdx), true)
			if err != nil {
				return totalWtn, err
			}
			allocNum -= batch
			for i := 0; i < len(nb); i++ {
				vf.Inode.DirectPointers[vf.offset.blockIdx] = nb[i]
				if wtn, broff, err := vf.fs.writeBlock(nb[i], data, 0); err != nil {
					return 0, err
				} else {
					data = data[wtn:]
					vf.Inode.FileSize += uint64(wtn)
					vf.Inode.Blocks++
					totalWtn += wtn
					//update offset
					vf.offset.offset += int64(wtn)
					vf.offset.blkRemOffset = broff
					if broff == 0 {
						vf.offset.blockIdx++
					}
				}
			}
			if err := vf.fs.syncInode(vf.Inodeptr, vf.Inode); err != nil {
				return 0, err
			}
		}
		if len(data) == 0 {
			return totalWtn, nil
		}
	}
	for len(data) > 0 {
		if vf.offset.blockIdx < DirectBlocks {
			return totalWtn, errors.New("Inner err,Wrong inode.Blocks ")
		}
		wtn, err := vf.writeIndirectBlocks(vf.offset.blockIdx-DirectBlocks, data)
		if err != nil {
			return totalWtn, err
		}
		if wtn == 0 {
			logrus.Errorf("write indirect blocks return zero length")
			break
		}
		totalWtn += wtn
		data = data[wtn:]
		logrus.Debugf("write indirect blocks, len:%d,left:%d,total:%d, err:%v\n", wtn, len(data), totalWtn, err)
	}

	if len(data) > 0 {
		return totalWtn, errors.New("Fill system full")
	} else {
		return totalWtn, nil
	}
}

func (vf *Vfile) writeIndirectBlocks(blockIndex uint32, data []byte) (int, error) {
	levels := []struct {
		blkptr    *uint32
		indirects int
	}{
		{&vf.Inode.SingleIndirect, SingleIndirectLv},
		{&vf.Inode.DoubleIndirect, DoubleIndirectLv},
		{&vf.Inode.TripleIndirect, TripleIndirectLv},
	}
	for _, level := range levels {
		if blockIndex < uint32(pow(BlockPointers, level.indirects)) {
			if *level.blkptr == 0 {
				nb, err := vf.fs.allocOneBlock()
				if err != nil {
					return 0, err
				}
				err = vf.fs.writePointerWithCache(nb, make([]uint32, BlockPointers), 0, level.indirects)
				if err != nil {
					return 0, err
				}
				*(level.blkptr) = nb
				if err := vf.fs.syncInode(vf.Inodeptr, vf.Inode); err != nil {
					return 0, err
				}
			}
			return vf.writeToIndirect(*level.blkptr, blockIndex, data, level.indirects)
		}
		if level.indirects != TripleIndirectLv {
			blockIndex -= uint32(pow(BlockPointers, level.indirects))
		}
	}
	return 0, errors.New("system full")
}

func (vf *Vfile) batchWriteNewBlk(blockptr uint32, blockIndex uint32, data []byte) (int, error) {
	totalWtn := 0
	batchLimit := BlockPointers - int(blockIndex)
	allocNum := vf.aliginBlock(len(data))
	blks, _, err := vf.fs.allocBlocks(allocNum, batchLimit, true)
	if err != nil {
		return totalWtn, err
	}
	logrus.Debugf("batch_fill [%d-%d @%d] batchLimit:%d alloc:%d", blockIndex, BlockPointers, blockptr, batchLimit, len(blks))
	err = vf.fs.writePointerWithCache(blockptr, blks, int(blockIndex), 1)
	if err != nil {
		return totalWtn, err
	}
	for i := 0; i < len(blks); i++ {
		wtn, broff, err := vf.fs.writeBlock(blks[i], data[totalWtn:], 0)
		if err != nil {
			return wtn, err
		}
		vf.Inode.FileSize += uint64(wtn)
		vf.Inode.Blocks++
		totalWtn += wtn
		vf.offset.offset += int64(wtn)
		vf.offset.blkRemOffset = broff
		if broff == 0 {
			vf.offset.blockIdx++
		}
	}
	if err := vf.fs.syncInode(vf.Inodeptr, vf.Inode); err != nil {
		return 0, err
	}

	return totalWtn, nil
}

func (vf *Vfile) writeToIndirect(blockptr uint32, blockIndex uint32, data []byte, depth int) (int, error) {
	if depth == 1 && vf.offset.blockIdx >= vf.Inode.Blocks {
		return vf.batchWriteNewBlk(blockptr, blockIndex, data)
	}
	if depth == 0 {
		wtn, bloff, err := vf.fs.writeBlock(blockptr, data, vf.offset.blkRemOffset)
		if err != nil {
			return wtn, err
		}
		if vf.offset.blkRemOffset == 0 {
			vf.Inode.Blocks++
		}
		vf.offset.offset += int64(wtn)
		if vf.offset.offset > int64(vf.Inode.FileSize) {
			vf.Inode.FileSize = uint64(vf.offset.offset)
		}
		if bloff == 0 {
			vf.offset.blockIdx++
		}
		vf.offset.blkRemOffset = bloff
		if err := vf.fs.syncInode(vf.Inodeptr, vf.Inode); err != nil {
			return 0, err
		}
		return wtn, nil
	}
	indirectIndex := blockIndex / uint32(pow(BlockPointers, depth-1))

	blockptrs := make([]uint32, 1)
	err := vf.fs.readPointerWithCache(blockptr, blockptrs, int(indirectIndex), depth)
	if err != nil && err != io.EOF {
		return 0, err
	}
	if blockptrs[0] == 0 {
		nb, err := vf.fs.allocOneBlock()
		if err != nil {
			return 0, err
		}

		err = vf.fs.writePointerWithCache(nb, make([]uint32, BlockPointers), 0, depth-1)
		if err != nil {
			return 0, err
		}

		blockptrs[0] = nb
		err = vf.fs.writePointerWithCache(blockptr, blockptrs, int(indirectIndex), depth)
		if err != nil {
			return 0, err
		}
	}
	return vf.writeToIndirect(blockptrs[0], blockIndex%uint32(pow(BlockPointers, depth-1)), data, depth-1)
}
