package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/jaco00/depot-fs/dpfs"

	"github.com/sirupsen/logrus"
)

var (
	fromDir       = flag.String("i", "", "Source directory to copy all files into the current Depot FS")
	toDir         = flag.String("o", "", "Destination directory to copy files to")
	dataDir       = flag.String("d", "./data", "Data file dir")
	fillLargeFile = flag.Int("f", 0, "Generate a large file (size in MB) with random data for performance testing")
	eraseAll      = flag.Bool("X", false, "Delete all data")
	delFile       = flag.String("x", "", "Delete file by uid")
	readFile      = flag.String("r", "", "Read file for performance testing")
	showInfo      = flag.Bool("I", false, "Show all info")
	batchAddFile  = flag.Int("b", 0, "Batch add a specified number of small files for testing")
	listFile      = flag.Bool("l", false, "Show all files")
	showGraph     = flag.Bool("g", false, "Show block bitmap graph")
	verboseLog    = flag.Bool("v", false, "Use verbose logging for developer")
	help          = flag.Bool("h", false, "Display this help message")
	fs            *dpfs.FileSystem
)

func main() {
	flag.Parse()
	if *help {
		printHelpInfo()
		return
	}
	logrus.SetFormatter(&logrus.TextFormatter{
		DisableColors:   false,
		FullTimestamp:   true,
		TimestampFormat: "15:04:05",
	})
	if *verboseLog {
		logrus.SetLevel(logrus.DebugLevel)
	} else {
		logrus.SetLevel(logrus.InfoLevel)
	}
	var group uint32 = 32
	var err error
	fs, err = dpfs.MakeFileSystem(group, 0, *dataDir, "", "", 1, true)
	if err != nil {
		logrus.Errorf("Init file system failed:%s", err)
		return
	}
	start := time.Now()
	if *eraseAll {
		snap, err := fs.GetFileList()
		if err != nil {
			logrus.Errorf("Load file list failed:%s", err)
			return
		}
		for _, f := range snap {
			if err := fs.DeleteFile(f.Key); err != nil {
				logrus.Errorf("Delete file:%s[%s], failed:%s", f.Name, f.Key, err)
				return
			}
		}
	} else if *delFile != "" {
		err := fs.DeleteFile(*delFile)
		fmt.Printf("Delete file: %s [%v]\n", *delFile, err)
	} else if *readFile != "" {
		var batchLimit int64 = 10 * 1024 * 1024
		dc, err := dpfs.NewNullDataConsumer(false)
		if err != nil {
			return
		}
		rdn, _, _, err := dpfs.ReadFile(fs, *readFile, dc, int64(batchLimit), true)
		if err != nil {
			fmt.Printf("Read file failed :%s\n", err)
			return
		}
		fmt.Printf("Read %s bytes\n", dpfs.FormatBytes(rdn))
	} else if *fromDir != "" {
		list, err := scanDir(*fromDir)
		if err != nil {
			logrus.Errorf("scan dir failed :%s", err)
			return
		}
		if *toDir != "" {
			err = saveFiles(list, *toDir, true)
			if err != nil {
				logrus.Errorf("save files failed :%s", err)
				return
			}
		}
	} else if *toDir != "" {
		fmt.Printf("############################save data###############################\n")
		snap, err := fs.GetFileList()
		if err != nil {
			logrus.Errorf("Load file list failed:%s", err)
			return
		}
		crcsnap := make([]FileCrc, len(snap))
		for i, a := range snap {
			crcsnap[i].snap = a
		}
		err = saveFiles(crcsnap, *toDir, false)
		if err != nil {
			logrus.Errorf("save files failed :%s", err)
			return
		}
	} else if *showInfo {
		printInfo()
	} else if *showGraph {
		fs.DrawBlockBm(int(group))
	} else if *batchAddFile > 0 {
		batchAddFiles(fs, *batchAddFile)
	} else if *listFile {
		fmt.Printf("== FILE LIST ==\n")
		snap, err := fs.GetFileList()
		if err != nil {
			logrus.Errorf("Load file list failed:%s", err)
			return
		}
		printFileList(snap)
	} else if *fillLargeFile > 0 {
		if err := testingLargeFile(fs, int64(*fillLargeFile)); err != nil {
			logrus.Errorf("Testing large file failed: %s", err)
			return
		}
	} else {
		printHelpInfo()
	}
	elapsed := time.Since(start)
	fmt.Printf("Cmd cost: %.3fs\n", elapsed.Seconds())
}

func printHelpInfo() {
	fmt.Printf("This is a demo for the Depot File System.\n")
	flag.PrintDefaults()
}

type FileCrc struct {
	snap dpfs.FileSnap
	crc  uint32
}

func printFileList(list []dpfs.FileSnap) {
	for _, v := range list {
		fmt.Printf("%-8x %-30s %-10s %-25s %s\n",
			v.Inode,
			v.Key,
			dpfs.FormatBytes(v.Size),
			time.Unix(int64(v.CTime), 0).Local().Format("2006-01-02 15:04:05 MST"),
			v.Name,
		)
	}
}

func printInfo() {
	fmt.Printf("== FS INFO ==\n")
	tb, fb := fs.StatBlocks(-1)
	ti, fi := fs.StatInodes(-1)
	fmt.Printf("Total group:%d\n", fs.Smeta.TotalGroups)
	fmt.Printf("Total space:%d GB\n", fs.Smeta.TotalSpace()/(1024*1024*1024))
	fmt.Printf("Block size:%d\n", fs.Smeta.BlockSize)
	fmt.Printf("Inode size:%d\n", binary.Size(dpfs.Inode{}))
	fmt.Printf("Blocks [%9d/%-9d]\n", tb-fb, tb)
	fmt.Printf("Inodes [%9d/%-9d]\n", ti-fi, ti)
	fmt.Printf("\n== GROUP INFO ==\n")
	fmt.Printf("%-3s %-15s %-18s %-18s %s\n", "ID", "FNAME", "INODES", "BLOCKS", "SIZE")
	for i := 0; i < int(fs.Smeta.TotalGroups); i++ {
		v := fs.GetVolumeInfo(i)
		tb, fb := fs.StatBlocks(i)
		ti, fi := fs.StatInodes(i)
		if v.Status == 0 {
			continue
		}
		fmt.Printf("%03d %-15s %-18s %-18s %s\n",
			v.Id,
			v.Fn,
			fmt.Sprintf("%d/%d", ti-fi, ti),
			fmt.Sprintf("%d/%d", tb-fb, tb),
			dpfs.FormatBytes(v.GetSize()))
	}
}

func saveFile(path, name string) (FileCrc, error) {
	src := filepath.Join(path, name)
	info := FileCrc{}
	info.snap.Name = name

	fdp, err := dpfs.NewFileDataProvider(src, 1024*1024, true)
	if err != nil {
		return info, err
	}
	key, _, crc1, _, err := dpfs.WriteFile(fs, fdp, name, nil, true)
	info.crc = crc1
	info.snap.Key = key
	return info, nil
}

func saveFiles(list []FileCrc, dst string, crcCheck bool) error {
	for _, e := range list {
		fdc, err := dpfs.NewFileDataConsumer(dst, e.snap.Name, crcCheck)
		if err != nil {
			fmt.Printf("New file data consumer failed:%s\n", err)
			return err
		}
		_, crc, _, err := dpfs.ReadFile(fs, e.snap.Key, fdc, 1024*1024, true)
		if err != nil {
			fmt.Printf("Load file data failed:%s\n", err)
			return err
		}
		if crcCheck && crc != e.crc {
			logrus.Errorf("Bad crc, file:%s", e.snap.Name)
		}
	}
	return nil
}

func scanDir(src string) ([]FileCrc, error) {
	infos := []FileCrc{}
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relativePath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			info, err := saveFile(src, relativePath)
			if err != nil {
				logrus.Errorf("save file failed:%s", err)
			} else {
				infos = append(infos, info)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return infos, nil
}

func batchAddFiles(fs *dpfs.FileSystem, n int) error {
	sizeLimit := 50 * 1024
	data := make([]byte, sizeLimit)
	for i := 0; i < n; i++ {
		totalSize := 1 + rand.Intn(sizeLimit)
		f, _, err := fs.CreateFile(fmt.Sprintf("testfile.%d", time.Now().UnixNano()), nil)
		if err != nil {
			fmt.Printf("Open file err:%s\n", err)
			return err
		}
		_, err = f.Write(data[0:totalSize])
		if err != nil {
			fmt.Printf("Write file err:%s\n", err)
			return err
		}
	}
	return nil
}

func testingLargeFile(fs *dpfs.FileSystem, size int64) error {
	var batchLimit int64 = 50 * 1024 * 1024
	var totalSize int64 = size * 1024 * 1024
	rdp, err := dpfs.NewRandomDataProvider(int64(batchLimit), int64(totalSize), false, true)
	if err != nil {
		return err
	}

	key, wtn, crc1, vf, err := dpfs.WriteFile(fs, rdp, "test.file", nil, true)
	if err != nil {
		fmt.Printf("test size:%d, write file failed :%s\n", totalSize, err)
		return err
	}
	fmt.Printf("total write: %d bytes\n", wtn)
	n := time.Now()
	if err := vf.Sync(); err != nil {
		return err
	}
	fmt.Printf("sync cost %s\n", time.Since(n))

	dc, err := dpfs.NewNullDataConsumer(true)
	if err != nil {
		return err
	}
	rdn, crc2, _, err2 := dpfs.ReadFile(fs, key, dc, int64(batchLimit), true)
	if err2 != nil {
		fmt.Printf("test size:%d,read file failed :%s\n", totalSize, err2)
		return err
	}
	if totalSize != wtn || totalSize != rdn {
		fmt.Printf("Failed (wrong length) [size:%d,%d,%d]\n", totalSize, wtn, rdn)
		return err
	}
	if crc1 != crc2 {
		fmt.Printf("Failed (wrong crc) [size:%d,crc:%d!=%d]\n", totalSize, crc1, crc2)
		return err
	} else {
		fmt.Printf("Write file Ok, size:%d, key:%s \n", totalSize, key)
	}
	return nil
}
