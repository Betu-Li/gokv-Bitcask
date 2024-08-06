package main

import (
	"errors"
	"fmt"
	"os"
)

var (
	readMissDataErr  = errors.New("miss data during read")
	writeMissDataErr = errors.New("miss data during write")
	crcErr           = errors.New("crc error")
	deleteEntryErr   = errors.New("read an entry which had deleted")
)

const (
	fileSuffix = ".dat"
	B          = 1
	KB         = 1024 * B
	MB         = 1024 * KB
	GB         = 1024 * MB
)

type Storage struct {
	dir        string // db directory
	fileSize   int64  // 可写入文件的阈值
	ActiveFile *ActiveFile
	fds        map[int]*os.File // db中所有数据文件的文件描述符（fd）缓存，免得重复打开文件描述符导致性能消耗。
}

// 写入数据
func (s *Storage) writeAt(bytes []byte) (i *Index, err error) {
	err = s.ActiveFile.writeAt(bytes)
	if err != nil {
		return nil, err
	}
	i = &Index{
		fid: s.ActiveFile.fid,
		off: s.ActiveFile.off,
	}
	s.ActiveFile.off += int64(len(bytes))
	// 如果当前的off大于设置的阈值，进行active file的切换
	// 具体操作是新建一个名为fid + 1 的文件，然后将af切换成代表最新可写入文件的对象。
	if s.ActiveFile.off >= s.fileSize {
		err := s.rotate()
		if err != nil {
			return nil, err
		}
	}
	return i, nil
}

// 读取Entry
func (s *Storage) readEntry(fid int, off int64) (e *Entry, err error) {
	buf := make([]byte, MetaSize) // 创建一个大小为MetaSize的字节数组
	err = s.readAt(fid, off, buf) // 从文件中读取
	if err != nil {
		return nil, err
	}
	e = NewEntry()    // 创建一个新的Entry
	e.DecodeMeta(buf) // 解码Meta
	if e.meta.flag == DeleteFlag {
		return nil, deleteEntryErr
	}
	off += MetaSize
	payloadSize := int(e.meta.keySize + e.meta.valueSize)
	payload := make([]byte, payloadSize)
	err = s.readAt(fid, off, payload)
	if err != nil {
		return nil, err
	}
	err = e.DecodePayload(payload) // 解码key和value
	if err != nil {
		return nil, err
	}
	crc := e.getCRC(buf) //
	if e.meta.crc != crc {
		return nil, crcErr
	}
	return e, nil
}

func (s *Storage) readFullEntry(fid int, off int64, buf []byte) (e *Entry, err error) {
	err = s.readAt(fid, off, buf)
	if err != nil {
		return nil, err
	}
	e = NewEntry()
	e.DecodeMeta(buf[0:MetaSize])
	payloadSize := e.meta.keySize + e.meta.keySize
	err = e.DecodePayload(buf[MetaSize : MetaSize+payloadSize])
	if err != nil {
		return nil, err
	}
	crc := e.getCRC(buf)
	if e.meta.crc != crc {
		return nil, crcErr
	}
	return e, nil
}

func (s *Storage) readAt(fid int, off int64, bytes []byte) (err error) {
	if fd := s.fds[fid]; fd != nil { // 如果文件描述符已经缓存
		// 从文件描述符中读取数据字节数，如果读取的字节数小于要读取的字节数，返回readMissDataErr
		n, err := fd.ReadAt(bytes, off)
		if err != nil {
			return err
		}
		if n < len(bytes) {
			return readMissDataErr
		}
		return nil
	}
	// 如果文件描述符没有缓存，打开文件获取文件描述符
	path := fmt.Sprintf("%s/%d.dat", s.dir, fid)         // 获取文件路径
	fd, err := os.OpenFile(path, os.O_RDWR, os.ModePerm) // 打开文件，获取文件描述符
	s.fds[fid] = fd                                      // 缓存文件描述符
	n, err := fd.ReadAt(bytes, off)
	if err != nil {
		return err
	}
	if n < len(bytes) {
		return readMissDataErr
	}
	return nil
}

// 切换活动文件
func (s *Storage) rotate() error {
	af := &ActiveFile{
		fid: s.ActiveFile.fid + 1, // 新建文件的fid是当前文件的fid + 1
		off: 0,                    // af的写入位置从0开始
	}
	fd, err := os.OpenFile(s.getPath(), os.O_CREATE|os.O_RDWR, os.ModePerm) // 创建一个新的文件
	if err != nil {
		return err
	}
	af.f = fd          // 将新建的文件描述符赋值给af
	s.fds[af.fid] = fd // 将新建的文件描述符缓存到fds中
	s.ActiveFile = af  // 设置活动文件
	return nil
}

// 获取当前活动文件的路径
func (s *Storage) getPath() string {
	path := fmt.Sprintf("%s/%d%s", s.dir, s.ActiveFile.fid, fileSuffix)
	return path
}

type ActiveFile struct {
	fid int      // file id
	off int64    // 当前写入数据的最新位置
	f   *os.File // 文件描述符
}

// 写入数据到活动文件中
func (af ActiveFile) writeAt(bytes []byte) error {
	n, err := af.f.WriteAt(bytes, af.off)
	if n < len(bytes) {
		return writeMissDataErr
	}
	return err
}
