package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

var (
	KeyNotFoundErr   = errors.New("key not found")
	NoNeedToMergeErr = errors.New("no need to merge")
)

type DB struct {
	rw sync.RWMutex // 读写锁
	kd *keyDir      // key目录
	s  *Storage
}

// NewDB 新建一个DB
func NewDB(opt *Options) (db *DB, err error) {
	// 初始化一个DB
	db = &DB{
		rw: sync.RWMutex{},
		kd: &keyDir{index: map[string]*Index{}},
	}
	// 判断目录是否存在
	if isExist, _ := isDirExist(opt.Dir); isExist {
		// 从磁盘文件中恢复数据
		if err := db.Recovery(opt); err != nil {
			return nil, err
		}
		return db, nil
	}
	// 创建一个新的Storage
	var fileSize = getSegmentSize(opt.SegmentSize)
	db.s, err = NewStorage(opt.Dir, fileSize)
	fmt.Println("db.s", db.s)
	if err != nil {
		return nil, err
	}
	return db, err
}

// Merge 清理掉无效的数据
func (db *DB) Merge() error {
	// 给整个db加入写锁防止在写入数据的时候有别的程序并发写入。当然这里实则锁住了整个db，效率很低。
	db.rw.Lock()
	defer db.rw.Unlock()
	// 获取fid缓存
	fids, err := getFids(db.s.dir)
	if err != nil {
		return err
	}
	if len(fids) < 2 {
		return NoNeedToMergeErr
	}

	// fid排序
	sort.Ints(fids)
	for _, fid := range fids[:len(fids)-1] { // 这里将active data file排除在外，因为最新的数据重新写入逻辑是写在active data file中的
		var off int64 = 0 // 设置写入位置
		for {             // 从头到尾遍历文件
			entry, err := db.s.readEntry(fid, off) // 读取entry
			if err == nil {
				// 更新off
				off += int64(entry.Size())
				// 从keyDir中找到key对应的index
				oldIndex := db.kd.find(string(entry.key))
				if oldIndex == nil {
					continue
				}
				// 如果oldIndex的fid和off和当前的fid和off相同，说明这个entry是最新的
				if oldIndex.fid == fid && oldIndex.off == off {
					// 重新写入该数据，拿到最新写入位置的索引，并更新。
					newIndex, err := db.s.writeAt(entry.Encode())
					if err != nil {
						return err
					}
					db.kd.update(string(entry.key), newIndex)
				}
			} else {
				if err == io.EOF {
					break
				}
				return err
			}
		}
		// 文件中的最新数据都重新写入完毕之后，就删除文件。
		err = os.Remove(fmt.Sprintf("%s/%d%s", db.s.dir, fid, fileSuffix))
		if err != nil {
			return err
		}
	}
	return nil
}

// Recovery 从磁盘中恢复数据（用于崩溃恢复）
func (db *DB) Recovery(opt *Options) error {
	var fileSize = getSegmentSize(opt.SegmentSize) // 获取段大小
	// 初始化Storage
	db.s = &Storage{
		dir:      opt.Dir,
		fileSize: fileSize,
		fds:      map[int]*os.File{},
	}
	fids, err := getFids(opt.Dir) // 获取目录里的fids
	if err != nil {
		return err
	}
	// fid排序
	sort.Ints(fids)
	// 遍历fids
	for _, fid := range fids {
		// 	初始化插入位置
		var off int64 = 0
		// 读取文件
		path := fmt.Sprintf("%s/%d%s", opt.Dir, fid, fileSuffix)
		fd, err := os.OpenFile(path, os.O_RDWR, os.ModePerm)
		if err != nil {
			return err
		}
		// 缓存文件描述符
		db.s.fds[fid] = fd
		for {
			// 读取entry
			entry, err := db.s.readEntry(fid, off)
			if err == nil {
				// 更新off
				off += int64(entry.Size())
				// 将数据的位置信息构建成索引插入到内存索引中
				// 不需要理会这个key在索引中是否存在，因为当前读到的数据，必定比索引中的存的索引所代表的数据要新
				// 因为当前读到的数据fid或off必定是大于索引中存储的index信息的。
				db.kd.update(string(entry.key), &Index{fid: fid, off: off, timeStamp: entry.meta.timeStamp})
			} else {
				if errors.Is(err, deleteEntryErr) {
					continue
				}
				if err == io.EOF {
					break
				}
				return err
			}
		}
		// 如果是最后一个文件，那么将这个文件设置为活动文件
		if fid == fids[len(fids)-1] {
			af := &ActiveFile{
				fid: fid,
				f:   fd,
				off: off,
			}
			db.s.ActiveFile = af
		}
	}
	return nil
}

// Set 设置数据
func (db *DB) Set(key []byte, value []byte) error {
	// 读写锁
	db.rw.Lock()
	defer db.rw.Unlock()

	// 用数据新建一个entry
	entry := NewEntryWithData(key, value)
	buf := entry.Encode() // 解码
	// 写入数据并建立index
	index, err := db.s.writeAt(buf)
	if err != nil {
		return err
	}
	// 更新index
	index.keySize = len(key)
	index.valueSize = len(value)
	db.kd.update(string(key), index)
	return nil
}

// Get 获取数据
func (db *DB) Get(key []byte) (value []byte, err error) {
	// 读写锁
	db.rw.RLock()
	defer db.rw.RUnlock()

	// 通过key找到对应的index
	i := db.kd.find(string(key))
	if i == nil {
		return nil, KeyNotFoundErr
	}
	// 从磁盘中读取数据
	dataSize := MetaSize + i.keySize + i.valueSize
	buf := make([]byte, dataSize)

	entry, err := db.s.readFullEntry(i.fid, i.off, buf)
	if err != nil {
		return nil, err
	}
	return entry.value, nil
}
