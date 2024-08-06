package main

// Index is the index of a file
type Index struct {
	fid       int   // file id
	off       int64 // 位置
	timeStamp uint64
	keySize   int
	valueSize int
}

// keyDir is the directory of keys
type keyDir struct {
	index map[string]*Index
}

// 找到key对应的index
func (kd *keyDir) find(key string) *Index {
	i := kd.index[key]
	return i
}

// 更新keyDir
func (kd *keyDir) update(key string, i *Index) {
	kd.index[key] = i
}
