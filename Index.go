package main

// Index is the index of a file
type Index struct {
	fid       int   // file id
	off       int64 // 位置
	timeStamp uint64
}

// keyDir is the directory of keys
type keyDir struct {
	index map[string]*Index
}
