package gokv_Bitcask

const (
	DefaultSegmentSize = 256 * MB
)

type Options struct {
	Dir         string // db目录
	SegmentSize int64  // 段大小
}
