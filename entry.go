package gokv_Bitcask

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	MetaSize   = 29
	DeleteFlag = 1
)

type Entry struct {
	key   []byte
	value []byte
	meta  *Meta
}

type Meta struct {
	crc       uint32 //用于校验数据的正确性
	position  uint64 //数据在文件中的位置
	timeStamp uint64 //数据的时间戳
	keySize   uint32
	valueSize uint32
	flag      byte //标记是否删除
}

// NewEntry 创建一个新的空Entry
func NewEntry() *Entry {
	return &Entry{
		meta: &Meta{},
	}
}

// Encode 将Entry编码成byte数组
func (e *Entry) Encode() []byte {
	// 计算元数据的大小
	size := e.Size()
	buf := make([]byte, size)
	//以小端字节序将meta写入到字节数组中
	binary.LittleEndian.PutUint64(buf[4:12], e.meta.position)
	binary.LittleEndian.PutUint64(buf[12:20], e.meta.timeStamp)
	binary.LittleEndian.PutUint32(buf[20:24], e.meta.keySize)
	binary.LittleEndian.PutUint32(buf[24:28], e.meta.valueSize)
	buf[28] = e.meta.flag
	if e.meta.flag != DeleteFlag { // 如果不是删除标记，那么写入key和value
		//将key和value写入到字节数组中
		copy(buf[MetaSize:MetaSize+len(e.key)], e.key)
		copy(buf[MetaSize+len(e.key):MetaSize+len(e.key)+len(e.value)], e.value)
	}
	//计算crc32校验码
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	return buf
}

// DecodeMeta 从Entry中解码出Meta
func (e *Entry) DecodeMeta(buf []byte) {
	e.meta = &Meta{
		crc:       binary.LittleEndian.Uint32(buf[0:4]),
		position:  binary.LittleEndian.Uint64(buf[4:12]),
		timeStamp: binary.LittleEndian.Uint64(buf[12:20]),
		keySize:   binary.LittleEndian.Uint32(buf[20:24]),
		valueSize: binary.LittleEndian.Uint32(buf[24:28]),
	}
}

// DecodePayload 从Entry中解码出key和value
func (e *Entry) DecodePayload(buf []byte) error {
	keyHighBound := int(e.meta.keySize)
	valueHighBound := keyHighBound + int(e.meta.valueSize)
	e.key = buf[0:keyHighBound]
	e.value = buf[keyHighBound:valueHighBound]
	return nil
}

// Size Entry的size是meta+key+value的长度。
func (e *Entry) Size() int {
	return int(MetaSize + e.meta.keySize + e.meta.valueSize)
}

// getCRC 用于计算Entry的CRC32校验码
func (e *Entry) getCRC(buf []byte) uint32 {
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, e.key)
	crc = crc32.Update(crc, crc32.IEEETable, e.value)
	return crc
}

// NewEntryWithData 通过key和value创建一个Entry
func NewEntryWithData(key []byte, value []byte) *Entry {
	return &Entry{
		key:   key,
		value: value,
		meta: &Meta{
			keySize:   uint32(len(key)),
			valueSize: uint32(len(value)),
		},
	}
}
