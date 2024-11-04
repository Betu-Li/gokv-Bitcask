package gokv_Bitcask

import (
	"os"
	"path"
	"strconv"
	"strings"
)

// getFids 获取缓存
func getFids(dir string) (fids []int, err error) {
	files, err := os.ReadDir(dir) // 读取目录
	if err != nil {
		return nil, err
	}
	// 遍历目录下的文件
	for _, f := range files {
		fileName := f.Name()
		filePath := path.Base(fileName)
		// 如果文件后缀是.dat
		if path.Ext(filePath) == fileSuffix {
			// 获取文件前缀
			filePrefix := strings.TrimSuffix(filePath, fileSuffix)
			// 将文件前缀转换为int类型，加入到fids中
			fid, err := strconv.Atoi(filePrefix)
			if err != nil {
				return nil, err
			}
			fids = append(fids, fid)
		}
	}
	return fids, nil
}

// getSegmentSize 获取段大小
func getSegmentSize(SegmentSize int64) int64 {
	if SegmentSize <= 0 {
		return DefaultSegmentSize
	}
	return SegmentSize
}

// isDirExist 判断目录是否存在
func isDirExist(dir string) (bool, error) {
	_, err := os.Stat(dir) // 获取文件信息
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
