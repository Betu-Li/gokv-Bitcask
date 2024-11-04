package main

import (
	"fmt"
	"gokv-Bitcask"
)

func main() {
	// 创建一个DB,并指定目录和段大小
	db, err := gokv_Bitcask.NewDB(&gokv_Bitcask.Options{Dir: "tmp", SegmentSize: 1024}) // 创建一个DB,并指定目录和段大小
	if err != nil {
		fmt.Println(err)
		return
	}

	// 清理掉无效的数据
	if err = db.Merge(); err != nil {
		fmt.Println(err)
	}

	// 写入数据
	err = db.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		fmt.Println(err)
		return
	}

	// 读取数据
	val, err := db.Get([]byte("key1"))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(val))

}
