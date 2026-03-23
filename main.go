package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	glide "github.com/valkey-io/valkey-glide/go/v2"
	"github.com/valkey-io/valkey-glide/go/v2/config"
	"github.com/valkey-io/valkey-glide/go/v2/pipeline"
	"io"
)

type jsonMultiRequest struct {
	HashList   []string `json:"hashlist"`
	OffsetList []uint32 `json:"offsetlist"`
}

func packTo64(songId uint32, offset uint32) uint64 {
	return (uint64(songId) << 32) | uint64(offset)
}

func unpackFrom64(packed uint64) (uint32, uint32) {
	songId := uint32(packed >> 32)
	offset := uint32(packed & 0xFFFFFFFF)
	return songId, offset
}

func arrayAnyToNestedString(array []any) [][]string {
	var tmp [][]string
	for _, element := range array {
		stringsSlice, ok := element.([]string)
		if !ok {
			fmt.Println("error on casting to array string")
			return nil
		}
		tmp = append(tmp, stringsSlice)
	}
	return tmp
}

func nestedStrToNestedInt(array [][]string) [][]uint64 {
	var tmp [][]uint64
	for i, _ := range array {
		tmp = append(tmp, []uint64{})
		for _, element2 := range array[i] {
			packed := binary.LittleEndian.Uint64([]byte(element2))
			tmp[i] = append(tmp[i], packed)
			songId, offset := unpackFrom64(packed)
			fmt.Printf("SongID=%d, Offset=%d\n", songId, offset)
		}
	}

	return tmp
}

// func extractOffsets(nestedInts [][]uint64) ([][]uint32, [][]uint32) {
// 	var ids [][]uint32
// 	var off [][]uint32
// 	for i, element := range nestedInts {
// 		ids = append(ids, []uint32{})
// 		off = append(off, []uint32{})
// 		for _, element2 = range element {
// 			tmpid, tmpoff = unpackFrom64(element2)
// 			ids[i][j], off[i][j] = unpackFrom64(nestedInts[i][j])
// 		}
// 	}
// }

func extractOffsetPairs(nestedInts [][]uint64) ([][]uint32, [][]uint32) {
	var ids [][]uint32
	var off [][]uint32
	for i, element := range nestedInts {
		ids = append(ids, []uint32{})
		off = append(off, []uint32{})
		for _, element2 := range element {
			tmpid, tmpoffset := unpackFrom64(element2)
			ids[i] = append(ids[i], tmpid)
			off[i] = append(off[i], tmpoffset)
		}
	}
	return ids, off
}

func determinePeak(nestedInts [][]uint64, clientOffsets []uint32) int {
	// var counterMap = make(map[uint32]map[uint32]int)
	//    songid     offset  count
	songIds, offsets := extractOffsetPairs(nestedInts)
	if (len(songIds) == len(offsets)) && (len(clientOffsets) == len(offsets)) {
		print("all the same length just checking. ")
	}
	return 0
}

func handleMultiGetLookup(client *glide.Client, c *gin.Context) {
	jsonData, err := io.ReadAll(c.Request.Body)
	if err != nil {
		fmt.Println("error reading request body: ", err)
		return
	}
	var hashData jsonMultiRequest
	err = json.Unmarshal(jsonData, &hashData)
	if err != nil {
		fmt.Println("error unmarshalling json: ", err)
		return
	}

	batch := pipeline.NewStandaloneBatch(false)

	for _, element := range hashData.HashList {
		batch.LRange(element, 0, -1)
	}

	data, err := client.Exec(context.Background(), *batch, false)

	if err != nil {
		fmt.Println("error on batch exec: ", err)
		return
	}
	nestedStrData := arrayAnyToNestedString(data)
	var nestedIntData [][]uint64 = nestedStrToNestedInt(nestedStrData)
	determinePeak(nestedIntData, hashData.OffsetList)
	fmt.Println(hashData.OffsetList)
	fmt.Println(nestedIntData)
}

func main() {
	host := "localhost"
	port := 6379

	config := config.NewClientConfiguration().
		WithAddress(&config.NodeAddress{Host: host, Port: port})
	client, err := glide.NewClient(config)
	if err != nil {
		fmt.Println("There was an error: ", err)
		return
	}

	router := gin.Default()
	router.POST("/multiget", func(c *gin.Context) { handleMultiGetLookup(client, c) })
	router.Run()

	client.Close()
}
