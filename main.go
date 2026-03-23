package main

import (
	"context"
	"encoding/binary"
	// "encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	glide "github.com/valkey-io/valkey-glide/go/v2"
	"github.com/valkey-io/valkey-glide/go/v2/config"
	"github.com/valkey-io/valkey-glide/go/v2/pipeline"
	// "io"
	"math"
	"net/http"
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
	for i := range array {
		tmp = append(tmp, []uint64{})
		for _, element2 := range array[i] {
			byteData := []byte(element2)
			if len(byteData) < 8 {
				fmt.Println("data longer than 46 bit")
				continue
			}
			packed := binary.LittleEndian.Uint64(byteData)
			tmp[i] = append(tmp[i], packed)
			// songId, offset := unpackFrom64(packed)
			// fmt.Printf("SongID=%d, Offset=%d\n", songId, offset)
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

func roundto(n float64, m float64) int32 {
	return int32(math.Round(n/m) * m)
}

func determinePeak(nestedInts [][]uint64, clientOffsets []uint32) uint32 {
	var counterMap = make(map[uint32]map[int32]int)
	//    songid     offset  count
	songIds, offsets := extractOffsetPairs(nestedInts)
	if len(songIds) != len(offsets) || len(clientOffsets) != len(offsets) {
		fmt.Println("Bad lists in determinePeak. Lengths don't match")
		return 0
	}

	for i, element := range songIds {
		for j, element2 := range element {
			if counterMap[element2] == nil {
				counterMap[element2] = make(map[int32]int)
			}
			delta := int32(offsets[i][j]) - int32(clientOffsets[i])

			binSize := int32(15)
			binnedDelta := (delta / binSize) * binSize
			counterMap[element2][binnedDelta]++
		}
	}

	var peakSongID uint32
	var peakOffset int32
	var peakCount int

	for songID, offsetMap := range counterMap {
		for offset, count := range offsetMap {
			if count > peakCount {
				peakCount = count
				peakSongID = songID
				peakOffset = offset
			}
		}
	}

	// fmt.Printf("Most common pair: songID=%d, offset=%d, count=%d\n", peakSongID, peakOffset, peakCount)
	return peakSongID
}

func handleMultiGetLookup(client *glide.Client, c *gin.Context) {
	var hashData jsonMultiRequest
	if err := c.ShouldBindJSON(&hashData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON format"})
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
	id := determinePeak(nestedIntData, hashData.OffsetList)
	c.JSON(http.StatusOK, gin.H{"songid": id})
	// fmt.Println(hashData.OffsetList)
	// fmt.Println(nestedIntData)
}

func upload(client *glide.Client, c *gin.Context) {

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

	defer client.Close()
	router := gin.Default()
	router.POST("/multiget", func(c *gin.Context) { handleMultiGetLookup(client, c) })
	router.POST("/upload", func(c *gin.Context) { upload(client, c) })
	router.Run()
}
