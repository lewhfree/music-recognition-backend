package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/gin-gonic/gin"
	glide "github.com/valkey-io/valkey-glide/go/v2"
	"github.com/valkey-io/valkey-glide/go/v2/config"
	"github.com/valkey-io/valkey-glide/go/v2/pipeline"
	"net/http"
)

type jsonMultiRequest struct {
	HashList   []string `json:"hashlist"`
	OffsetList []uint32 `json:"offsetlist"`
}

type jsonMultiUpload struct {
	SongId     uint32   `json:"id"`
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
		}
	}

	return tmp
}

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
	var peakCount int

	for songID, offsetMap := range counterMap {
		for _, count := range offsetMap {
			if count > peakCount {
				peakCount = count
				peakSongID = songID
			}
		}
	}

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
}

func ingest(client *glide.Client, c *gin.Context) {
	var data jsonMultiUpload
	if err := c.ShouldBindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON format"})
		return
	}
	if len(data.HashList) != len(data.OffsetList) {
		fmt.Println("injest hash length doesn't match input offset length")
		c.JSON(http.StatusBadRequest, gin.H{"error": "hash length doesn't match offset length"})
		return
	}

	batch := pipeline.NewStandaloneBatch(false)

	b := make([]byte, 8)

	for i, hash := range data.HashList {
		offset := data.OffsetList[i]

		packed := packTo64(data.SongId, offset)

		binary.LittleEndian.PutUint64(b, packed)
		rawBinaryString := string(b)

		batch.RPush(hash, []string{rawBinaryString})
	}

	_, err := client.Exec(c.Request.Context(), *batch, false)
	if err != nil {
		fmt.Println("Error ingesting batch valkey: ", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save to database"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "ingested",
		"song_id": data.SongId,
	})
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
	router.POST("/upload", func(c *gin.Context) { ingest(client, c) })
	router.Run()
}
