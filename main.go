package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	glide "github.com/valkey-io/valkey-glide/go/v2"
	"github.com/valkey-io/valkey-glide/go/v2/config"
	"github.com/valkey-io/valkey-glide/go/v2/pipeline"
	"io"
)

type jsonMultiRequest struct {
	HashList []string `json:"hashlist"`
}
type jsonSingleRequest struct {
	Hash string `json:"hash"`
}

type songId uint32
type offset uint32

func packTo64(songId uint32, offset uint32) uint64 {
	return (uint64(songId) << 32) | uint64(offset)
}

func unpackFrom64(packed uint64) (uint32, uint32) {
	songId := uint32(packed >> 32)
	offset := uint32(packed & 0xFFFFFFFF)
	return songId, offset
}

func handleSingleHashLookup(client *glide.Client, c *gin.Context) {
	jsonData, err := io.ReadAll(c.Request.Body)
	if err != nil {
		fmt.Println("error reading request body: ", err)
		return
	}
	var hashData jsonSingleRequest
	err = json.Unmarshal(jsonData, &hashData)
	if err != nil {
		fmt.Println("error unmarshalling json: ", err)
		return
	}

	fmt.Println(hashData.Hash)

	keys, err := client.LRange(context.Background(), hashData.Hash, 0, -1)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	fmt.Println(keys)
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

	fmt.Println("data from batch: ", data)
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
	router.POST("/singleget", func(c *gin.Context) { handleSingleHashLookup(client, c) })
	router.POST("/multiget", func(c *gin.Context) { handleMultiGetLookup(client, c) })
	router.Run()

	client.Close()
}
