package main

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
	"sync"
)

type ChunkProcessor interface {
	ProcessChunk([]User)
}

type InsertOneProcessor struct {
	usersColl *mongo.Collection
	wg        *sync.WaitGroup
}

func (dp *InsertOneProcessor) ProcessChunk(chunkOfUsers []User) {
	defer dp.wg.Done()
	for _, user := range chunkOfUsers {
		_, err := dp.usersColl.InsertOne(context.TODO(), user)
		if err != nil {
			log.Printf("Error inserting user: %v", err)
		}
	}
}

type InsertManyProcessor struct {
	usersColl *mongo.Collection
	wg        *sync.WaitGroup
}

func (ap *InsertManyProcessor) ProcessChunk(chunkOfUsers []User) {
	defer ap.wg.Done()
	documents := make([]interface{}, len(chunkOfUsers))
	for i, user := range chunkOfUsers {
		documents[i] = user
	}
	_, err := ap.usersColl.InsertMany(context.TODO(), documents)
	if err != nil {
		log.Printf("Error inserting user: %v", err)
	}
}
