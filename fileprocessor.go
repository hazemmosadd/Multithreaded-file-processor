package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type fileProcessor struct {
	usersColl *mongo.Collection
	chunks    [][]User
	wg        sync.WaitGroup
	processor ChunkProcessor
}

func NewFileProcessor() fileProcessor {
	return fileProcessor{}
}

func (s *fileProcessor) Initiate(uri string, database string, collection string, fileName string, numberOfRoutines int, processorType string) {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf(err.Error())
	}
	s.usersColl = client.Database(database).Collection(collection)
	users, err := s.readCSVAndCreateUsers(fileName)
	if err != nil {
		log.Fatalf(err.Error())
	}
	chuncks, err := s.createChunks(users, numberOfRoutines)
	s.chunks = chuncks

	s.wg.Add(numberOfRoutines)

	if processorType == "insertOne" {
		s.processor = &InsertOneProcessor{usersColl: s.usersColl, wg: &s.wg}
	} else if processorType == "insertMany" {
		s.processor = &InsertManyProcessor{usersColl: s.usersColl, wg: &s.wg}
	} else {
		log.Fatalf("invalid processor type")
	}

	// i don't know if its wrong or right but i will set the waiting groups number here

}

func (s *fileProcessor) readCSVAndCreateUsers(fileName string) ([]User, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	r := csv.NewReader(file)
	r.Read() // to skip the header
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	var users []User

	for _, record := range records {
		age, err := strconv.Atoi(record[1])
		if err != nil {
			log.Printf("Invalid age data: %v", err)
			continue // or return an error if that's preferred
		}
		user := NewUser(record[0], age, record[2])
		users = append(users, user)
	}

	return users, nil
}

func (s *fileProcessor) createChunks(users []User, numChunks int) ([][]User, error) {

	if numChunks <= 0 || numChunks > len(users) {
		log.Fatalf("invalid number of chuncks or goroutines")
	}
	chunks := make([][]User, numChunks)
	chunkSize := len(users) / numChunks
	remainder := len(users) % numChunks

	start := 0
	for i := 0; i < numChunks; i++ {
		end := start + chunkSize
		if i == 0 {
			end += remainder // there is another approach where i distribute the remainder on the chunks
		}
		chunks[i] = users[start:end]
		start = end
	}

	return chunks, nil
}

func (s *fileProcessor) StartProcessing() {
	fmt.Println("Processing ")
	startTime := time.Now()
	for i, chunk := range s.chunks {
		fmt.Println("Routine :", i+1, " will process a chunk of length : ", len(chunk))
		go s.processor.ProcessChunk(chunk)
	}

	s.wg.Wait()
	elapsedTime := time.Since(startTime)
	fmt.Printf("Processing took %s\n", elapsedTime)
}
