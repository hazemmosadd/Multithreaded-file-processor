package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
)

func main() {
	// Open a file for writing
	file, err := os.Create("output.csv")
	if err != nil {
		log.Fatal("Error creating file:", err)
	}
	defer file.Close()

	// Create a new CSV writer using the file
	writer := csv.NewWriter(file)
	defer writer.Flush() // Ensure all data is written to the file

	// Write the header row
	header := []string{"name", "age", "email"}
	if err := writer.Write(header); err != nil {
		log.Fatal("Error writing header:", err)
	}

	// Generate 20,000 data rows
	totalRows := 20000
	for i := 1; i <= totalRows; i++ {
		name := fmt.Sprintf("Name_%d", i)
		age := strconv.Itoa((i % 100) + 20) // Ages vary between 20 and 119
		email := fmt.Sprintf("user%d@example.com", i)

		record := []string{name, age, email}
		if err := writer.Write(record); err != nil {
			log.Fatal("Error writing record:", err)
		}
	}

	log.Println("CSV file with 20,000 records generated successfully.")
}
