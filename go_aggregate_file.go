package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

func main() {
	// Define PK columns
	pkColumns := []string{"id", "name"}

	// Open the CSV file
	file, err := os.Open("small_mock_data.csv")
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	// Create a CSV reader
	reader := csv.NewReader(file)

	// Read the header (if necessary)
	header, err := reader.Read()
	if err != nil {
		fmt.Printf("Error reading header: %v\n", err)
		return
	}
	fmt.Printf("Header: %v\n", header)

	// Map PK columns to their indices
	pkIndices := make([]int, 0, len(pkColumns))
	for _, pk := range pkColumns {
		index := indexOf(header, pk)
		if index == -1 {
			fmt.Printf("Error: PK column '%s' not found in header\n", pk)
			return
		}
		pkIndices = append(pkIndices, index)
	}

	// Initialize variables to track the previous PK
	var lastPK string

	// Process the CSV line by line
	for {
		record, err := reader.Read()
		if err != nil {
			// Check if it's the end of file
			if err.Error() == "EOF" {
				break
			}
			fmt.Printf("Error reading line: %v\n", err)
			return
		}

		// Assume the first column is the PK
		currentPK := generateCompositeKey(record, pkIndices)

		// If the PK changes, process the first row of the new group
		if currentPK != lastPK {
			// Process the current record as the first for its PK group
			fmt.Printf("Processing record: %v\n", record)

			// Update the lastPK
			lastPK = currentPK
		}
	}
}

// indexOf finds the index of a column in the header
func indexOf(header []string, column string) int {
	for i, col := range header {
		if col == column {
			return i
		}
	}
	return -1
}

// generateCompositeKey creates a unique key from PK column values
func generateCompositeKey(record []string, indices []int) string {
	var parts []string
	for _, idx := range indices {
		parts = append(parts, record[idx])
	}
	return strings.Join(parts, "|")
}
