package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"time"
)

var minLineLen = 32
var maxLineLen = 42

var chars = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")

func randLine(n int) []byte {
	b := make([]byte, n, n+1)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	b = append(b, '\n')
	return b
}

func genTestFile(fileSize, bufferSize int, fileName string) {
	rand.Seed(time.Now().UnixNano())

	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	buffer := make([]byte, 0, bufferSize)

	// show percentage markers
	fmt.Print("o")
	for i := 0; i < 100; i++ {
		fmt.Print(".")
	}
	fmt.Print("o\no")

	barsShown := 0
	for amountWritten := 0; amountWritten < fileSize-maxLineLen-1; {
		// if writting another buffer to the file will be too much
		// we shrink the buffer size
		if amountWritten+bufferSize > fileSize {
			bufferSize = fileSize - amountWritten
		}

		// the buffer is filled with lines
		for len(buffer) < bufferSize-maxLineLen-1 {
			lineLen := rand.Intn(maxLineLen-minLineLen) + minLineLen
			buffer = append(buffer, randLine(lineLen)...)
		}

		bytesWritten, err := writer.Write(buffer)
		if err != nil {
			panic(err)
		}

		amountWritten += bytesWritten

		// loading bar
		percentWritten := float64(amountWritten) / float64(fileSize)

		newBars := int(100*percentWritten) - barsShown
		for i := 0; i < newBars; i++ {
			fmt.Print("|")
		}
		barsShown += newBars

		buffer = buffer[:0]
	}
	fmt.Println("o")
}
