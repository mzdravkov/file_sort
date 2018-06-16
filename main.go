package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
)

var sorterPool = make(chan chan []byte, 8)
var writerInput = make(chan []byte)
var readerFinished = make(chan int)
var writerFinished = make(chan struct{})
var newLine = []byte("\n")[0]

func init() {
	for i := 0; i < 1; i++ {
		sorterPool <- createSorter()
	}
}

func createSorter() chan []byte {
	ch := make(chan []byte)
	fmt.Println("creating a sorter")
	go func() {
		for {
			buff := <-ch
			sort(buff)
			writerInput <- buff
			sorterPool <- ch
			fmt.Println("sorter returning to pool")
		}
	}()
	return ch
}

func insertionSort(lines [][]byte) {
	for i := 1; i < len(lines); i++ {
		for j := i; j > 0 && bytes.Compare(lines[j-1], lines[j]) > 1; j-- {
			lines[j-1], lines[j] = lines[j], lines[j-1]
		}
	}
}

func nextSort(lines [][]byte, signalFinishChan chan struct{}) {
	insertionSort(lines)

	signalFinishChan <- struct{}{}
}

// This function assumes that the buckets of lines are generated from the data in dest,
// hence the size of dest should be exactly the same as the size of all the data in the buckets
func flattenBucketsOfLines(dest []byte, buckets [][][]byte) {
	var totalLen int
	for _, bucket := range buckets {
		for _, line := range bucket {
			totalLen += len(line)
		}
	}

	var destPos int
	for _, bucket := range buckets {
		for _, line := range bucket {
			destPos += copy(dest[destPos:], line)
		}
	}
}

func bucketSort(buff []byte) {
	buckets := make([][][]byte, 256)

	reader := bufio.NewReader(bytes.NewBuffer(buff))

	for {
		line, err := reader.ReadBytes(newLine)
		// TODO: is it sure that I should break here or should I loop once more?
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		// TODO: think about zero-length lines

		bucketIndex := int(line[0])
		buckets[bucketIndex] = append(buckets[bucketIndex], line)
	}

	signalFinishChan := make(chan struct{}, 256)
	for _, bucket := range buckets {
		go nextSort(bucket, signalFinishChan)
	}
	for i := 0; i < 256; i++ {
		<-signalFinishChan
	}

	// overwrite the sorted data back to the source buffer
	flattenBucketsOfLines(buff, buckets)
}

func sort(buff []byte) {
	fmt.Printf("Starting to sort %d bytes of data.\n", len(buff))
	bucketSort(buff)
}

func partitioningReader(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	remainder := []byte{}

	partitionsRead := 0

	for {
		// TODO: think about lines longer than bufferSize
		bufferSize := 10 * 1024 * 1024
		buff := make([]byte, bufferSize)

		copy(buff, remainder)

		remainingBuff := buff[len(remainder):]

		_, err = reader.Read(remainingBuff)
		if err == io.EOF {
			break
		}

		// newLine := []byte("\n")[0]
		cutAt := len(buff) - 1
		for ; cutAt >= 0; cutAt-- {
			if buff[cutAt] == newLine {
				break
			}
		}

		remainder = buff[cutAt+1:]

		buff = buff[:cutAt+1]

		fmt.Println("Reader: waiting for available sorter")
		for {
			sorter := <-sorterPool
			fmt.Println("sorter taken from pool")
			sorter <- buff
			partitionsRead += 1
			break
		}
	}
	readerFinished <- partitionsRead
}

func writer(sortedBuffs <-chan []byte) {
	partitionsWritten := 0
	readerFinishedFlag := false
	partitionsRead := 0
	for {
		select {
		case partitionsRead = <-readerFinished:
			readerFinishedFlag = true
		case buff := <-sortedBuffs:
			fmt.Printf("Writting a sorted buff with size %d to file.\n", len(buff))

			file, err := os.Create("partition_" + strconv.Itoa(partitionsWritten) + ".txt")
			if err != nil {
				panic(err)
			}
			defer file.Close()
			writer := bufio.NewWriter(file)

			_, err = writer.Write(buff)
			if err != nil {
				panic(err)
			}

			partitionsWritten += 1
		}
		if readerFinishedFlag && partitionsRead == partitionsWritten {
			writerFinished <- struct{}{}
		}
	}
}

func main() {
	go writer(writerInput)
	partitioningReader("generated.txt")
	<-writerFinished
	fmt.Println("Done")
}
