package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

var sorterPool = make(chan chan []byte, 8)
var writerInput = make(chan []byte)
var readerFinished = make(chan int)
var writerFinished = make(chan struct{})

func init() {
	for i := 0; i < 8; i++ {
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

func sort(buff []byte) {
	fmt.Printf("Starting to sort %d bytes of data.\n", len(buff))
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
		bufferSize := 10 * 1024 * 1024
		buff := make([]byte, bufferSize)

		copy(buff, remainder)

		remainingBuff := buff[len(remainder):]

		_, err = reader.Read(remainingBuff)
		if err == io.EOF {
			break
		}

		newLine := []byte("\n")[0]
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
