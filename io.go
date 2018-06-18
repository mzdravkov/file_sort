package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"io"
	"os"
	"strconv"
)

var writerInput = make(chan []byte)
var readerFinished = make(chan int)
var writerFinished = make(chan int)

// Reads the input file into
func partitioningReader(filename string, assignedMemory, numOfSorters int) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// NOTE: divided by two, because a sorter will allocate
	// bufferSize number of bytes more for the list of buckets
	bufferSize := MB * (assignedMemory / numOfSorters) / 2

	remainder := []byte{}

	partitionsRead := 0

	lastPartition := false
	for !lastPartition {
		// TODO: think about lines longer than bufferSize
		buff := make([]byte, bufferSize)

		// The idea here is that we will fill the whole buffer with data,
		// but since it will probably end at the middle of a line we
		// subtract bytes from the end of the buffer until we find a '\n'.
		// This part of a line which remains will be stored and copied
		// to the beginning of the buffer the next time we fill it.
		copy(buff, remainder)

		remainingBuff := buff[len(remainder):]

		bytesRead, err := reader.Read(remainingBuff)
		if err != nil && err != io.EOF {
			panic(err)
		}
		// have read the whole file and EOF follows
		if bytesRead+len(remainder) < bufferSize {
			buff = buff[:len(remainder)+bytesRead]
			lastPartition = true
		}

		cutAt := len(buff) - 1
		for ; cutAt >= 0; cutAt-- {
			if buff[cutAt] == '\n' {
				break
			}
		}

		if cutAt == len(buff)-1 {
			remainder = make([]byte, 0)
		} else {
			remainder = buff[cutAt+1:]
			buff = buff[:cutAt+1]
		}

		// the buffer is ready, so we wait for an available sorter
		for {
			sorter := <-sorterPool
			log("sorter taken from pool")
			sorter <- buff
			partitionsRead += 1
			break
		}
	}
	readerFinished <- partitionsRead
}

// Waits for a buffer of sorted lines to be sent it's way and writes it
// to a partition file
func writer(sortedBuffs <-chan []byte) {
	partitionsWritten := 0
	readerFinishedFlag := false
	partitionsRead := 0
	for {
		select {
		case partitionsRead = <-readerFinished:
			readerFinishedFlag = true
		case buff := <-sortedBuffs:
			filename := "partition_" + strconv.Itoa(partitionsWritten)

			log("Writting a sorted buff with size ", len(buff), "to file ", filename)

			file, err := os.Create(filename)
			if err != nil {
				panic(err)
			}
			defer file.Close()
			writer := bufio.NewWriter(file)

			_, err = writer.Write(buff)
			if err != nil {
				panic(err)
			}
			writer.Flush()

			partitionsWritten += 1
		}
		if readerFinishedFlag && partitionsRead == partitionsWritten {
			writerFinished <- partitionsWritten
			return
		}
	}
}

// We create FileLine that will implement the Heap interface, so that when we
// are performing the k-way merge, we can keep all the current lines from the files
// in a heap for a faster and easier retrieval of the min line
type FileLine struct {
	index int
	line  []byte
}

type FileLineHeap []FileLine

func (h FileLineHeap) Len() int           { return len(h) }
func (h FileLineHeap) Less(i, j int) bool { return bytes.Compare(h[i].line, h[j].line) == -1 }
func (h FileLineHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *FileLineHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(FileLine))
}

func (h *FileLineHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func kWayMerge(inputFilePrefix, outputFileName string, firstPartitionIndex, partitionFilesCount int, assignedMemory int) {
	log("Starting a k-way merge for ", inputFilePrefix, firstPartitionIndex, "-", inputFilePrefix, firstPartitionIndex+partitionFilesCount-1)
	file, err := os.Create(outputFileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)

	writeBufferSize := MB * assignedMemory / 2

	// will write output to an in-memory buffer before writting it to disk
	// otherwise we will constantly have one-line writes
	writeBuffer := make([]byte, writeBufferSize)
	writtenToBuffer := 0

	inputBufferSize := MB * assignedMemory / (2 * partitionFilesCount)

	// similarly, we keep a slice with buffers for the input partition files
	inputBuffers := make([]*bytes.Buffer, partitionFilesCount)

	inputFileReaders := make([]*bufio.Reader, partitionFilesCount)
	inputLineReaders := make([]*bufio.Reader, partitionFilesCount)

	partitionFiles := make([]string, partitionFilesCount)

	// open all partition files and create readers
	for i := 0; i < partitionFilesCount; i++ {
		filename := inputFilePrefix + strconv.Itoa(firstPartitionIndex+i)
		partitionFiles[i] = filename
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		inputBuffers[i] = bytes.NewBuffer(make([]byte, 0, inputBufferSize))
		inputBuffers[i].Reset()
		inputFileReaders[i] = bufio.NewReader(file)
	}

	currentLines := &FileLineHeap{}

	// returns the line read (if available) and a boolean whether a line was read or not
	readLine := func(inputIndex int) ([]byte, bool) {
		line, err := inputBuffers[inputIndex].ReadBytes('\n')
		if err == nil {
			return line, true
		} else if err == io.EOF {
			inputBuffers[inputIndex].Write(line)
			_, err := io.CopyN(inputBuffers[inputIndex], inputFileReaders[inputIndex], int64(inputBufferSize-4096-len(line)))
			if err != nil && err != io.EOF {
				panic(err)
			}

			if inputBuffers[inputIndex].Len() == 0 {
				return []byte{}, false
			}

			line, err := inputBuffers[inputIndex].ReadBytes('\n')
			if err != nil {
				panic(err)
			}

			return line, true
		} else {
			panic(err)
		}
	}

	// initial step: read the first line for each of the inputLineReaders
	for i := range inputLineReaders {
		if line, hasLine := readLine(i); hasLine {
			heap.Push(currentLines, FileLine{line: line, index: i})
		}
	}

	// general case step: get the min line from all currentLines and then move forward only that reader
	for currentLines.Len() > 0 {
		minLine := heap.Pop(currentLines).(FileLine)

		// if there's not enough space in the in-memory buffer, we write the buffer to disk
		// and then append the line at the start of the buffer
		if len(minLine.line)+writtenToBuffer > writeBufferSize {
			_, err = writer.Write(writeBuffer[:writtenToBuffer])
			if err != nil {
				panic(err)
			}

			writtenToBuffer = 0
		}
		writtenToBuffer += copy(writeBuffer[writtenToBuffer:], minLine.line)

		// move forward the reader for the file that we just took a line from
		if nextLine, hasLine := readLine(minLine.index); hasLine {
			heap.Push(currentLines, FileLine{line: nextLine, index: minLine.index})
		} else {
			log("Deleting", partitionFiles[minLine.index])
			os.Remove(partitionFiles[minLine.index])
		}
	}
	_, err = writer.Write(writeBuffer[:writtenToBuffer])
	if err != nil {
		panic(err)
	}
	writer.Flush()
}
