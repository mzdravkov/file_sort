package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
)

var sorterPool chan chan []byte
var writerInput = make(chan []byte)
var readerFinished = make(chan int)
var writerFinished = make(chan int)
var killSorters = make(chan struct{})

func createSorters(n int) {
	for i := 0; i < n; i++ {
		sorterPool <- createSorter()
	}
}

func createSorter() chan []byte {
	ch := make(chan []byte)
	fmt.Println("creating a sorter")
	go func() {
		for {
			select {
			case buff := <-ch:
				sortBuffer(buff)
				writerInput <- buff
				sorterPool <- ch
				fmt.Println("sorter returning to pool")
			case <-killSorters:
				fmt.Println("Another one bites the dust...")
				return
			}
		}
	}()
	return ch
}

type ByteSliceSort [][]byte

func (a ByteSliceSort) Len() int           { return len(a) }
func (a ByteSliceSort) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByteSliceSort) Less(i, j int) bool { return bytes.Compare(a[i][1:], a[j][1:]) == -1 }

// TODO: Currently I'm sorting by bytes.Compare. However, as bytes V < c, which is probably not what we desire
func nextSort(lines [][]byte, signalFinishChan chan struct{}) {
	sort.Sort(ByteSliceSort(lines))

	signalFinishChan <- struct{}{}
}

// This function assumes that the buckets of lines are generated from the data in dest,
// hence the size of dest should be exactly the same as the size of all the data in the buckets
func flattenBucketsOfLines(dest []byte, buckets [][][]byte) {
	var destPos int
	for _, bucket := range buckets {
		for _, line := range bucket {
			destPos += copy(dest[destPos:], line)
		}
	}
}

// TODO: maybe use sample sort, but it may be hard to do so though, because we don't have
// all lines at the beginning when assigning them to buckets
// TODO: maybe use my own append function, because go's append grows the underlying array twice
// this may cause millions of unneccessarily allocated []byte elements
func bucketSort(buff []byte) {
	buckets := make([][][]byte, 256)

	reader := bufio.NewReader(bytes.NewBuffer(buff))

	for {
		line, err := reader.ReadBytes('\n')
		// TODO: is it sure that I should break here or should I loop once more?
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		// TODO: think about zero-length lines

		bucketIndex := int(line[0])
		// buckets[bucketIndex] = append(buckets[bucketIndex], line)

		if len(buckets[bucketIndex])+1 > cap(buckets[bucketIndex]) {
			tmp := make([][]byte, len(buckets[bucketIndex]), len(buckets[bucketIndex])+1+len(buckets[bucketIndex])/2)
			copy(tmp, buckets[bucketIndex])
			buckets[bucketIndex] = tmp
		}
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

func sortBuffer(buff []byte) {
	fmt.Printf("Starting to sort %d bytes of data.\n", len(buff))
	bucketSort(buff)
}

func partitioningReader(filename string, bufferSize int) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	remainder := []byte{}

	partitionsRead := 0

	lastPartition := false
	for {
		// TODO: think about lines longer than bufferSize
		buff := make([]byte, bufferSize)

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

		for {
			sorter := <-sorterPool
			fmt.Println("sorter taken from pool")
			sorter <- buff
			partitionsRead += 1
			break
		}
		if lastPartition {
			break
		}
	}
	readerFinished <- partitionsRead
}

func writer(sortedBuffs <-chan []byte, bufferSize int) {
	partitionsWritten := 0
	readerFinishedFlag := false
	partitionsRead := 0
	for {
		select {
		case partitionsRead = <-readerFinished:
			readerFinishedFlag = true
		case buff := <-sortedBuffs:
			fmt.Printf("Writting a sorted buff with size %d to file.\n", len(buff))

			file, err := os.Create("partition_" + strconv.Itoa(partitionsWritten))
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
			fmt.Println(partitionsWritten)
			writerFinished <- partitionsWritten
			return
		}
	}
}

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

// TODO: think about maybe doing one initial to merge groups of N files which may increase the overall performance
// TODO: use buffering when reading the partition files
func kWayMerge(inputFilePrefix, outputFileName string, firstPartitionIndex, partitionFilesCount, bufferSize int, assignedMemory int) {
	fmt.Println("Starting a k-way merge")
	file, err := os.Create(outputFileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)

	writeBufferSize := 1024 * 1024 * assignedMemory / partitionFilesCount

	// will write output to an in-memory buffer before writting it to disk
	// otherwise we will constantly have one-line writes
	writeBuffer := make([]byte, writeBufferSize)
	writtenToBuffer := 0

	inputBufferSize := 1024 * 1024 * assignedMemory / (2 * partitionFilesCount)

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
			_, err := io.CopyN(inputBuffers[inputIndex], inputFileReaders[inputIndex], int64(inputBufferSize-512-len(line)))
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
	// partitionsForDeletion := make([]int, 0)
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
			os.Remove(inputFilePrefix + strconv.Itoa(minLine.index))
		}
	}
	_, err = writer.Write(writeBuffer[:writtenToBuffer])
	if err != nil {
		panic(err)
	}
	writer.Flush()
}

func printHelp() {
	fmt.Println("USAGE: file_sort [OPTIONS]... FILE\nSorts big files lexicographically.\n\nOptions:")
	flag.VisitAll(func(f *flag.Flag) {
		fmt.Printf("\t%s\t%s\t%s\n", f.Name, f.Usage, f.DefValue)
	})
}

func main() {
	assignedMemory := flag.Int("memory", 1024, "The amount of memory assigned for the sorting program (in MB).\n\t\tNote: It is not strict and it may use slightly more than this.")
	verify := flag.Bool("verify", false, "Perform a verification step to check that the output file is sorted (shouldn't be neccessary)")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printHelp()
		return
	}

	inputFile := flag.Arg(0)

	// inputFileInfo, err := os.Stat(inputFile)
	// if err != nil {
	// 	panic(err)
	// }

	// get the size in MB
	// inputSize := int(inputFileInfo.Size() / (1024 * 1024))

	// don't change the setting, just query the current value
	maxProcs := runtime.GOMAXPROCS(-1)
	fmt.Println(*assignedMemory)
	numOfSorters := maxProcs
	fmt.Println("sorters: ", numOfSorters)
	sorterPool = make(chan chan []byte, int(numOfSorters))
	createSorters(int(numOfSorters))

	bufferSize := 1024 * 1024 * (*assignedMemory / numOfSorters) / 2
	fmt.Println("Buffer size: ", bufferSize)

	go writer(writerInput, bufferSize)
	partitioningReader(inputFile, bufferSize)

	// wait for the writer to finish
	partitionsWritten := <-writerFinished

	for i := 0; i < numOfSorters; i++ {
		killSorters <- struct{}{}
	}

	// if there are too many files, do a two stage k-way merge
	if partitionsWritten > 128 {
		base := int(math.Floor(math.Log2(float64(partitionsWritten))))
		for partitionsWritten%base != 0 {
			base += 1
		}

		bigPartitions := 0
		for i := 0; i < partitionsWritten; i += base {
			kWayMerge("partition_", "big_partition_"+strconv.Itoa(bigPartitions), i, base, bufferSize, *assignedMemory)
			bigPartitions += 1
		}

		kWayMerge("big_partition_", "output", 0, partitionsWritten/base, bufferSize, *assignedMemory)
	} else {
		kWayMerge("partition_", "output", 0, partitionsWritten, bufferSize, *assignedMemory)
	}

	if *verify {
		fmt.Println("verifying...")
	}
	fmt.Println("Done")
}
