package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
)

var sorterPool chan chan []byte
var writerInput = make(chan []byte)
var readerFinished = make(chan int)
var writerFinished = make(chan struct{})
var newLine = []byte("\n")[0]

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
			buff := <-ch
			sortBuffer(buff)
			writerInput <- buff
			sorterPool <- ch
			fmt.Println("sorter returning to pool")
		}
	}()
	return ch
}

func insertionSort(lines [][]byte) {
	for i := 1; i < len(lines); i++ {
		for j := i; j > 0 && bytes.Compare(lines[j-1][1:], lines[j][1:]) == 1; j-- {
			lines[j-1], lines[j] = lines[j], lines[j-1]
		}
	}
}

type ByteSliceSort [][]byte

func (a ByteSliceSort) Len() int           { return len(a) }
func (a ByteSliceSort) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByteSliceSort) Less(i, j int) bool { return bytes.Compare(a[i][1:], a[j][1:]) == -1 }

// TODO: Currently I'm sorting by bytes.Compare. However, as bytes V < c, which is probably not what we desire
func nextSort(lines [][]byte, signalFinishChan chan struct{}) {
	// insertionSort(lines)
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

	for {
		// TODO: think about lines longer than bufferSize
		// bufferSize := 10 * 1024 * 1024
		buff := make([]byte, bufferSize)

		copy(buff, remainder)

		remainingBuff := buff[len(remainder):]

		_, err = reader.Read(remainingBuff)
		if err == io.EOF {
			break
		}

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
			kWayMerge(partitionsWritten)
			writerFinished <- struct{}{}
			return
		}
	}
}

// TODO: think about maybe doing one initial to merge groups of N files which may increase the overall performance
func kWayMerge(partitionFiles int) {
	file, err := os.Create("output.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)

	inputReaders := make([]*bufio.Reader, partitionFiles)

	for i := 0; i < partitionFiles; i++ {
		filename := "partition_" + strconv.Itoa(i) + ".txt"
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		inputReaders[i] = bufio.NewReader(file)
	}

	// initial step: read the first line for each of the inputReaders
	inputReadersForDeletion := make([]int, 0)
	currentLines := make([][]byte, 0, len(inputReaders))
	for i, reader := range inputReaders {
		if line, err := reader.ReadBytes(newLine); err == nil {
			currentLines = append(currentLines, line)
		} else {
			if err == io.EOF {
				// if all lines from this file have bean read, we mark it for deletion
				inputReadersForDeletion = append(inputReadersForDeletion, i)
			} else {
				panic(err)
			}
		}
	}
	for _, i := range inputReadersForDeletion {
		inputReaders = append(inputReaders[:i], inputReaders[i+1:]...)
		// TODO: delete the file from the disk
	}

	// general case step: get the min line from all currentLines and then move forward only that reader
	for len(inputReaders) > 0 {
		minLine := 0
		for i, line := range currentLines[1:] {
			if bytes.Compare(currentLines[minLine], line) == 1 {
				minLine = i
			}
		}

		_, err = writer.Write(currentLines[minLine])
		if err != nil {
			panic(err)
		}

		if newLine, err := inputReaders[minLine].ReadBytes(newLine); err == nil {
			currentLines[minLine] = newLine
		} else {
			if err == io.EOF {
				// if all lines from this file have bean read, we delete it
				inputReaders = append(inputReaders[:minLine], inputReaders[minLine+1:]...)
				currentLines = append(currentLines[:minLine], currentLines[minLine+1:]...)
				// TODO: delete the file from the disk
			} else {
				panic(err)
			}
		}
	}
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
	// numOfSorters := maxProcs * inputSize / *assignedMemory
	fmt.Println(*assignedMemory)
	numOfSorters := maxProcs
	fmt.Println("sorters: ", numOfSorters)
	sorterPool = make(chan chan []byte, int(numOfSorters))
	createSorters(int(numOfSorters))

	bufferSize := 1024 * 1024 * (*assignedMemory / numOfSorters) / 2
	fmt.Println("Buffer size: ", bufferSize)

	go writer(writerInput)
	partitioningReader(inputFile, bufferSize)
	<-writerFinished

	if *verify {
		fmt.Println("verifying...")
	}
	fmt.Println("Done")
}
