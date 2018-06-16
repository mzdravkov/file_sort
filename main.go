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
			if buff[cutAt] == newLine {
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
			if len(buff) < 1024 {
				fmt.Println("Midget: ", partitionsWritten)
				fmt.Println(string(buff))
			}

			file, err := os.Create("partition_" + strconv.Itoa(partitionsWritten) + ".txt")
			if err != nil {
				panic(err)
			}
			defer file.Close()
			writer := bufio.NewWriter(file)

			bytesWritten, err := writer.Write(buff)
			if err != nil {
				panic(err)
			}
			writer.Flush()
			fmt.Println("Writer wrote ", bytesWritten, " to partition ", partitionsWritten)

			partitionsWritten += 1
		}
		if readerFinishedFlag && partitionsRead == partitionsWritten {
			fmt.Println(partitionsWritten)
			kWayMerge(partitionsWritten, bufferSize)
			writerFinished <- struct{}{}
			return
		}
	}
}

// TODO: think about maybe doing one initial to merge groups of N files which may increase the overall performance
func kWayMerge(partitionFilesCount int, bufferSize int) {
	fmt.Println("Starting a k-way merge")
	file, err := os.Create("output.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)

	// will write output to an in-memory buffer before writting it to disk
	// otherwise we will have constantly one-line writes
	writeBuffer := make([]byte, bufferSize)
	writtenToBuffer := 0

	inputReaders := make([]*bufio.Reader, partitionFilesCount)

	partitionFiles := make([]string, partitionFilesCount)

	// open all partition files and create readers
	for i := 0; i < partitionFilesCount; i++ {
		filename := "partition_" + strconv.Itoa(i) + ".txt"
		fmt.Println(filename)
		partitionFiles[i] = filename
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		inputReaders[i] = bufio.NewReader(file)
	}

	currentLines := make([][]byte, len(inputReaders))

	deletePartition := func(i int) {
		inputReaders = append(inputReaders[:i], inputReaders[i+1:]...)
		currentLines = append(currentLines[:i], currentLines[i+1:]...)
		fmt.Println("deleting " + partitionFiles[i])
		// os.Remove(partitionFiles[i])
		partitionFiles = append(partitionFiles[:i], partitionFiles[i+1:]...)
		fmt.Println(partitionFiles)
	}

	// initial step: read the first line for each of the inputReaders
	partitionsForDeletion := make([]int, 0)
	for i, reader := range inputReaders {
		fmt.Println(i)
		if line, err := reader.ReadBytes(newLine); err == nil {
			currentLines[i] = line
		} else {
			if err == io.EOF {
				// if there are no lines in a file, we remove the partition file and the corresponding reader
				partitionsForDeletion = append(partitionsForDeletion, i)
			} else {
				panic(err)
			}
		}
	}
	for i := 0; i < len(partitionsForDeletion); i++ {
		deletePartition(partitionsForDeletion[i])
		// if we delete a partition, all indices greater than the deleted one must be decremented
		for j := i + 1; j < len(partitionsForDeletion); j++ {
			if partitionsForDeletion[j] > partitionsForDeletion[i] {
				partitionsForDeletion[j] -= 1
			}
		}
	}

	// general case step: get the min line from all currentLines and then move forward only that reader
	for len(currentLines) > 0 {
		minLine := 0
		for i, line := range currentLines[1:] {
			if bytes.Compare(currentLines[minLine], line) == 1 {
				minLine = i
			}
		}

		// if there's not enough space in the in-memory buffer, we write the buffer to disk
		// and then append the line at the start of the buffer
		if len(currentLines[minLine])+writtenToBuffer > bufferSize {
			_, err = writer.Write(writeBuffer[:writtenToBuffer])
			if err != nil {
				panic(err)
			}
			writer.Flush()

			writtenToBuffer = 0
		}
		writtenToBuffer += copy(writeBuffer[writtenToBuffer:], currentLines[minLine])

		// move forward the reader for the file that we just took a line from
		if nextLine, err := inputReaders[minLine].ReadBytes(newLine); err == nil {
			currentLines[minLine] = nextLine
		} else {
			if err == io.EOF {
				// if all lines from this file have bean read, we delete it
				deletePartition(minLine)
			} else {
				panic(err)
			}
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
	// numOfSorters := maxProcs * inputSize / *assignedMemory
	fmt.Println(*assignedMemory)
	numOfSorters := maxProcs
	fmt.Println("sorters: ", numOfSorters)
	sorterPool = make(chan chan []byte, int(numOfSorters))
	createSorters(int(numOfSorters))

	bufferSize := 1024 * 1024 * (*assignedMemory / numOfSorters) / 2
	fmt.Println("Buffer size: ", bufferSize)

	go writer(writerInput, bufferSize)
	partitioningReader(inputFile, bufferSize)
	<-writerFinished

	if *verify {
		fmt.Println("verifying...")
	}
	fmt.Println("Done")
}
