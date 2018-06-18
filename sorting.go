package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"sort"
)

var sorterPool chan chan []byte
var killSorters = make(chan struct{})

func createSorters(n int) {
	for i := 0; i < n; i++ {
		sorterPool <- createSorter()
	}
}

// Starts a sorter worker in a new goroutine and returns the input chan
// at which buffers with lines for sorting should be sent to it.
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

// Create type that will implement sort.Interface, so that we can use std sorting
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

// Reads linearly all lines and assigns each one to a bucket based on it's first byte
// (there are 256 buckets). This gives us 256 segments of lines. The final position
// of each line (when the data is sorted) will be somewhere in it's segment. Hence,
// we can sort each bucket with another sorting algorithm and concatenate all buckets.
func bucketSort(buff []byte) {
	buckets := make([][][]byte, 256)

	reader := bufio.NewReader(bytes.NewBuffer(buff))

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		// line has at least one byte with '\n'
		bucketIndex := int(line[0])

		// manually grow the slice, so that it grows by 1.5 instead of 2
		// which will signifcantly decrease the non-utilised memory allocation
		// as we expect millions of lines (at the expense of little bit of speed)
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
