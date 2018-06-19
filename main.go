package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
)

var MB = 1024 * 1024

var showMessages bool = false

func log(messages ...interface{}) {
	if showMessages {
		fmt.Println(messages...)
	}
}

func printHelp() {
	fmt.Println("USAGE: file_sort [OPTIONS]... FILE\nSorts big files lexicographically.\n\nOptions:")
	flag.VisitAll(func(f *flag.Flag) {
		fmt.Printf("  %8s%8s\t%s\n", f.Name, f.DefValue, f.Usage)
	})
}

func main() {
	assignedMemory := flag.Int("memory", 1024, "The amount of memory assigned for the sorting program (in MB).\n\t\t\tNote: It is not strict and it may use slightly more than this.")
	verify := flag.Bool("verify", false, "Checks if the input file is sorted.")
	twoStepMerge := flag.Int("two-step", 512, "Will perform two-step merge when files are more than the specified number.\n\t\t\tFirst step merges log2(partitionFilesCount) and the second merges the resulting files.")
	outputFileName := flag.String("output", "output", "The name of the output sorted file.")
	// -1 so that we don't change the setting, just query the current value
	n := flag.Int("n", runtime.GOMAXPROCS(-1), "Number of parallel sorters. Defaults to GOMAXPROCS.")
	help := flag.Bool("help", false, "Prints this message.")
	verbose := flag.Bool("verbose", false, "Shows status messages.")
	genFileSize := flag.Int("gen-file", 0, "Generates a test file with the given size filled with random lines.\n\t\t\tThe -memory flag determines the size of the buffer used.\n\t\t\tNote that if you don't pass -output neither here nor when sorting the file, you will overwrite it.\n\t\t\tDo not pass the FILE argument whit this option.")
	flag.Parse()

	showMessages = *verbose

	// the GC should allocate (about?) twice the actually used memory
	*assignedMemory = MB * (*assignedMemory) / 2

	if *genFileSize > 0 {
		genTestFile(*genFileSize*MB, *assignedMemory, *outputFileName)
		return
	}

	args := flag.Args()
	if len(args) == 0 || *help {
		printHelp()
		return
	}

	inputFile := flag.Arg(0)

	if *verify {
		if sorted := verifyFileIsSorted(inputFile, *assignedMemory); !sorted {
			os.Stderr.WriteString("Not sorted\n")
		}
		return
	}

	numOfSorters := *n
	sorterPool = make(chan chan []byte, int(numOfSorters))
	createSorters(int(numOfSorters))

	go writer(writerInput)
	partitioningReader(inputFile, *assignedMemory, numOfSorters)

	// wait for the writer to finish
	partitionsWritten := <-writerFinished

	for i := 0; i < numOfSorters; i++ {
		killSorters <- struct{}{}
	}

	// if there are too many files, do a two stage k-way merge
	if partitionsWritten > *twoStepMerge {
		// get how much partition files will be merged to one bigger partition file on the first step
		base := int(math.Floor(math.Log2(float64(partitionsWritten))))
		// NOTE: the parenthesis are important
		lPartitionsCount := base * (partitionsWritten / base)
		rPartitionsCount := partitionsWritten - lPartitionsCount
		log("Two-step merge")

		log("Merge", lPartitionsCount, "partitions in groups by", base)
		bigPartitions := 0
		for i := 0; i < lPartitionsCount; i += base {
			kWayMerge("partition_", "big_partition_"+strconv.Itoa(bigPartitions), i, base, *assignedMemory)
			bigPartitions += 1
		}

		log("Merge the remaining", rPartitionsCount, "into another big partition")

		// if one file remains, just rename it
		if rPartitionsCount == 1 {
			if err := os.Rename("partition_"+strconv.Itoa(lPartitionsCount), "big_partition_"+strconv.Itoa(bigPartitions)); err != nil {
				panic(err)
			}
			bigPartitions += 1
		} else if rPartitionsCount > 1 {
			kWayMerge("partition_", "big_partition_"+strconv.Itoa(bigPartitions), lPartitionsCount, rPartitionsCount, *assignedMemory)
			bigPartitions += 1
		}

		// merge all big partititins into one output file
		kWayMerge("big_partition_", *outputFileName, 0, bigPartitions, *assignedMemory)
	} else {
		kWayMerge("partition_", *outputFileName, 0, partitionsWritten, *assignedMemory)
	}

	log("Done")
}
