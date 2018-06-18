package main

import (
	"bufio"
	"bytes"
	"io"
	"os"
)

func verifyFileIsSorted(filename string, assignedMemory int) bool {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	fileReader := bufio.NewReader(file)

	inputBuffer := bytes.NewBuffer(make([]byte, 0, assignedMemory))

	// returns the line read (if available) and a boolean whether a line was read or not
	readLine := func() ([]byte, bool) {
		line, err := inputBuffer.ReadBytes('\n')
		if err == nil {
			return line, true
		} else if err == io.EOF {
			inputBuffer.Write(line)
			_, err := io.CopyN(inputBuffer, fileReader, int64(MB*assignedMemory-4096-len(line)))
			if err != nil && err != io.EOF {
				panic(err)
			}

			if inputBuffer.Len() == 0 {
				return []byte{}, false
			}

			line, err := inputBuffer.ReadBytes('\n')
			if err != nil {
				panic(err)
			}

			return line, true
		} else {
			panic(err)
		}
	}

	prev, hasLine := readLine()
	if !hasLine {
		return true
	}
	for {
		current, hasLine := readLine()
		if !hasLine {
			return true
		}
		if bytes.Compare(prev, current) != -1 {
			return false
		}
		prev = current
	}
}
