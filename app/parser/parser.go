package parser

// parser to create map from text file for each node (tracefile string, node_id int) map[uint32][]int

import (
	"bufio"
	"bytes"
	"github.com/sincronia-coflow/implementation/client"
	"io"
	"os"
	"strconv"
	"strings"
)

// Blob is a lazily evaluated bucket of bytes of fixed size.
// We will pass it to sincronia to transfer as a test.
type Blob struct {
	remaining int
}

func (b *Blob) Read(p []byte) (n int, err error) {
	if b.remaining == 0 {
		return 0, io.EOF
	}

	written := 0
	if l := len(p); l <= b.remaining {
		// fill in len(p) random bytes
		copy(p, bytes.Repeat([]byte{'b'}, l))
		b.remaining -= l
		written = l
	} else {
		copy(p, bytes.Repeat([]byte{'b'}, b.remaining))
		written = b.remaining
		b.remaining = 0
	}

	return written, nil
}

func createBlob(size uint32) io.Reader { //takes as input size in MB and returns an array of that size
	return &Blob{
		remaining: int(size) * 1024 * 1024,
	}
}

// GenerateMapFromTracefile reads an input tracefile and generates coflows in
// sincronia format for use in the client library, as a
func GenerateMapFromTracefile(tracefile string, numCoflows int) map[uint32][]int {
	f, _ := os.Open(tracefile)
	r := bufio.NewReader(f)
	m := make(map[uint32][]int)

	for i := 0; i < numCoflows; i++ {
		line, _ := r.ReadString('\n')
		line = strings.TrimSuffix(line, "\n")
		line = strings.TrimSuffix(line, "\x00")
		coflowInfo := strings.Split(line, " ")
		flows := []int{}
		coflowID, err := strconv.Atoi(coflowInfo[0])
		if err != nil {
			panic(err)
		}

		releaseDate, _ := strconv.Atoi(coflowInfo[1])
		flows = append(flows, releaseDate) //first element of flows is release_date
		for j := range coflowInfo[2:] {
			temp, err := strconv.Atoi(coflowInfo[j+2])
			if err != nil {
				panic(err)
			}

			flows = append(flows, temp)
		}

		m[uint32(coflowID)] = flows

	}

	return m
}

// GenerateCoflowsFromTracefileMaster reads an input tracefile and generates coflows in
// sincronia format for use in appMaster.
func GenerateCoflowsFromTracefileMaster(tracefile string) []client.Coflow {
	coflows := []client.Coflow{}
	f, _ := os.Open(tracefile)
	r := bufio.NewReader(f)
	line, _ := r.ReadString('\n') //read the first line of the tracefile containing the coflow number and number of ports
	line = strings.TrimSuffix(line, "\n")
	coflowInfo := strings.Split(line, " ")
	numCoflows, err := strconv.Atoi(coflowInfo[1])
	if err != nil {
		panic(err)
	}

	flows := []client.Flow{}
	dataID := 0

	for i := 0; i < numCoflows; i++ {
		flows = []client.Flow{}
		line, _ := r.ReadString('\n') //read the first line of the tracefile containing the coflow number and number of ports
		line = strings.TrimSuffix(line, "\n")
		coflowInfo := strings.Split(line, " ")
		coflowID, err := strconv.Atoi(coflowInfo[0])
		if err != nil {
			panic(err)
		}

		numFlows, _ := strconv.Atoi(coflowInfo[2])
		for j := 0; j < numFlows; j++ {
			jobID := coflowID
			from, err := strconv.Atoi(coflowInfo[5+3*j])
			if err != nil {
				panic(err)
			}

			to, err := strconv.Atoi(coflowInfo[6+3*j])
			if err != nil {
				panic(err)
			}

			size, err := strconv.Atoi(coflowInfo[7+3*j])
			if err != nil {
				panic(err)
			}

			flowToAppend := client.Flow{
				JobID: uint32(jobID),
				From:  uint32(from),
				To:    uint32(to),
				Info: client.Data{
					DataID: uint32(dataID),
					Size:   uint32(size * 1024 * 1024),
					Blob:   createBlob(uint32(size)), //[]byte{'h', 'e', 'l', 'l', 'o'},
				},
			}

			flows = append(flows, flowToAppend)
			dataID++
		}

		coflowToAppend := client.Coflow{
			JobID: uint32(coflowID),
			Flows: flows,
		}

		coflows = append(coflows, coflowToAppend)
	}

	return coflows
}
