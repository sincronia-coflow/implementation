package main

import (
	"fmt"
	"github.com/akshayknarayan/sincronia/app/parser"
	"github.com/akshayknarayan/sincronia/client"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

var nodesSync = map[uint32]string{
	0:  "10.1.1.2:17001", //nsl003
	1:  "10.1.1.3:17001", //nsl004
	2:  "10.1.2.2:17001", //nsl005
	3:  "10.1.2.3:17001", //nsl006
	4:  "10.2.1.2:17001", //nsl007
	5:  "10.2.1.3:17001", //nsl008
	6:  "10.2.2.2:17001", //nsl009
	7:  "10.2.2.3:17001", //nsl010
	8:  "10.3.1.2:17001", //nsl011
	9:  "10.3.1.3:17001", //nsl012
	10: "10.3.2.2:17001", //nsl013
	11: "10.3.2.3:17001", //nsl014
	12: "10.4.1.2:17001", //nsl015
	13: "10.4.1.3:17001", //nsl016
	14: "10.4.2.2:17001", //nsl017
	15: "10.4.2.3:17001", //nsl018
}

var clientDone = make(chan interface{})
var serverDone = make(chan interface{})

func createServer() {
	nodeID, _ := strconv.Atoi(os.Args[2])
	ip := nodesSync[uint32(nodeID)]
	ln, err := net.Listen("tcp", ip)
	if err != nil {
		fmt.Println("Server creationg error")
	}
	count := 0
	for {
		_, err := ln.Accept()
		if err != nil {
			fmt.Println("Server connection error")
		} else {
			count = count + 1
			fmt.Println(count, " clients connected")
			if count == len(nodesSync) {
				break
			}
		}
		// go handleConnection(conn)
	}
	ln.Close()
	serverDone <- struct{}{}
}

func connect(ip string, done chan interface{}) {
	for {
		_, err := net.DialTimeout("tcp", ip, 5*time.Millisecond)
		if err != nil {
			continue
		} else {
			fmt.Println("connected to ", ip)
			clientDone <- struct{}{}
			break
		}
	}

}

func clientfunc(
	schedAddr string,
	id uint32,
	nodes map[uint32]string,
	cfs map[uint32][]client.Flow,
	done chan interface{},
	releaseDates map[uint32]int,
) {
	s, err := client.New(schedAddr, id, nodes)
	if err != nil {
		panic(err)
	}

	<-time.After(1 * time.Second)

	prevReleaseDate := 0
	cfsDone := make(chan interface{})
	coflowIds := []int{}
	for cf := range cfs {
		coflowIds = append(coflowIds, int(cf))
	}
	sort.Ints(coflowIds)
	for _, coflowID := range coflowIds {
		cf := uint32(coflowID)
		fs := cfs[cf]

		timeToWait := releaseDates[cf] - prevReleaseDate
		prevReleaseDate = releaseDates[cf]
		<-time.After(time.Duration(timeToWait) * time.Millisecond)

		rs, err := s.SendCoflow(cf, fs)
		if err != nil {
			panic(err)
		}

		go func(cf uint32, rcvs chan client.Data) {
		recv:
			for {
				select {
				case r, ok := <-rcvs:
					if !ok {
						break recv
					} else {
						log.WithFields(log.Fields{
							"node":   id,
							"dataid": r.DataID,
						}).Info("flow done")
					}
				case <-time.After(5 * time.Second):
					log.WithFields(log.Fields{
						"node": id,
						"job":  cf,
					}).Info("still listening")
				}
			}

			log.WithFields(log.Fields{
				"node": id,
				"cf":   cf,
			}).Info("done receiving")
			cfsDone <- struct{}{}
		}(cf, rs)

	}

	for _ = range cfs {
		<-cfsDone
	}

	log.WithFields(log.Fields{
		"node": id,
	}).Info("client done")
	done <- struct{}{}
}

func clientfunc2(
	schedAddr string,
	id uint32,
	nodes map[uint32]string,
	cfs map[uint32][]client.Flow,
	done chan interface{},
) {
	s, err := client.New(schedAddr, id, nodes)
	if err != nil {
		panic(err)
	}

	<-time.After(10 * time.Second)

	cfsDone := make(chan interface{})
	for cf, fs := range cfs {
		fmt.Println("client: ", id, " cf: ", cf, " fs: ", fs)
		rs, err := s.SendCoflow(cf, fs)
		if err != nil {
			panic(err)
		}

		go func(cf uint32, rcvs chan client.Data) {
		recv:
			for {
				select {
				case r, ok := <-rcvs:
					if !ok {
						break recv
					} else {
						log.WithFields(log.Fields{
							"node":   id,
							"dataid": r.DataID,
						}).Info("flow done")
					}
				case <-time.After(5 * time.Second):
					log.WithFields(log.Fields{
						"node": id,
						"job":  cf,
					}).Info("still listening")
				}
			}

			log.WithFields(log.Fields{
				"node": id,
				"cf":   cf,
			}).Info("done receiving")
			cfsDone <- struct{}{}
		}(cf, rs)
	}

	for _ = range cfs {
		<-cfsDone
	}

	log.WithFields(log.Fields{
		"node": id,
	}).Info("client done")
	done <- struct{}{}
}

func main() {
	nodes := map[uint32]string{
		0:  "10.1.1.2:17001", //nsl003
		1:  "10.1.1.3:17001", //nsl004
		2:  "10.1.2.2:17001", //nsl005
		3:  "10.1.2.3:17001", //nsl006
		4:  "10.2.1.2:17001", //nsl007
		5:  "10.2.1.3:17001", //nsl008
		6:  "10.2.2.2:17001", //nsl009
		7:  "10.2.2.3:17001", //nsl010
		8:  "10.3.1.2:17001", //nsl011
		9:  "10.3.1.3:17001", //nsl012
		10: "10.3.2.2:17001", //nsl013
		11: "10.3.2.3:17001", //nsl014
		12: "10.4.1.2:17001", //nsl015
		13: "10.4.1.3:17001", //nsl016
		14: "10.4.2.2:17001", //nsl017
		15: "10.4.2.3:17001", //nsl018
	}

	schedAddr := "10.1.1.3:16424" //nsl004

	coflows := parser.GenerateCoflowsFromTracefileMaster(os.Args[1])

	inputMapFile := "node" + os.Args[2] + "-trace.txt"

	flowMap := parser.GenerateMapFromTracefile(inputMapFile, len(coflows))
	fmt.Println("flow map generated")
	fmt.Println(flowMap)

	done := make(chan interface{})
	csl := map[uint32][]client.Flow{}
	for i := 0; i < len(coflows); i++ {
		csl[uint32(i)] = []client.Flow{}
	}

	releaseDates := map[uint32]int{}
	for i := 0; i < len(coflows); i++ {
		flowNumbers := flowMap[coflows[i].JobID]
		releaseDate := flowNumbers[0]
		releaseDates[coflows[i].JobID] = releaseDate

		// csl[0] = []Flow{}
		for j := 1; j < len(flowNumbers); j++ {
			csl[uint32(coflows[i].JobID)] = append(csl[uint32(coflows[i].JobID)], coflows[i].Flows[flowNumbers[j]])
		}

	}
	nodeNum, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}

	coflowIds := []int{}
	for cf := range csl {
		coflowIds = append(coflowIds, int(cf))
	}

	sort.Ints(coflowIds)
	fmt.Println(coflowIds)
	go createServer()

	for i := range nodesSync {
		go connect(nodesSync[uint32(i)], clientDone)
	}

	for _ = range nodesSync {
		<-clientDone
	}

	<-serverDone

	fmt.Println("Clients synchronized at time ", time.Now())
	<-time.After(1 * time.Second)
	clientfunc(schedAddr, uint32(nodeNum), nodes, csl, done, releaseDates)
	<-done
	<-done

}
