package main

import (
	"github.com/akshayknarayan/sincronia/app/parser"
	"github.com/akshayknarayan/sincronia/client"
	"os"

	log "github.com/sirupsen/logrus"
)

func main() {
	log.Debug("Ensure that the scheduler is running prior to running appmaster")

	// define clients
	// nodeID 16 = app master
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

	schedAddr := "127.0.0.1:16424"

	s, err := client.New(schedAddr, 16, nodes)
	if err != nil {
		panic(err)
	}

	coflows := parser.GenerateCoflowsFromTracefileMaster(os.Args[1])
	log.Info("registered coflows")
	s.RegCoflow(coflows)
	log.Info("registration done")
	log.Info("appmaster() done")
}
