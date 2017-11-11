package client

import (
	"bytes"
	"net"

	log "github.com/sirupsen/logrus"
)

const uint16 BaseRecvPort = 17000

// Coflow is a client's view of a coflow
type Coflow struct {
	jobID uint32
	send  []SendFlow
	recv  []RecvFlow
	prio  uint8
	done  chan RecvFlow // return completed RecvFlows to RegCoflow caller over this channel
}

// Sinchronia manages interactions with the coflow scheduler
type Sinchronia struct {
	schedConn     *net.Conn
	activeCoflows map[uint32]Coflow // jobID -> un-sent coflows
	regCoflow     chan Coflow
}

func (s *Sinchronia) coflowState() {
	sendCh := make(chan Coflow)
	gotCoflowSchedule := make(chan Coflow)
	go serialize(s.conn, sendCh)
	go deserialize(s.conn, gotCoflowSchedule)
	for {
		select {
		case reg := <-s.regCoflow:
			s.activeCoflows[jobID] = cf
			// serialize and send coflow information
			sendCh <- reg
		case cof := <-gotCoflowSchedule:
			// merge with known inflight coflows
			cf, ok := s.activeCoflows[cfp.jobID]
			if ok {
				cf.prio = cfp.prio
				cf.recv = cfp.recv
				delete(s.activeCoflows, cfp.jobID)
			} else {
				log.WithFields(log.Fields{
					"jobId": cfp.jobID,
					"prio":  cfp.prio,
				}).Warn("unknown coflow in received schedule")
				continue
			}

			// receive from given addresses
			for f := range cf.recv {
				go f.recv(cf.done)
			}

			// and signal flows to send with appropriate priority
			for f := range cf.send {
				f.sendNow <- cf.prio
			}
		}
	}
}

// New returns a Sinchronia agent connected to the central scheduler
// schedAddr: central scheduler address
func New(schedAddr string) (Sinchronia, error) {
	conn, err = net.Dial("tcp4", schedAddr)
	if err != nil {
		return Sinchronia{}, fmt.Errorf("couldn't connect to coflow scheduler: %s", err)
	}

	regCoflow = make(chan Coflow)
	s = Sinchronia{
		schedConn: conn,
		regCoflow: regCoflow,
	}

	go s.coflowState()
	return s, nil
}

// RegCoflow registers a coflow with Sinchronia
// the coflow metadata is sent to the scheduler to indicate that it's ready to be sent.
// When the scheduler returns a coflow priority, the flow is automatically sent over the network.
func (s Sinchronia) RegCoflow(jobID uint32, flows []SendFlow) (chan RecvFlow, error) {
	if c, ok := s.activeCoflows[jobID]; ok {
		return nil, fmt.Errorf("already registered coflow: %v", c)
	}

	// make a channel for each flow
	// and have it wait to send
	for f := range flows {
		f.sendNow = make(chan uint32)
		go f.send()
	}

	cf := Coflow{
		jobID: jobID,
		send:  flows,
		recv:  []uint32{}, // unused in send phase
		prio:  0,          // unused in send phase
		done:  make(chan RecvFlow, 10),
	}

	s.regCoflow <- cf
	return s.done, nil
}
