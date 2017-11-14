package client

import (
	"net"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/ipv4"
)

// SendFlow is an outgoing flow
type SendFlow struct {
	DataID uint32   // which piece of data this is (flow id)
	To     net.Addr // flow destination
	Data   []uint8  // data to send

	sendNow chan uint32 // get priority to send with
}

// https://en.wikipedia.org/wiki/Differentiated_services
/*
DSCP_value  Hex_value  Decimal_value  Meaning                    Drop_probability  Equivalent_IP_precedence_value
101_110     0x2e       46             Expedited_forwarding_(EF)  N/A               101-Critical
000_000     0x00       0              Best_effort                N/A               000-Routine
001_010     0x0a       10             AF11                       Low               001-Priority
001_100     0x0c       12             AF12                       Medium            001-Priority
001_110     0x0e       14             AF13                       High              001-Priority
010_010     0x12       18             AF21                       Low               010-Immediate
010_100     0x14       20             AF22                       Medium            010-Immediate
010_110     0x16       22             AF23                       High              010-Immediate
011_010     0x1a       26             AF31                       Low               011-Flash
011_100     0x1c       28             AF32                       Medium            011-Flash
011_110     0x1e       30             AF33                       High              011-Flash
100_010     0x22       34             AF41                       Low               100-Flash_override
100_100     0x24       36             AF42                       Medium            100-Flash_override
100_110     0x26       38             AF43                       High              100-Flash_override
*/
func diffServFromPriority(prio uint8) int {
	switch prio {
	case 0:
		return 0x00
	case 1:
		return 0x0e
	case 2:
		return 0x0c
	case 3:
		return 0x0a
	case 4:
		return 0x16
	case 5:
		return 0x14
	case 6:
		return 0x12
	case 7:
		return 0x1e
	}
}

func (f SendFlow) send() {
	prio := <-f.sendNow
	prio := diffServFromPriority(prio)

	conn, err = net.Dial("tcp4", f.To)
	if err != nil {
		log.WithFields(log.Fields{
			"DataID":   f.DataId,
			"To":       f.To,
			"Priority": prio,
		}).Error("Dial", err)
		return
	}

	// set DiffServ bits
	if err := ipv4.NewConn(conn).SetTOS(prio); err != nil {
		log.WithFields(log.Fields{
			"DataID":   f.DataId,
			"To":       f.To,
			"Priority": prio,
		}).Error("setting DiffServ", err)
	}

	written, err := conn.Write(f.Data)
	if err != nil {
		log.WithFields(log.Fields{
			"DataID":   f.DataId,
			"To":       f.To,
			"Priority": prio,
		}).Error("Conn write", err)
		return
	}

	log.WithFields(log.Fields{
		"DataID":   f.DataId,
		"To":       f.To,
		"Wrote":    written,
		"Priority": prio,
	}).Info("sent")
}
