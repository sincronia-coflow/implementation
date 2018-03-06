package client

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/akshayknarayan/sincronia/client/scheduler"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/ipv4"
	"zombiezen.com/go/capnproto2/rpc"
)

type sendFlow struct {
	f    Flow
	done chan interface{}
}

/*
https://en.wikipedia.org/wiki/Differentiated_services
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
		return 0x20
	case 2:
		return 0x40
	case 3:
		return 0x60
	case 4:
		return 0x80
	case 5:
		return 0xa0
	case 6:
		return 0xc0
	case 7:
		return 0xe0
	}

	panic("unreachable uint8")
}

func (f *sendFlow) send(
	ctx context.Context,
	job uint32,
	givenPrio uint32,
	nodeMap map[uint32]string,
) {
	prio := diffServFromPriority(uint8(givenPrio))

	toAddr, ok := nodeMap[f.f.To]
	if !ok {
		log.WithFields(log.Fields{
			"To":       f.f.To,
			"Priority": prio,
			"NodeMap":  nodeMap,
		}).Panic("Could not resolve address")
	}

dial:
	conn, err := net.Dial("tcp4", toAddr)
	if err != nil {
		log.WithFields(log.Fields{
			"DataID":   f.f.Info.DataID,
			"To":       f.f.To,
			"node":     f.f.From,
			"Priority": prio,
		}).Warn("Dial", err)
		<-time.After(time.Millisecond)
		goto dial
	}

	defer conn.Close()

	// set DiffServ bits
	if err := ipv4.NewConn(conn).SetTOS(prio); err != nil {
		log.WithFields(log.Fields{
			"DataID":   f.f.Info.DataID,
			"To":       f.f.To,
			"Priority": prio,
		}).Error("setting DiffServ", err)
	}

	rpcConn := rpc.NewConn(rpc.StreamTransport(conn))
	client := scheduler.Receiver{Client: rpcConn.Bootstrap(ctx)}

	client.Send(
		ctx,
		func(
			p scheduler.Receiver_send_Params,
		) error {
			d, err := p.NewData()
			if err != nil {
				log.WithFields(log.Fields{
					"DataID":   f.f.Info.DataID,
					"To":       f.f.To,
					"Priority": prio,
					"err":      err,
				}).Error("send - rpc send")
				return err
			}

			d.SetJobID(job)
			d.SetDataID(f.f.Info.DataID)
			d.SetFrom(f.f.From)
			d.SetTo(f.f.To)

			blob := make([]byte, flowChunkSizeBytes)
			n, err := f.f.Info.Blob.Read(blob)
			if err != nil && err != io.EOF {
				log.WithFields(log.Fields{
					"DataID": f.f.Info.DataID,
					"To":     f.f.To,
					"err":    err,
				}).Panic("send - read data")
				return err
			} else if uint32(n) != flowChunkSizeBytes && uint32(n) != f.f.Info.Size {
				log.WithFields(log.Fields{
					"DataID":          f.f.Info.DataID,
					"To":              f.f.To,
					"read":            n,
					"remaining size":  f.f.Info.Size,
					"flow chunk size": flowChunkSizeBytes,
				}).Panic("send - not enough Read")
				return fmt.Errorf("send - not enough Read")
			}

			if f.f.Info.Size > flowChunkSizeBytes {
				f.f.Info.Size -= flowChunkSizeBytes
			} else {
				blob = blob[:n]
				f.f.Info.Size = 0
			}

			log.WithFields(log.Fields{
				"DataID":         f.f.Info.DataID,
				"To":             f.f.To,
				"remaining size": f.f.Info.Size,
				"should_send":    n,
				"sending":        len(blob),
			}).Info("send")
			d.SetBlob(blob)
			return nil
		},
	).Struct()

	log.WithFields(log.Fields{
		"DataID":         f.f.Info.DataID,
		"To":             f.f.To,
		"Priority":       prio,
		"remaining size": f.f.Info.Size,
	}).Info("sent chunk")

	f.done <- struct{}{}
}
