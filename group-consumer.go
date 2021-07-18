package main

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mediocregopher/radix/v3"
)

func groupConsumerRoutine(addr string, groupName string, consumerName string, keyName string, pcount int, stop chan struct{}, wg *sync.WaitGroup) {
	// Tell the caller we've stopped
	defer wg.Done()

	conn, _ := bootstrapGroupConsumer(addr, consumerName)
	defer conn.Close()

	done := false
	ids := make([]radix.StreamEntryID, 0)
	readCmdArgs := []string{
		"GROUP", groupName, consumerName,
		"COUNT", "1",
		"STREAMS", keyName, ">",
	}
	var entries []radix.StreamEntries
	var startT, endT time.Time
	var duration time.Duration
	var id radix.StreamEntryID
	var err error

	for {
		select {
		case <-stop:
			return
		default:
			if len(ids) < pcount && !done {
				startT = time.Now()
				err = conn.Do(radix.Cmd(&entries, "XREADGROUP", readCmdArgs...))
				if err != nil {
					log.Fatal(err)
				}
				endT = time.Now()
				duration = endT.Sub(startT)
				err = readLatencies.RecordValue(duration.Microseconds())
				if err != nil {
					log.Fatalf("Received an error while recording latencies: %v", err)
				}
				if len(entries) == 0 {
					done = true
					continue
				}
				ids = append(ids, entries[0].Entries[0].ID)
				atomic.AddUint64(&totalMessagesRead, uint64(1))
			} else if len(ids) > 0 {
				id = ids[0]
				ids = ids[1:]
				startT = time.Now()
				err = conn.Do(radix.Cmd(nil, "XACK", keyName, groupName, id.String()))
				if err != nil {
					log.Fatal(err)
				}
				endT = time.Now()
				duration = endT.Sub(startT)
				err = ackLatencies.RecordValue(duration.Microseconds())
				if err != nil {
					log.Fatalf("Received an error while recording latencies: %v", err)
				}
				atomic.AddUint64(&totalMessagesAcked, uint64(1))
			} else {
				return
			}
		}
	}
}

func bootstrapGroupConsumer(addr string, consumerName string) (radix.Conn, error) {
	// Create a normal redis connection
	conn, err := radix.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	err = conn.Do(radix.FlatCmd(nil, "CLIENT", "SETNAME", consumerName))
	if err != nil {
		log.Fatal(err)
	}

	return conn, err
}
