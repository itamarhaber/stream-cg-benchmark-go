package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/mediocregopher/radix/v3"
)

var totalMessagesRead uint64
var totalMessagesAcked uint64
var readLatencies *hdrhistogram.Histogram
var ackLatencies *hdrhistogram.Histogram

type testResult struct {
	StartTime     int64     `json:"StartTime"`
	Duration      float64   `json:"Duration"`
	MessageRate   float64   `json:"MessageRate"`
	TotalMessages uint64    `json:"TotalMessages"`
	MessageRateTs []float64 `json:"MessageRateTs"`
}

func main() {
	host := flag.String("host", "127.0.0.1", "Redis host.")
	port := flag.Int("port", 6379, "Redis port.")
	key_name := flag.String("key-name", "mystream", "Key name of the stream.")
	message_count := flag.Uint64("message-count", 0, "Number of messages to process (0 means all).")
	group_name := flag.String("group-name", "group", "Name of consumer group.")
	group_consumer_prefix := flag.String("group-consumer-prefix", "consumer-", "Prefix for consumer name.")
	group_consumer_max_pending := flag.Int("group-consumer-max-pending", 1, "Maximum number of pending messages before acking.")
	group_consumers_count := flag.Int("group-consumers-count", 1, "Number of consumers in group.")
	json_out_file := flag.String("json-out-file", "", "Name of json output file, if not set, will not print to json.")
	client_update_tick := flag.Int("client-update-tick", 1, "Client update tick.")
	flag.Parse()

	totalMessagesRead = 0
	totalMessagesAcked = 0
	readLatencies = hdrhistogram.New(1, 90000000, 3)
	ackLatencies = hdrhistogram.New(1, 90000000, 3)

	stopChan := make(chan struct{})
	connectionStr := fmt.Sprintf("%s:%d", *host, *port)

	conn, err := radix.Dial("tcp", connectionStr)
	if err != nil {
		log.Fatal(err)
	}

	// Get length of stream in case message_count is 0.
	if *message_count == 0 {
		err = conn.Do(radix.Cmd(message_count, "XLEN", *key_name))
		if err != nil {
			log.Fatal(err)
		}
	}

	// Delete the group
	destroyCmdArgs := []string{"DESTROY", *key_name, *group_name}
	err = conn.Do(radix.Cmd(nil, "XGROUP", destroyCmdArgs...))
	if err != nil {
		log.Fatal(err)
	}

	// Create the group
	createCmdArgs := []string{"CREATE", *key_name, *group_name, "0"}
	err = conn.Do(radix.Cmd(nil, "XGROUP", createCmdArgs...))
	if err != nil {
		log.Fatal(err)
	}

	// a WaitGroup for the goroutines to tell us they've stopped
	wg := sync.WaitGroup{}

	for consumer_id := 1; consumer_id <= *group_consumers_count; consumer_id++ {
		consumerName := fmt.Sprintf("%s%d", *group_consumer_prefix, consumer_id)
		wg.Add(1)
		go groupConsumerRoutine(connectionStr, *group_name, consumerName, *key_name, *group_consumer_max_pending, stopChan, &wg)
	}

	// listen for C-c
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	w := new(tabwriter.Writer)

	tick := time.NewTicker(time.Duration(*client_update_tick) * time.Second)
	closed, start_time, duration, totalMessages, messageRateTs := updateCLI(tick, c, w, *message_count)
	messageRate := float64(totalMessages) / float64(duration.Seconds())

	fmt.Fprint(w, fmt.Sprintf("#################################################\nTotal Duration %f Seconds\nMessage Rate %f\n#################################################\n", duration.Seconds(), messageRate))
	fmt.Fprint(w, "\r\n")
	w.Flush()

	printLatencySummary(w, "XREADGROUP", readLatencies)
	printLatencySummary(w, "XACK", ackLatencies)

	if strings.Compare(*json_out_file, "") != 0 {

		res := testResult{
			StartTime:     start_time.Unix(),
			Duration:      duration.Seconds(),
			MessageRate:   messageRate,
			TotalMessages: totalMessages,
			MessageRateTs: messageRateTs,
		}
		file, err := json.MarshalIndent(res, "", " ")
		if err != nil {
			log.Fatal(err)
		}

		err = ioutil.WriteFile(*json_out_file, file, 0644)
		if err != nil {
			log.Fatal(err)
		}
	}

	if closed {
		return
	}

	// tell the goroutine to stop
	close(stopChan)
	// and wait for them both to reply back
	wg.Wait()
}

func updateCLI(tick *time.Ticker, c chan os.Signal, w *tabwriter.Writer, count uint64) (bool, time.Time, time.Duration, uint64, []float64) {

	start := time.Now()
	prevTime := time.Now()
	prevMessageCount := uint64(0)
	messageRateTs := []float64{}

	w.Init(os.Stdout, 25, 0, 1, ' ', tabwriter.AlignRight)
	fmt.Fprint(w, fmt.Sprintf("Test Time\tTotal Messages\t Message Rate \t"))
	fmt.Fprint(w, "\n")
	w.Flush()
	for {
		select {
		case <-tick.C:
			{
				now := time.Now()
				took := now.Sub(prevTime)
				messageRate := float64(totalMessagesRead-prevMessageCount) / float64(took.Seconds())
				if prevMessageCount == 0 && totalMessagesRead != 0 {
					start = time.Now()
				}
				if totalMessagesRead != 0 {
					messageRateTs = append(messageRateTs, messageRate)
				}
				prevMessageCount = totalMessagesRead
				prevTime = now

				fmt.Fprint(w, fmt.Sprintf("%.0f\t%d\t%.2f\t", time.Since(start).Seconds(), totalMessagesRead, messageRate))
				fmt.Fprint(w, "\r\n")
				w.Flush()

				if totalMessagesAcked == count {
					return false, start, time.Since(start), totalMessagesRead, messageRateTs
				}
				break
			}

		case <-c:
			fmt.Println("received Ctrl-c - shutting down")
			return true, start, time.Since(start), totalMessagesRead, messageRateTs
		}
	}
	return false, start, time.Since(start), totalMessagesRead, messageRateTs
}

func printLatencySummary(w *tabwriter.Writer, n string, h *hdrhistogram.Histogram) {
	p50Ms := float64(h.ValueAtQuantile(50.0)) / 1000.0
	p95Ms := float64(h.ValueAtQuantile(95.0)) / 1000.0
	p99Ms := float64(h.ValueAtQuantile(99.0)) / 1000.0
	fmt.Fprintf(w, "%s latency summary (msec):\n", n)
	fmt.Fprintf(w, "    %9s %9s %9s\n", "p50", "p95", "p99")
	fmt.Fprintf(w, "    %9.3f %9.3f %9.3f\n", p50Ms, p95Ms, p99Ms)
}
