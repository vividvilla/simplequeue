// Command simplequeue is a CLI tool for interacting with a simplequeue broker.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/vivek-ng/simplequeue/client"
)

func main() {
	brokerAddr := flag.String("broker", "http://localhost:8080", "broker address")
	topic := flag.String("topic", "default", "topic name")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
	}

	c := client.New(*brokerAddr, client.WithTopic(*topic))
	ctx := context.Background()

	switch args[0] {
	case "enqueue":
		// enqueue [--id <id>] <payload-json>
		var id, payload string
		switch {
		case len(args) >= 4 && args[1] == "--id":
			id = args[2]
			payload = args[3]
		case len(args) >= 2:
			payload = args[1]
		default:
			fmt.Fprintln(os.Stderr, "usage: simplequeue enqueue [--id <id>] <payload-json>")
			os.Exit(1)
		}
		assignedID, err := c.Enqueue(ctx, id, json.RawMessage(payload))
		if err != nil {
			fatal(err)
		}
		fmt.Printf("enqueued: %s\n", assignedID)

	case "claim":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "usage: simplequeue claim <worker-id>")
			os.Exit(1)
		}
		job, err := c.Claim(ctx, args[1])
		if err != nil {
			fatal(err)
		}
		if job == nil {
			fmt.Println("no pending jobs")
			return
		}
		data, _ := json.MarshalIndent(job, "", "  ")
		fmt.Println(string(data))

	case "ack":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "usage: simplequeue ack <job-id>")
			os.Exit(1)
		}
		if err := c.Ack(ctx, args[1]); err != nil {
			fatal(err)
		}
		fmt.Println("acked")

	case "nack":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "usage: simplequeue nack <job-id>")
			os.Exit(1)
		}
		if err := c.Nack(ctx, args[1]); err != nil {
			fatal(err)
		}
		fmt.Println("nacked")

	case "heartbeat":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "usage: simplequeue heartbeat <job-id>")
			os.Exit(1)
		}
		if err := c.Heartbeat(ctx, args[1]); err != nil {
			fatal(err)
		}
		fmt.Println("heartbeat sent")

	case "status":
		s, err := c.Status(ctx)
		if err != nil {
			fatal(err)
		}
		data, _ := json.MarshalIndent(s, "", "  ")
		fmt.Println(string(data))

	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: simplequeue [-broker addr] [-topic name] <command> [args...]")
	fmt.Fprintln(os.Stderr, "commands: enqueue, claim, ack, nack, heartbeat, status")
	os.Exit(1)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
