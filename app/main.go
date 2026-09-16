package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
)

var pong = []byte("+PONG\r\n")

type commandEvent struct {
	command  []byte
	response chan []byte
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		log.Fatal("Failed to bind to port 6379: ", err)
	}
	defer listener.Close()

	events := make(chan commandEvent)
	go runEventLoop(events)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err)
			continue
		}
		go handleConn(conn, events)
	}
}

// runEventLoop serializes command execution. This is where shared Redis state
// can be added when commands such as SET and GET are implemented.
func runEventLoop(events <-chan commandEvent) {
	for event := range events {
		event.response <- execute(event.command)
	}
}

func execute(_ []byte) []byte {
	return pong
}

func handleConn(conn net.Conn, events chan<- commandEvent) {
	defer conn.Close()

	response := make(chan []byte)
	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Println("Error reading: ", err)
			}
			return
		}

		events <- commandEvent{
			command:  buf[:n],
			response: response,
		}

		_, err = conn.Write(<-response)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Println("Error writing: ", err)
			}
			return
		}
	}
}
