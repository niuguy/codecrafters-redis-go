package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
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

// runEventLoop serializes command execution and owns the shared Redis state.
func runEventLoop(events <-chan commandEvent) {
	store := make(map[string][]byte)

	for event := range events {
		event.response <- execute(event.command, store)
	}
}

func execute(command []byte, store map[string][]byte) []byte {
	arguments, err := parseRESPCommand(command)
	if err != nil || len(arguments) == 0 {
		return []byte("-ERR protocol error\r\n")
	}

	switch {
	case bytes.EqualFold(arguments[0], []byte("PING")) && len(arguments) == 1:
		return pong
	case bytes.EqualFold(arguments[0], []byte("ECHO")) && len(arguments) == 2:
		return bulkString(arguments[1])
	case bytes.EqualFold(arguments[0], []byte("ECHO")):
		return []byte("-ERR wrong number of arguments for 'echo' command\r\n")
	case bytes.EqualFold(arguments[0], []byte("SET")):
		if len(arguments) != 3 {
			return []byte("-ERR wrong number of arguments for 'set' command\r\n")
		}
		store[string(arguments[1])] = append([]byte(nil), arguments[2]...)
		return []byte("+OK\r\n")
	case bytes.EqualFold(arguments[0], []byte("GET")):
		if len(arguments) != 2 {
			return []byte("-ERR wrong number of arguments for 'get' command\r\n")
		}
		value, ok := store[string(arguments[1])]
		if !ok {
			return []byte("$-1\r\n")
		}
		return bulkString(value)

	default:
		return []byte("-ERR unknown command\r\n")
	}
}

func bulkString(value []byte) []byte {
	response := make([]byte, 0, len(value)+32)
	response = append(response, '$')
	response = strconv.AppendInt(response, int64(len(value)), 10)
	response = append(response, '\r', '\n')
	response = append(response, value...)
	response = append(response, '\r', '\n')
	return response
}

func parseRESPCommand(command []byte) ([][]byte, error) {
	if len(command) < 4 || command[0] != '*' {
		return nil, errors.New("command is not a RESP array")
	}

	lineEnd := bytes.Index(command, []byte("\r\n"))
	if lineEnd == -1 {
		return nil, errors.New("incomplete RESP array header")
	}
	count, err := strconv.Atoi(string(command[1:lineEnd]))
	if err != nil || count < 1 {
		return nil, errors.New("invalid RESP array length")
	}

	arguments := make([][]byte, 0, count)
	position := lineEnd + 2
	for range count {
		if position >= len(command) || command[position] != '$' {
			return nil, errors.New("command argument is not a RESP bulk string")
		}

		argumentEnd := bytes.Index(command[position:], []byte("\r\n"))
		if argumentEnd == -1 {
			return nil, errors.New("incomplete RESP bulk string header")
		}
		argumentEnd += position

		length, err := strconv.Atoi(string(command[position+1 : argumentEnd]))
		if err != nil || length < 0 {
			return nil, errors.New("invalid RESP bulk string length")
		}
		position = argumentEnd + 2
		if position+length+2 > len(command) || !bytes.Equal(command[position+length:position+length+2], []byte("\r\n")) {
			return nil, errors.New("incomplete RESP bulk string")
		}

		arguments = append(arguments, command[position:position+length])
		position += length + 2
	}

	return arguments, nil
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
