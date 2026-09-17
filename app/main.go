package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"time"
)

var pong = []byte("+PONG\r\n")

type valueKind uint8

const (
	stringKind valueKind = iota
	listKind
)

type redisValue struct {
	kind   valueKind
	string []byte
	list   [][]byte
}

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
	store := make(map[string]redisValue)

	for event := range events {
		event.response <- execute(event.command, store)
	}
}

func execute(command []byte, store map[string]redisValue) []byte {
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
		if len(arguments) < 3 {
			return []byte("-ERR wrong number of arguments for 'set' command\r\n")
		}
		if len(arguments) > 4 {
			var sleepTime time.Duration
			if bytes.EqualFold(arguments[3], []byte("EX")) {
				sleepTimeNumber, _ := strconv.Atoi(string(arguments[4]))
				sleepTime = time.Duration(sleepTimeNumber) * time.Second
			} else if bytes.EqualFold(arguments[3], []byte("PX")) {
				sleepTimeNumber, _ := strconv.Atoi(string(arguments[4]))
				sleepTime = time.Duration(sleepTimeNumber) * time.Millisecond

			}

			go func() {
				time.Sleep(sleepTime)
				delete(store, string(arguments[1]))
			}()
		}

		store[string(arguments[1])] = redisValue{
			kind:   stringKind,
			string: append([]byte(nil), arguments[2]...),
		}
		return []byte("+OK\r\n")
	case bytes.EqualFold(arguments[0], []byte("GET")):
		if len(arguments) != 2 {
			return []byte("-ERR wrong number of arguments for 'get' command\r\n")
		}
		value, ok := store[string(arguments[1])]
		if !ok {
			return []byte("$-1\r\n")
		}
		if value.kind != stringKind {
			return wrongTypeError()
		}
		return bulkString(value.string)
	case bytes.EqualFold(arguments[0], []byte("RPUSH")):
		if len(arguments) < 3 {
			return []byte("-ERR wrong number of arguments for 'rpush' command\r\n")
		}

		key := string(arguments[1])
		value, ok := store[key]
		if ok && value.kind != listKind {
			return wrongTypeError()
		}
		if !ok {
			// A missing key starts as an empty list, then receives the elements.
			value.kind = listKind
		}
		for _, element := range arguments[2:] {
			value.list = append(value.list, append([]byte(nil), element...))
		}
		store[key] = value
		return integerResponse(len(value.list))
	case bytes.EqualFold(arguments[0], []byte("LRANGE")):
		if len(arguments) != 4 {
			return []byte("-ERR wrong number of arguments for 'lrange' command\r\n")
		}

		start, err := strconv.Atoi(string(arguments[2]))
		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}
		stop, err := strconv.Atoi(string(arguments[3]))
		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}

		value, ok := store[string(arguments[1])]
		if !ok {
			return arrayResponse(nil)
		}
		if value.kind != listKind {
			return wrongTypeError()
		}

		start, stop, ok = listRange(start, stop, len(value.list))
		if !ok {
			return arrayResponse(nil)
		}
		return arrayResponse(value.list[start : stop+1])

	default:
		return []byte("-ERR unknown command\r\n")
	}
}

func integerResponse(value int) []byte {
	response := make([]byte, 0, 24)
	response = append(response, ':')
	response = strconv.AppendInt(response, int64(value), 10)
	response = append(response, '\r', '\n')
	return response
}

func arrayResponse(values [][]byte) []byte {
	response := make([]byte, 0, 32)
	response = append(response, '*')
	response = strconv.AppendInt(response, int64(len(values)), 10)
	response = append(response, '\r', '\n')
	for _, value := range values {
		response = append(response, bulkString(value)...)
	}
	return response
}

func listRange(start, stop, length int) (int, int, bool) {
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop < 0 || start >= length || start > stop {
		return 0, 0, false
	}
	if stop >= length {
		stop = length - 1
	}
	return start, stop, true
}

func wrongTypeError() []byte {
	return []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
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
