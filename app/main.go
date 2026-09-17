package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"slices"
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

func isCommand(value []byte, name string) bool {
	return bytes.EqualFold(value, []byte(name))
}

func execute(command []byte, store map[string]redisValue) []byte {
	arguments, err := parseRESPCommand(command)
	if err != nil || len(arguments) == 0 {
		return []byte("-ERR protocol error\r\n")
	}

	switch {
	case isCommand(arguments[0], "PING"):
		return executePing(arguments)
	case isCommand(arguments[0], "ECHO"):
		return executeEcho(arguments)
	case isCommand(arguments[0], "SET"):
		return executeSet(arguments, store)
	case isCommand(arguments[0], "GET"):
		return executeGet(arguments, store)
	case isCommand(arguments[0], "RPUSH"):
		return executeRPush(arguments, store)
	case isCommand(arguments[0], "LRANGE"):
		return executeLRange(arguments, store)
	case isCommand(arguments[0], "LPUSH"):
		return executeLPush(arguments, store)
	case isCommand(arguments[0], "LLEN"):
		return executeLLen(arguments, store)
	default:
		return []byte("-ERR unknown command\r\n")
	}
}

func executePing(arguments [][]byte) []byte {
	if len(arguments) != 1 {
		return []byte("-ERR wrong number of arguments for 'ping' command\r\n")
	}
	return pong
}

func executeEcho(arguments [][]byte) []byte {
	if len(arguments) != 2 {
		return []byte("-ERR wrong number of arguments for 'echo' command\r\n")
	}
	return bulkString(arguments[1])
}

func executeSet(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) < 3 {
		return []byte("-ERR wrong number of arguments for 'set' command\r\n")
	}
	if len(arguments) > 4 {
		var sleepTime time.Duration
		if isCommand(arguments[3], "EX") {
			sleepTimeNumber, _ := strconv.Atoi(string(arguments[4]))
			sleepTime = time.Duration(sleepTimeNumber) * time.Second
		} else if isCommand(arguments[3], "PX") {
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
		string: cloneBytes(arguments[2]),
	}
	return []byte("+OK\r\n")
}

func executeGet(arguments [][]byte, store map[string]redisValue) []byte {
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
}

func executeRPush(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) < 3 {
		return []byte("-ERR wrong number of arguments for 'rpush' command\r\n")
	}

	key := string(arguments[1])
	value, ok := store[key]
	if ok && value.kind != listKind {
		return wrongTypeError()
	}
	if !ok {
		value.kind = listKind
	}
	for _, element := range arguments[2:] {
		value.list = append(value.list, cloneBytes(element))
	}
	store[key] = value
	return integerResponse(len(value.list))
}

func executeLRange(arguments [][]byte, store map[string]redisValue) []byte {
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
}

func executeLPush(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) < 3 {
		return []byte("-ERR wrong number of arguments for 'lpush' command\r\n")
	}
	key := string(arguments[1])
	value, ok := store[key]
	if ok && value.kind != listKind {
		return wrongTypeError()
	}
	if !ok {
		value.kind = listKind
	}
	value.list = prependList(value.list, arguments[2:])
	store[key] = value
	return integerResponse(len(value.list))
}

func executeLLen(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) != 2 {
		return []byte("-ERR wrong number of arguments for 'llen' command\r\n")
	}
	value, ok := store[string(arguments[1])]
	if !ok {
		return integerResponse(0)
	}
	if value.kind != listKind {
		return wrongTypeError()
	}
	return integerResponse(len(value.list))
}

func integerResponse(value int) []byte {
	response := make([]byte, 0, 24)
	response = append(response, ':')
	response = strconv.AppendInt(response, int64(value), 10)
	response = append(response, '\r', '\n')
	return response
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}

func prependList(existing, values [][]byte) [][]byte {
	result := make([][]byte, 0, len(existing)+len(values))
	for _, value := range slices.Backward(values) {
		result = append(result, cloneBytes(value))
	}
	return append(result, existing...)
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
