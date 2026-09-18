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
	setKind
	zsetKind
	hashKind
	streamKind
	vectorSetKind
)

var valueKindNames = [...]string{
	"string",
	"list",
	"set",
	"zset",
	"hash",
	"stream",
	"vectorset",
}

func (kind valueKind) String() string {
	if int(kind) >= len(valueKindNames) {
		return "unknown"
	}
	return valueKindNames[kind]
}

type redisValue struct {
	kind   valueKind
	string []byte
	list   [][]byte
	stream []streamEntry
}

type streamEntry struct {
	id     streamID
	fields [][]byte
}

type streamID struct {
	milliseconds uint64
	sequence     uint64
}

type commandEvent struct {
	command       []byte
	response      chan []byte
	blocked       *blockedRequest
	blockedStream *blockedStreamRequest
}

type blockedRequest struct {
	event commandEvent
	keys  []string
	done  bool
	timer *time.Timer
}

type blockedStreamRequest struct {
	event commandEvent
	keys  []string
	ids   []streamID
	done  bool
	timer *time.Timer
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
func runEventLoop(events chan commandEvent) {
	store := make(map[string]redisValue)
	waiters := make(map[string][]*blockedRequest)
	streamWaiters := make(map[string][]*blockedStreamRequest)

	for event := range events {
		if event.blocked != nil {
			expireBlockedRequest(event.blocked, waiters)
			continue
		}
		if event.blockedStream != nil {
			expireBlockedStreamRequest(event.blockedStream, streamWaiters)
			continue
		}

		arguments, err := parseRESPCommand(event.command)
		if err == nil && len(arguments) > 0 && isCommand(arguments[0], "BLPOP") {
			handleBlockingPop(event, arguments, store, waiters, events)
			continue
		}
		if err == nil && len(arguments) > 0 && isBlockingXRead(arguments) {
			handleBlockingXRead(event, arguments, store, streamWaiters, events)
			continue
		}

		response := execute(event.command, store)
		event.response <- response
		if err == nil && isListPush(arguments) {
			wakeBlockedRequests(arguments[1], store, waiters)
		}
		if err == nil && len(arguments) > 1 && isCommand(arguments[0], "XADD") {
			wakeBlockedStreamRequests(arguments[1], store, streamWaiters)
		}
	}
}

func isCommand(value []byte, name string) bool {
	return bytes.EqualFold(value, []byte(name))
}

func isListPush(arguments [][]byte) bool {
	return len(arguments) > 1 &&
		(isCommand(arguments[0], "RPUSH") || isCommand(arguments[0], "LPUSH"))
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
	case isCommand(arguments[0], "LPOP"):
		return executeLPop(arguments, store)
	case isCommand(arguments[0], "LLEN"):
		return executeLLen(arguments, store)
	case isCommand(arguments[0], "TYPE"):
		return executeType(arguments, store)
	case isCommand(arguments[0], "XADD"):
		return executeXAdd(arguments, store)
	case isCommand(arguments[0], "XRANGE"):
		return executeXRange(arguments, store)
	case isCommand(arguments[0], "XREAD"):
		return executeXRead(arguments, store)
	default:
		return []byte("-ERR unknown command\r\n")
	}
}

func handleBlockingPop(event commandEvent, arguments [][]byte, store map[string]redisValue, waiters map[string][]*blockedRequest, events chan commandEvent) {
	if len(arguments) < 3 {
		event.response <- []byte("-ERR wrong number of arguments for 'blpop' command\r\n")
		return
	}

	timeout, err := strconv.ParseFloat(string(arguments[len(arguments)-1]), 64)
	if err != nil || timeout < 0 {
		event.response <- []byte("-ERR timeout is not a float or out of range\r\n")
		return
	}

	keys := make([]string, 0, len(arguments)-2)
	for _, argument := range arguments[1 : len(arguments)-1] {
		key := string(argument)
		keys = append(keys, key)

		value, ok := store[key]
		if !ok {
			continue
		}
		if value.kind != listKind {
			event.response <- wrongTypeError()
			return
		}
		if len(value.list) == 0 {
			delete(store, key)
			continue
		}

		element, _ := popLeft(store, key)
		event.response <- arrayResponse([][]byte{[]byte(key), element})
		return
	}

	waiter := &blockedRequest{
		event: event,
		keys:  keys,
	}
	for _, key := range keys {
		waiters[key] = append(waiters[key], waiter)
	}
	if timeout > 0 {
		duration := time.Duration(timeout * float64(time.Second))
		waiter.timer = time.AfterFunc(duration, func() {
			events <- commandEvent{blocked: waiter}
		})
	}
}

func isBlockingXRead(arguments [][]byte) bool {
	return len(arguments) >= 2 &&
		isCommand(arguments[0], "XREAD") &&
		isCommand(arguments[1], "BLOCK")
}

func handleBlockingXRead(event commandEvent, arguments [][]byte, store map[string]redisValue, waiters map[string][]*blockedStreamRequest, events chan commandEvent) {
	if len(arguments) < 6 || !isCommand(arguments[3], "STREAMS") || (len(arguments)-4)%2 != 0 {
		event.response <- []byte("-ERR syntax error\r\n")
		return
	}

	timeout, err := strconv.ParseInt(string(arguments[2]), 10, 64)
	if err != nil || timeout < 0 {
		event.response <- []byte("-ERR timeout is not an integer or out of range\r\n")
		return
	}

	streamCount := (len(arguments) - 4) / 2
	keys := make([]string, streamCount)
	ids := make([]streamID, streamCount)
	for i, argument := range arguments[4 : 4+streamCount] {
		keys[i] = string(argument)
		value, exists := store[keys[i]]
		if exists && value.kind != streamKind {
			event.response <- wrongTypeError()
			return
		}
		ids[i], err = readStartID(arguments[4+streamCount+i], value, exists)
		if err != nil {
			event.response <- []byte("-ERR invalid stream ID specified as stream command argument\r\n")
			return
		}
	}

	if results := collectStreamReadResults(keys, ids, store); len(results) > 0 {
		event.response <- streamReadResponse(results)
		return
	}

	waiter := &blockedStreamRequest{event: event, keys: keys, ids: ids}
	for _, key := range keys {
		waiters[key] = append(waiters[key], waiter)
	}
	if timeout > 0 {
		waiter.timer = time.AfterFunc(time.Duration(timeout)*time.Millisecond, func() {
			events <- commandEvent{blockedStream: waiter}
		})
	}
}

func collectStreamReadResults(keys []string, ids []streamID, store map[string]redisValue) []streamReadResult {
	results := make([]streamReadResult, 0, len(keys))
	for i, key := range keys {
		value, exists := store[key]
		if !exists {
			continue
		}

		entries := make([]streamEntry, 0)
		for _, entry := range value.stream {
			if entry.id.greaterThan(ids[i]) {
				entries = append(entries, entry)
			}
		}
		if len(entries) > 0 {
			results = append(results, streamReadResult{key: key, entries: entries})
		}
	}
	return results
}

func wakeBlockedStreamRequests(keyArgument []byte, store map[string]redisValue, waiters map[string][]*blockedStreamRequest) {
	key := string(keyArgument)
	for _, waiter := range append([]*blockedStreamRequest(nil), waiters[key]...) {
		if waiter.done {
			continue
		}
		results := collectStreamReadResults(waiter.keys, waiter.ids, store)
		if len(results) == 0 {
			continue
		}

		waiter.done = true
		if waiter.timer != nil {
			waiter.timer.Stop()
		}
		removeBlockedStreamRequest(waiter, waiters)
		waiter.event.response <- streamReadResponse(results)
	}
}

func expireBlockedStreamRequest(waiter *blockedStreamRequest, waiters map[string][]*blockedStreamRequest) {
	if waiter.done {
		return
	}
	waiter.done = true
	removeBlockedStreamRequest(waiter, waiters)
	waiter.event.response <- []byte("*-1\r\n")
}

func removeBlockedStreamRequest(waiter *blockedStreamRequest, waiters map[string][]*blockedStreamRequest) {
	for _, key := range waiter.keys {
		keyWaiters := waiters[key]
		remaining := keyWaiters[:0]
		for _, candidate := range keyWaiters {
			if candidate != waiter {
				remaining = append(remaining, candidate)
			}
		}
		if len(remaining) == 0 {
			delete(waiters, key)
		} else {
			waiters[key] = remaining
		}
	}
}

func popLeft(store map[string]redisValue, key string) ([]byte, bool) {
	value, ok := store[key]
	if !ok || value.kind != listKind || len(value.list) == 0 {
		return nil, false
	}

	element := value.list[0]
	value.list = value.list[1:]
	if len(value.list) == 0 {
		delete(store, key)
	} else {
		store[key] = value
	}
	return element, true
}

func wakeBlockedRequests(keyArgument []byte, store map[string]redisValue, waiters map[string][]*blockedRequest) {
	key := string(keyArgument)
	for {
		value, ok := store[key]
		if !ok || value.kind != listKind || len(value.list) == 0 {
			return
		}

		var waiter *blockedRequest
		for _, candidate := range waiters[key] {
			if !candidate.done {
				waiter = candidate
				break
			}
		}
		if waiter == nil {
			return
		}

		element, _ := popLeft(store, key)
		waiter.done = true
		if waiter.timer != nil {
			waiter.timer.Stop()
		}
		removeBlockedRequest(waiter, waiters)
		waiter.event.response <- arrayResponse([][]byte{[]byte(key), element})
	}
}

func expireBlockedRequest(waiter *blockedRequest, waiters map[string][]*blockedRequest) {
	if waiter.done {
		return
	}
	waiter.done = true
	removeBlockedRequest(waiter, waiters)
	waiter.event.response <- []byte("*-1\r\n")
}

func removeBlockedRequest(waiter *blockedRequest, waiters map[string][]*blockedRequest) {
	for _, key := range waiter.keys {
		keyWaiters := waiters[key]
		remaining := keyWaiters[:0]
		for _, candidate := range keyWaiters {
			if candidate != waiter {
				remaining = append(remaining, candidate)
			}
		}
		if len(remaining) == 0 {
			delete(waiters, key)
		} else {
			waiters[key] = remaining
		}
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

func executeLPop(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) != 2 && len(arguments) != 3 {
		return []byte("-ERR wrong number of arguments for 'lpop' command\r\n")
	}

	key := string(arguments[1])
	value, ok := store[key]
	if !ok {
		if len(arguments) == 3 {
			return []byte("*-1\r\n")
		}
		return []byte("$-1\r\n")
	}
	if value.kind != listKind {
		return wrongTypeError()
	}
	if len(value.list) == 0 {
		delete(store, key)
		if len(arguments) == 3 {
			return []byte("*-1\r\n")
		}
		return []byte("$-1\r\n")
	}

	if len(arguments) == 2 {
		element := value.list[0]
		value.list = value.list[1:]
		if len(value.list) == 0 {
			delete(store, key)
		} else {
			store[key] = value
		}
		return bulkString(element)
	}

	count, err := strconv.Atoi(string(arguments[2]))
	if err != nil || count < 0 {
		return []byte("-ERR value is not an integer or out of range\r\n")
	}
	if count > len(value.list) {
		count = len(value.list)
	}
	popped := value.list[:count]
	value.list = value.list[count:]
	if len(value.list) == 0 {
		delete(store, key)
	} else {
		store[key] = value
	}
	return arrayResponse(popped)
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

func executeType(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) != 2 {
		return []byte("-ERR wrong number of arguments for 'type' command\r\n")
	}
	value, ok := store[string(arguments[1])]
	if !ok {
		return simpleString("none")
	}
	return simpleString(value.kind.String())
}

func simpleString(value string) []byte {
	response := make([]byte, 0, len(value)+3)
	response = append(response, '+')
	response = append(response, value...)
	response = append(response, '\r', '\n')
	return response
}

func executeXAdd(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) < 5 || (len(arguments)-3)%2 != 0 {
		return []byte("-ERR wrong number of arguments for 'xadd' command\r\n")
	}

	key := string(arguments[1])
	value, ok := store[key]
	if ok && value.kind != streamKind {
		return wrongTypeError()
	}

	var lastID streamID
	if len(value.stream) > 0 {
		lastID = value.stream[len(value.stream)-1].id
	}
	id, err := nextStreamID(arguments[2], lastID)
	if err != nil {
		return []byte(err.Error() + "\r\n")
	}

	entry := streamEntry{id: id, fields: make([][]byte, 0, len(arguments)-3)}
	for _, field := range arguments[3:] {
		entry.fields = append(entry.fields, cloneBytes(field))
	}
	value.kind = streamKind
	value.stream = append(value.stream, entry)
	store[key] = value
	return bulkString([]byte(id.String()))
}

func nextStreamID(raw []byte, last streamID) (streamID, error) {
	if bytes.Equal(raw, []byte("*")) {
		return generatedStreamID(last), nil
	}

	dash := bytes.IndexByte(raw, '-')
	if dash <= 0 || dash == len(raw)-1 {
		return streamID{}, errors.New("-ERR Invalid stream ID specified as stream command argument")
	}
	milliseconds, err := strconv.ParseUint(string(raw[:dash]), 10, 64)
	if err != nil {
		return streamID{}, errors.New("-ERR Invalid stream ID specified as stream command argument")
	}

	sequencePart := raw[dash+1:]
	var sequence uint64
	if bytes.Equal(sequencePart, []byte("*")) {
		if milliseconds == last.milliseconds {
			sequence = last.sequence + 1
		}
	} else {
		sequence, err = strconv.ParseUint(string(sequencePart), 10, 64)
		if err != nil {
			return streamID{}, errors.New("-ERR Invalid stream ID specified as stream command argument")
		}
	}

	id := streamID{milliseconds: milliseconds, sequence: sequence}
	if id.milliseconds == 0 && id.sequence == 0 {
		return streamID{}, errors.New("-ERR The ID specified in XADD must be greater than 0-0")
	}
	if !id.greaterThan(last) {
		return streamID{}, errors.New("-ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	return id, nil
}

func generatedStreamID(last streamID) streamID {
	milliseconds := uint64(time.Now().UnixMilli())
	if milliseconds < last.milliseconds {
		milliseconds = last.milliseconds
	}
	sequence := uint64(0)
	if milliseconds == last.milliseconds {
		sequence = last.sequence + 1
	}
	return streamID{milliseconds: milliseconds, sequence: sequence}
}

func (id streamID) greaterThan(other streamID) bool {
	return id.milliseconds > other.milliseconds ||
		(id.milliseconds == other.milliseconds && id.sequence > other.sequence)
}

func (id streamID) String() string {
	return strconv.FormatUint(id.milliseconds, 10) + "-" + strconv.FormatUint(id.sequence, 10)
}

func executeXRange(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) != 4 && len(arguments) != 6 {
		return []byte("-ERR wrong number of arguments for 'xrange' command\r\n")
	}

	start, err := parseRangeID(arguments[2], false)
	if err != nil {
		return []byte("-ERR invalid stream ID specified as stream command argument\r\n")
	}
	end, err := parseRangeID(arguments[3], true)
	if err != nil {
		return []byte("-ERR invalid stream ID specified as stream command argument\r\n")
	}

	count := -1
	if len(arguments) == 6 {
		if !isCommand(arguments[4], "COUNT") {
			return []byte("-ERR syntax error\r\n")
		}
		count, err = strconv.Atoi(string(arguments[5]))
		if err != nil || count < 0 {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}
		if count == 0 {
			return streamEntriesResponse(nil)
		}
	}

	value, ok := store[string(arguments[1])]
	if !ok {
		return streamEntriesResponse(nil)
	}
	if value.kind != streamKind {
		return wrongTypeError()
	}

	entries := make([]streamEntry, 0)
	for _, entry := range value.stream {
		if entry.id.lessThan(start) || entry.id.greaterThan(end) {
			continue
		}
		entries = append(entries, entry)
		if count >= 0 && len(entries) == count {
			break
		}
	}
	return streamEntriesResponse(entries)
}

func executeXRead(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) < 4 || !isCommand(arguments[1], "STREAMS") || (len(arguments)-2)%2 != 0 {
		return []byte("-ERR syntax error\r\n")
	}

	streamCount := (len(arguments) - 2) / 2
	keys := arguments[2 : 2+streamCount]
	ids := arguments[2+streamCount:]
	results := make([]streamReadResult, 0, streamCount)

	for i, keyArgument := range keys {
		key := string(keyArgument)
		value, exists := store[key]
		if exists && value.kind != streamKind {
			return wrongTypeError()
		}

		start, err := readStartID(ids[i], value, exists)
		if err != nil {
			return []byte("-ERR invalid stream ID specified as stream command argument\r\n")
		}

		if !exists {
			continue
		}
		entries := make([]streamEntry, 0)
		for _, entry := range value.stream {
			if entry.id.greaterThan(start) {
				entries = append(entries, entry)
			}
		}
		if len(entries) > 0 {
			results = append(results, streamReadResult{key: key, entries: entries})
		}
	}

	if len(results) == 0 {
		return []byte("*-1\r\n")
	}
	return streamReadResponse(results)
}

type streamReadResult struct {
	key     string
	entries []streamEntry
}

func readStartID(raw []byte, value redisValue, exists bool) (streamID, error) {
	if bytes.Equal(raw, []byte("$")) {
		if exists && len(value.stream) > 0 {
			return value.stream[len(value.stream)-1].id, nil
		}
		return streamID{}, nil
	}
	return parseRangeID(raw, false)
}

func streamReadResponse(results []streamReadResult) []byte {
	response := appendArrayHeader(nil, len(results))
	for _, result := range results {
		response = appendArrayHeader(response, 2)
		response = append(response, bulkString([]byte(result.key))...)
		response = append(response, streamEntriesResponse(result.entries)...)
	}
	return response
}

func parseRangeID(raw []byte, end bool) (streamID, error) {
	if bytes.Equal(raw, []byte("-")) {
		return streamID{}, nil
	}
	if bytes.Equal(raw, []byte("+")) {
		return streamID{milliseconds: ^uint64(0), sequence: ^uint64(0)}, nil
	}

	dash := bytes.IndexByte(raw, '-')
	if dash == -1 {
		milliseconds, err := strconv.ParseUint(string(raw), 10, 64)
		if err != nil {
			return streamID{}, err
		}
		if end {
			return streamID{milliseconds: milliseconds, sequence: ^uint64(0)}, nil
		}
		return streamID{milliseconds: milliseconds}, nil
	}
	if dash <= 0 || dash == len(raw)-1 {
		return streamID{}, errors.New("invalid stream ID")
	}
	milliseconds, err := strconv.ParseUint(string(raw[:dash]), 10, 64)
	if err != nil {
		return streamID{}, err
	}
	sequence, err := strconv.ParseUint(string(raw[dash+1:]), 10, 64)
	if err != nil {
		return streamID{}, err
	}
	return streamID{milliseconds: milliseconds, sequence: sequence}, nil
}

func (id streamID) lessThan(other streamID) bool {
	return other.greaterThan(id)
}

func streamEntriesResponse(entries []streamEntry) []byte {
	response := appendArrayHeader(nil, len(entries))
	for _, entry := range entries {
		response = appendArrayHeader(response, 2)
		response = append(response, bulkString([]byte(entry.id.String()))...)
		response = appendArrayHeader(response, len(entry.fields))
		for _, field := range entry.fields {
			response = append(response, bulkString(field)...)
		}
	}
	return response
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
	response = appendArrayHeader(response, len(values))
	for _, value := range values {
		response = append(response, bulkString(value)...)
	}
	return response
}

func appendArrayHeader(response []byte, count int) []byte {
	response = append(response, '*')
	response = strconv.AppendInt(response, int64(count), 10)
	return append(response, '\r', '\n')
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
