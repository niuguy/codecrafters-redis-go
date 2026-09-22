package main

import (
	"bufio"
	"bytes"
	cryptorand "crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	defaultPort       = 6379
	defaultDBFilename = "dump.rdb"
	defaultAppendOnly = "no"
	defaultAppendDir  = "appendonlydir"
	defaultAppendFile = "appendonly.aof"
	defaultAppendSync = "everysec"
)

var defaultDir = currentWorkingDirectory()

var masterReplicationID = newReplicationID()
var serverRole = "master"
var replicaMasterConn net.Conn
var configuredDir = defaultDir
var configuredDBFilename = defaultDBFilename
var configuredAppendOnly = defaultAppendOnly
var configuredAppendDir = defaultAppendDir
var configuredAppendFile = defaultAppendFile
var configuredAppendSync = defaultAppendSync

var replicationPing = []byte("*1\r\n$4\r\nPING\r\n")

const emptyRDBBase64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

var emptyRDB = mustDecodeBase64(emptyRDBBase64)

type serverConfig struct {
	port           int
	replicaOf      string
	dir            string
	dbfilename     string
	appendOnly     string
	appendDirName  string
	appendFilename string
	appendFsync    string
}

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
	zset   map[string]float64
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
	client        *clientState
	connection    net.Conn
	fromReplica   bool
	disconnect    bool
	blocked       *blockedRequest
	blockedStream *blockedStreamRequest
	wait          *replicationWaitRequest
	expiration    *expirationEvent
}

type transactionContext struct {
	events            chan<- commandEvent
	replicas          map[net.Conn]struct{}
	replicationOffset *int64
	lastWriteOffset   *int64
}

type clientState struct {
	inMulti       bool
	subscribed    bool
	queue         [][]byte
	watched       map[string]uint64
	subscriptions map[string]struct{}
	outbound      chan []byte
	done          chan struct{}
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

type replicationWaitRequest struct {
	event    commandEvent
	target   int64
	required int64
	done     bool
	timer    *time.Timer
}

type expirationEvent struct {
	key     string
	version uint64
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	config, err := parseServerConfig(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}
	serverRole = config.role()
	configuredDir = config.dir
	configuredDBFilename = config.dbfilename
	configuredAppendOnly = config.appendOnly
	configuredAppendDir = config.appendDirName
	configuredAppendFile = config.appendFilename
	configuredAppendSync = config.appendFsync
	store, err := loadRDBStore(filepath.Join(config.dir, config.dbfilename))
	if err != nil {
		log.Printf("Could not load RDB file: %v", err)
		store = make(map[string]redisValue)
	}
	address := net.JoinHostPort("0.0.0.0", strconv.Itoa(config.port))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to bind to port %d: %v", config.port, err)
	}
	defer listener.Close()
	events := make(chan commandEvent)
	go runEventLoop(events, store)
	if config.replicaOf != "" {
		go initiateReplicaHandshakeWithEvents(config.replicaOf, config.port, events)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err)
			continue
		}
		go handleConn(conn, events)
	}
}

func currentWorkingDirectory() string {
	directory, err := os.Getwd()
	if err != nil {
		return "."
	}
	return directory
}

func parsePort(arguments []string) (int, error) {
	if len(arguments) == 0 {
		return defaultPort, nil
	}
	if len(arguments) != 2 || arguments[0] != "--port" {
		return 0, errors.New("usage: your_program --port <port>")
	}

	port, err := strconv.Atoi(arguments[1])
	if err != nil || port < 1 || port > 65535 {
		return 0, errors.New("port must be an integer between 1 and 65535")
	}
	return port, nil
}

func parseServerConfig(arguments []string) (serverConfig, error) {
	config := serverConfig{
		port:           defaultPort,
		dir:            currentWorkingDirectory(),
		dbfilename:     defaultDBFilename,
		appendOnly:     defaultAppendOnly,
		appendDirName:  defaultAppendDir,
		appendFilename: defaultAppendFile,
		appendFsync:    defaultAppendSync,
	}
	for position := 0; position < len(arguments); {
		switch arguments[position] {
		case "--port":
			if position+1 >= len(arguments) {
				return serverConfig{}, errors.New("usage: your_program --port <port>")
			}
			port, err := parsePort(arguments[position : position+2])
			if err != nil {
				return serverConfig{}, err
			}
			config.port = port
			position += 2
		case "--replicaof":
			if position+1 >= len(arguments) {
				return serverConfig{}, errors.New("usage: your_program --replicaof \"<host> <port>\"")
			}
			replicaOf := strings.Fields(arguments[position+1])
			consumed := 2
			if len(replicaOf) != 2 && position+2 < len(arguments) && !strings.HasPrefix(arguments[position+2], "--") {
				replicaOf = []string{arguments[position+1], arguments[position+2]}
				consumed = 3
			}
			if len(replicaOf) != 2 {
				return serverConfig{}, errors.New("usage: your_program --replicaof \"<host> <port>\"")
			}
			if _, err := parsePort([]string{"--port", replicaOf[1]}); err != nil {
				return serverConfig{}, errors.New("replica port must be an integer between 1 and 65535")
			}
			config.replicaOf = strings.Join(replicaOf, " ")
			position += consumed
		case "--dir":
			if position+1 >= len(arguments) || strings.HasPrefix(arguments[position+1], "--") {
				return serverConfig{}, errors.New("usage: your_program --dir <path>")
			}
			config.dir = arguments[position+1]
			position += 2
		case "--dbfilename":
			if position+1 >= len(arguments) || strings.HasPrefix(arguments[position+1], "--") {
				return serverConfig{}, errors.New("usage: your_program --dbfilename <filename>")
			}
			config.dbfilename = arguments[position+1]
			position += 2
		default:
			return serverConfig{}, errors.New("usage: your_program [--port <port>] [--replicaof \"<host> <port>\"] [--dir <path>] [--dbfilename <filename>]")
		}
	}
	return config, nil
}

func (config serverConfig) role() string {
	if config.replicaOf != "" {
		return "slave"
	}
	return "master"
}

func initiateReplicaHandshake(replicaOf string, listeningPorts ...int) {
	listeningPort := defaultPort
	if len(listeningPorts) > 0 {
		listeningPort = listeningPorts[0]
	}
	initiateReplicaHandshakeWithEvents(replicaOf, listeningPort, nil)
}

func initiateReplicaHandshakeWithEvents(replicaOf string, listeningPort int, events chan<- commandEvent) {
	address, err := replicaAddress(replicaOf)
	if err != nil {
		log.Println("Invalid replica master address:", err)
		return
	}

	for {
		connection, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if err := connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			_ = connection.Close()
			continue
		}
		reader := bufio.NewReader(connection)
		if err := sendHandshakeCommand(connection, reader, replicationPing, "+PONG\r\n"); err != nil {
			_ = connection.Close()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		commands := [][]byte{
			encodeRESPCommand("REPLCONF", "listening-port", strconv.Itoa(listeningPort)),
			encodeRESPCommand("REPLCONF", "capa", "psync2"),
		}
		valid := true
		for _, command := range commands {
			if err := sendHandshakeCommand(connection, reader, command, "+OK\r\n"); err != nil {
				valid = false
				break
			}
		}
		if !valid {
			_ = connection.Close()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if _, err := connection.Write(encodeRESPCommand("PSYNC", "?", "-1")); err != nil {
			_ = connection.Close()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err := readFullResync(reader); err != nil {
			_ = connection.Close()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err := connection.SetDeadline(time.Time{}); err != nil {
			_ = connection.Close()
			continue
		}
		replicaMasterConn = connection
		if events != nil {
			go receiveReplicaCommands(connection, reader, events)
		}
		return
	}
}

func readFullResync(reader *bufio.Reader) error {
	response, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(response, "+FULLRESYNC ") || !strings.HasSuffix(response, "\r\n") {
		return fmt.Errorf("unexpected full resync response %q", response)
	}
	header, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	if len(header) < 4 || header[0] != '$' || !strings.HasSuffix(header, "\r\n") {
		return fmt.Errorf("invalid RDB bulk string header %q", header)
	}
	length, err := strconv.Atoi(strings.TrimSuffix(header[1:], "\r\n"))
	if err != nil || length < 0 {
		return fmt.Errorf("invalid RDB length %q", header)
	}
	contents := make([]byte, length)
	_, err = io.ReadFull(reader, contents)
	return err
}

func receiveReplicaCommands(connection net.Conn, reader *bufio.Reader, events chan<- commandEvent) {
	for {
		command, err := readRESPFrame(reader)
		if err != nil {
			return
		}
		events <- commandEvent{command: command, connection: connection, fromReplica: true}
	}
}

func sendHandshakeCommand(connection net.Conn, reader *bufio.Reader, command []byte, expectedResponse string) error {
	if _, err := connection.Write(command); err != nil {
		return err
	}
	response, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	if response != expectedResponse {
		return fmt.Errorf("unexpected handshake response %q, want %q", response, expectedResponse)
	}
	return nil
}

func encodeRESPCommand(arguments ...string) []byte {
	command := make([]byte, 0, 64)
	command = append(command, '*')
	command = strconv.AppendInt(command, int64(len(arguments)), 10)
	command = append(command, '\r', '\n')
	for _, argument := range arguments {
		command = append(command, '$')
		command = strconv.AppendInt(command, int64(len(argument)), 10)
		command = append(command, '\r', '\n')
		command = append(command, argument...)
		command = append(command, '\r', '\n')
	}
	return command
}

func replicaAddress(replicaOf string) (string, error) {
	parts := strings.Fields(replicaOf)
	if len(parts) != 2 || parts[0] == "" {
		return "", errors.New("replica master must be specified as '<host> <port>'")
	}
	if _, err := parsePort([]string{"--port", parts[1]}); err != nil {
		return "", errors.New("replica master port must be an integer between 1 and 65535")
	}
	return net.JoinHostPort(parts[0], parts[1]), nil
}

func newReplicationID() string {
	identifier := make([]byte, 20)
	if _, err := cryptorand.Read(identifier); err != nil {
		// A zero ID is still the correct length and keeps INFO available if the
		// operating system cannot provide random bytes during startup.
		return hex.EncodeToString(make([]byte, 20))
	}
	return hex.EncodeToString(identifier)
}

func mustDecodeBase64(value string) []byte {
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		panic(err)
	}
	return decoded
}

// runEventLoop serializes command execution and owns the shared Redis state.
func runEventLoop(events chan commandEvent, initialStores ...map[string]redisValue) {
	store := make(map[string]redisValue)
	if len(initialStores) > 0 && initialStores[0] != nil {
		store = initialStores[0]
	}
	versions := make(map[string]uint64)
	waiters := make(map[string][]*blockedRequest)
	streamWaiters := make(map[string][]*blockedStreamRequest)
	subscribers := make(map[string]map[*clientState]struct{})
	replicas := make(map[net.Conn]struct{})
	replicaOffsets := make(map[net.Conn]int64)
	waitRequests := make([]*replicationWaitRequest, 0)
	var replicationOffset int64
	var lastWriteOffset int64

	for event := range events {
		if event.disconnect {
			unregisterClientSubscriptions(event.client, subscribers)
			delete(replicas, event.connection)
			delete(replicaOffsets, event.connection)
			wakeReplicationWaiters(&waitRequests, replicas, replicaOffsets)
			continue
		}
		if event.expiration != nil {
			applyExpiration(event.expiration, store, versions)
			continue
		}
		if event.blocked != nil {
			expireBlockedRequest(event.blocked, waiters)
			continue
		}
		if event.blockedStream != nil {
			expireBlockedStreamRequest(event.blockedStream, streamWaiters)
			continue
		}
		if event.wait != nil {
			expireReplicationWait(event.wait, &waitRequests, replicas, replicaOffsets)
			continue
		}

		arguments, err := parseRESPCommand(event.command)
		if event.fromReplica {
			if err != nil {
				continue
			}
			if isReplConfAck(arguments) {
				if event.connection != nil {
					if _, ok := replicas[event.connection]; ok {
						offset := parseReplicaAckOffset(arguments)
						if offset > replicaOffsets[event.connection] {
							replicaOffsets[event.connection] = offset
						}
						wakeReplicationWaiters(&waitRequests, replicas, replicaOffsets)
					}
				}
				continue
			}
			if isReplConfGetAck(arguments) {
				if event.connection != nil {
					_, _ = event.connection.Write(replConfAckResponse(replicaOffsets[event.connection]))
				}
				advanceReplicaOffset(replicaOffsets, event.connection, event.command)
				continue
			}
			response := execute(event.command, store)
			touchModifiedKeys(arguments, response, versions)
			scheduleExpiration(arguments, response, versions, events)
			wakeAfterCommand(arguments, store, waiters, streamWaiters)
			advanceReplicaOffset(replicaOffsets, event.connection, event.command)
			continue
		}
		if event.connection != nil {
			if _, isReplica := replicas[event.connection]; isReplica {
				if isReplConfAck(arguments) {
					offset := parseReplicaAckOffset(arguments)
					if offset > replicaOffsets[event.connection] {
						replicaOffsets[event.connection] = offset
					}
					wakeReplicationWaiters(&waitRequests, replicas, replicaOffsets)
				}
				// ACKs are protocol messages on the replication connection, not
				// client commands. Send an empty response so handleConn can read
				// the next frame without writing anything back to the replica.
				event.response <- nil
				continue
			}
		}
		if err == nil && event.client != nil && event.client.subscribed && !allowedInSubscribedMode(arguments[0]) {
			commandName := strings.ToLower(string(arguments[0]))
			event.response <- subscribedModeError(commandName)
			continue
		}
		if err == nil && event.client != nil && event.client.subscribed && isCommand(arguments[0], "PING") {
			event.response <- subscribedPing(arguments)
			continue
		}
		if err == nil && event.client != nil {
			if isCommand(arguments[0], "WATCH") {
				event.response <- watchKeys(arguments, event.client, versions)
				continue
			}
			if isCommand(arguments[0], "UNWATCH") {
				event.response <- unwatchKeys(arguments, event.client)
				continue
			}
			if isCommand(arguments[0], "MULTI") {
				event.response <- beginTransaction(event.client)
				continue
			}
			if isCommand(arguments[0], "EXEC") {
				event.response <- executeTransaction(event.client, store, versions, waiters, streamWaiters, &transactionContext{
					events:            events,
					replicas:          replicas,
					replicationOffset: &replicationOffset,
					lastWriteOffset:   &lastWriteOffset,
				})
				continue
			}
			if isCommand(arguments[0], "DISCARD") {
				event.response <- discardTransaction(event.client)
				continue
			}
			if isCommand(arguments[0], "SUBSCRIBE") {
				response := executeSubscribe(arguments, event.client)
				registerClientSubscriptions(event.client, arguments[1:], subscribers)
				event.response <- response
				continue
			}
			if isCommand(arguments[0], "UNSUBSCRIBE") {
				event.response <- executeUnsubscribe(arguments, event.client, subscribers)
				continue
			}
			if event.client.inMulti {
				event.client.queue = append(event.client.queue, cloneBytes(event.command))
				event.response <- []byte("+QUEUED\r\n")
				continue
			}
		}
		if err == nil && len(arguments) > 0 && isCommand(arguments[0], "BLPOP") {
			handleBlockingPop(event, arguments, store, waiters, events)
			continue
		}
		if err == nil && len(arguments) > 0 && isBlockingXRead(arguments) {
			handleBlockingXRead(event, arguments, store, streamWaiters, events)
			continue
		}
		if err == nil && len(arguments) > 0 && isCommand(arguments[0], "WAIT") {
			handleWait(event, arguments, replicas, replicaOffsets, lastWriteOffset, &waitRequests, events, &replicationOffset)
			continue
		}
		if err == nil && len(arguments) > 0 && isCommand(arguments[0], "PUBLISH") {
			event.response <- executePublish(arguments, subscribers)
			continue
		}

		response := execute(event.command, store, len(replicas))
		if err == nil {
			touchModifiedKeys(arguments, response, versions)
			scheduleExpiration(arguments, response, versions, events)
			wakeAfterCommand(arguments, store, waiters, streamWaiters)
			if isCommand(arguments[0], "PSYNC") && event.connection != nil {
				replicas[event.connection] = struct{}{}
				replicaOffsets[event.connection] = 0
			} else {
				if shouldPropagateCommand(arguments, response) {
					replicationOffset += int64(len(event.command))
					lastWriteOffset = replicationOffset
				}
				propagateCommand(event.command, arguments, response, replicas)
			}
		}
		event.response <- response
	}
}

func applyExpiration(expiration *expirationEvent, store map[string]redisValue, versions map[string]uint64) {
	if versions[expiration.key] != expiration.version {
		return
	}
	delete(store, expiration.key)
	versions[expiration.key]++
}

func beginTransaction(client *clientState) []byte {
	if client.inMulti {
		return []byte("-ERR MULTI calls can not be nested\r\n")
	}
	client.inMulti = true
	client.queue = nil
	return []byte("+OK\r\n")
}

func watchKeys(arguments [][]byte, client *clientState, versions map[string]uint64) []byte {
	if client.inMulti {
		return []byte("-ERR WATCH inside MULTI is not allowed\r\n")
	}
	if len(arguments) < 2 {
		return []byte("-ERR wrong number of arguments for 'watch' command\r\n")
	}
	if client.watched == nil {
		client.watched = make(map[string]uint64)
	}
	for _, argument := range arguments[1:] {
		key := string(argument)
		client.watched[key] = versions[key]
	}
	return []byte("+OK\r\n")
}

func unwatchKeys(arguments [][]byte, client *clientState) []byte {
	if client.inMulti {
		return []byte("-ERR UNWATCH inside MULTI is not allowed\r\n")
	}
	if len(arguments) != 1 {
		return []byte("-ERR wrong number of arguments for 'unwatch' command\r\n")
	}
	client.watched = nil
	return []byte("+OK\r\n")
}

func discardTransaction(client *clientState) []byte {
	if !client.inMulti {
		return []byte("-ERR DISCARD without MULTI\r\n")
	}
	client.inMulti = false
	client.queue = nil
	client.watched = nil
	return []byte("+OK\r\n")
}

func executeTransaction(client *clientState, store map[string]redisValue, versions map[string]uint64, waiters map[string][]*blockedRequest, streamWaiters map[string][]*blockedStreamRequest, contexts ...*transactionContext) []byte {
	context := &transactionContext{}
	if len(contexts) > 0 && contexts[0] != nil {
		context = contexts[0]
	}
	if !client.inMulti {
		return []byte("-ERR EXEC without MULTI\r\n")
	}

	if watchedKeyChanged(client.watched, versions) {
		client.inMulti = false
		client.queue = nil
		client.watched = nil
		return []byte("*-1\r\n")
	}

	queued := client.queue
	client.inMulti = false
	client.queue = nil
	client.watched = nil
	replies := make([][]byte, 0, len(queued))
	for _, command := range queued {
		arguments, err := parseRESPCommand(command)
		response := execute(command, store, len(context.replicas))
		replies = append(replies, response)
		if err == nil {
			touchModifiedKeys(arguments, response, versions)
			scheduleExpiration(arguments, response, versions, context.events)
			wakeAfterCommand(arguments, store, waiters, streamWaiters)
			if shouldPropagateCommand(arguments, response) && context.replicationOffset != nil {
				*context.replicationOffset += int64(len(command))
				if context.lastWriteOffset != nil {
					*context.lastWriteOffset = *context.replicationOffset
				}
			}
			propagateCommand(command, arguments, response, context.replicas)
		}
	}
	return rawArrayResponse(replies)
}

func watchedKeyChanged(watched, versions map[string]uint64) bool {
	for key, version := range watched {
		if versions[key] != version {
			return true
		}
	}
	return false
}

func wakeAfterCommand(arguments [][]byte, store map[string]redisValue, waiters map[string][]*blockedRequest, streamWaiters map[string][]*blockedStreamRequest) {
	if len(arguments) > 1 && isListPush(arguments) {
		wakeBlockedRequests(arguments[1], store, waiters)
	}
	if len(arguments) > 1 && isCommand(arguments[0], "XADD") {
		wakeBlockedStreamRequests(arguments[1], store, streamWaiters)
	}
}

func touchModifiedKeys(arguments [][]byte, response []byte, versions map[string]uint64) {
	if len(arguments) < 2 || len(response) == 0 || response[0] == '-' {
		return
	}

	command := arguments[0]
	key := string(arguments[1])
	switch {
	case isCommand(command, "SET"),
		isCommand(command, "INCR"),
		isCommand(command, "SETBIT"),
		isCommand(command, "RPUSH"),
		isCommand(command, "LPUSH"),
		isCommand(command, "ZADD"),
		isCommand(command, "XADD"):
		versions[key]++
	case isCommand(command, "DEL"):
		for _, argument := range arguments[1:] {
			versions[string(argument)]++
		}
	case isCommand(command, "LPOP"):
		if !bytes.Equal(response, []byte("$-1\r\n")) &&
			!bytes.Equal(response, []byte("*-1\r\n")) &&
			!bytes.Equal(response, []byte("*0\r\n")) {
			versions[key]++
		}
	case isCommand(command, "ZREM") && !bytes.Equal(response, []byte(":0\r\n")):
		versions[key]++
	}
}

func shouldPropagateCommand(arguments [][]byte, response []byte) bool {
	if !isWriteCommand(arguments) || len(response) == 0 || response[0] == '-' {
		return false
	}
	if isCommand(arguments[0], "LPOP") &&
		(bytes.Equal(response, []byte("$-1\r\n")) ||
			bytes.Equal(response, []byte("*-1\r\n")) ||
			bytes.Equal(response, []byte("*0\r\n"))) {
		return false
	}
	if isCommand(arguments[0], "ZREM") && bytes.Equal(response, []byte(":0\r\n")) {
		return false
	}
	return true
}

func isWriteCommand(arguments [][]byte) bool {
	if len(arguments) == 0 {
		return false
	}
	switch {
	case isCommand(arguments[0], "SET"),
		isCommand(arguments[0], "DEL"),
		isCommand(arguments[0], "INCR"),
		isCommand(arguments[0], "SETBIT"),
		isCommand(arguments[0], "RPUSH"),
		isCommand(arguments[0], "LPUSH"),
		isCommand(arguments[0], "LPOP"),
		isCommand(arguments[0], "ZADD"),
		isCommand(arguments[0], "ZREM"),
		isCommand(arguments[0], "XADD"):
		return true
	default:
		return false
	}
}

func propagateCommand(command []byte, arguments [][]byte, response []byte, replicas map[net.Conn]struct{}) {
	if !shouldPropagateCommand(arguments, response) {
		return
	}
	for connection := range replicas {
		if _, err := connection.Write(command); err != nil {
			delete(replicas, connection)
			_ = connection.Close()
		}
	}
}

func isReplConfAck(arguments [][]byte) bool {
	if len(arguments) != 3 ||
		!isCommand(arguments[0], "REPLCONF") ||
		!isCommand(arguments[1], "ACK") {
		return false
	}
	offset, err := strconv.ParseInt(string(arguments[2]), 10, 64)
	return err == nil && offset >= 0
}

func parseReplicaAckOffset(arguments [][]byte) int64 {
	offset, _ := strconv.ParseInt(string(arguments[2]), 10, 64)
	return offset
}

func requestReplicaAcks(replicas map[net.Conn]struct{}, replicationOffset *int64) {
	command := encodeRESPCommand("REPLCONF", "GETACK", "*")
	sent := false
	for connection := range replicas {
		if _, err := connection.Write(command); err != nil {
			delete(replicas, connection)
			_ = connection.Close()
			continue
		}
		sent = true
	}
	if sent && replicationOffset != nil {
		*replicationOffset += int64(len(command))
	}
}

func countAcknowledgedReplicas(replicas map[net.Conn]struct{}, offsets map[net.Conn]int64, target int64) int {
	count := 0
	for connection := range replicas {
		if offsets[connection] >= target {
			count++
		}
	}
	return count
}

func handleWait(event commandEvent, arguments [][]byte, replicas map[net.Conn]struct{}, offsets map[net.Conn]int64, target int64, requests *[]*replicationWaitRequest, events chan<- commandEvent, replicationOffset *int64) {
	required, timeout, errorResponse := parseWaitArguments(arguments)
	if errorResponse != nil {
		event.response <- errorResponse
		return
	}

	acknowledged := countAcknowledgedReplicas(replicas, offsets, target)
	if target == 0 || int64(acknowledged) >= required {
		event.response <- integerResponse(acknowledged)
		return
	}

	request := &replicationWaitRequest{
		event:    event,
		target:   target,
		required: required,
	}
	*requests = append(*requests, request)
	request.timer = time.AfterFunc(time.Duration(timeout)*time.Millisecond, func() {
		events <- commandEvent{wait: request}
	})
	requestReplicaAcks(replicas, replicationOffset)
	wakeReplicationWaiters(requests, replicas, offsets)
}

func parseWaitArguments(arguments [][]byte) (int64, int64, []byte) {
	if len(arguments) != 3 {
		return 0, 0, []byte("-ERR wrong number of arguments for 'wait' command\r\n")
	}
	required, err := strconv.ParseInt(string(arguments[1]), 10, 64)
	if err != nil || required < 0 {
		return 0, 0, []byte("-ERR numreplicas is not an integer or out of range\r\n")
	}
	timeout, err := strconv.ParseInt(string(arguments[2]), 10, 64)
	if err != nil || timeout < 0 {
		return 0, 0, []byte("-ERR timeout is not an integer or out of range\r\n")
	}
	return required, timeout, nil
}

func wakeReplicationWaiters(requests *[]*replicationWaitRequest, replicas map[net.Conn]struct{}, offsets map[net.Conn]int64) {
	active := (*requests)[:0]
	for _, request := range *requests {
		if request == nil || request.done {
			continue
		}
		acknowledged := countAcknowledgedReplicas(replicas, offsets, request.target)
		if int64(acknowledged) >= request.required {
			completeReplicationWait(request, acknowledged)
			continue
		}
		active = append(active, request)
	}
	*requests = active
}

func completeReplicationWait(request *replicationWaitRequest, acknowledged int) {
	if request.done {
		return
	}
	request.done = true
	if request.timer != nil {
		request.timer.Stop()
	}
	request.event.response <- integerResponse(acknowledged)
}

func expireReplicationWait(request *replicationWaitRequest, requests *[]*replicationWaitRequest, replicas map[net.Conn]struct{}, offsets map[net.Conn]int64) {
	if request == nil || request.done {
		return
	}
	acknowledged := countAcknowledgedReplicas(replicas, offsets, request.target)
	completeReplicationWait(request, acknowledged)
	active := (*requests)[:0]
	for _, current := range *requests {
		if current != request && current != nil && !current.done {
			active = append(active, current)
		}
	}
	*requests = active
}

func isReplConfGetAck(arguments [][]byte) bool {
	return len(arguments) == 3 &&
		isCommand(arguments[0], "REPLCONF") &&
		isCommand(arguments[1], "GETACK") &&
		bytes.Equal(arguments[2], []byte("*"))
}

func replConfAckResponse(offset int64) []byte {
	return encodeRESPCommand("REPLCONF", "ACK", strconv.FormatInt(offset, 10))
}

func advanceReplicaOffset(offsets map[net.Conn]int64, connection net.Conn, command []byte) {
	if connection != nil {
		offsets[connection] += int64(len(command))
	}
}

func isCommand(value []byte, name string) bool {
	return bytes.EqualFold(value, []byte(name))
}

func isListPush(arguments [][]byte) bool {
	return len(arguments) > 1 &&
		(isCommand(arguments[0], "RPUSH") || isCommand(arguments[0], "LPUSH"))
}

func execute(command []byte, store map[string]redisValue, connectedReplicas ...int) []byte {
	arguments, err := parseRESPCommand(command)
	if err != nil || len(arguments) == 0 {
		return []byte("-ERR protocol error\r\n")
	}
	replicaCount := 0
	if len(connectedReplicas) > 0 {
		replicaCount = connectedReplicas[0]
	}

	switch {
	case isCommand(arguments[0], "PING"):
		return executePing(arguments)
	case isCommand(arguments[0], "ECHO"):
		return executeEcho(arguments)
	case isCommand(arguments[0], "SET"):
		return executeSet(arguments, store)
	case isCommand(arguments[0], "DEL"):
		return executeDel(arguments, store)
	case isCommand(arguments[0], "GET"):
		return executeGet(arguments, store)
	case isCommand(arguments[0], "INCR"):
		return executeIncr(arguments, store)
	case isCommand(arguments[0], "SETBIT"):
		return executeSetBit(arguments, store)
	case isCommand(arguments[0], "GETBIT"):
		return executeGetBit(arguments, store)
	case isCommand(arguments[0], "STRLEN"):
		return executeStrLen(arguments, store)
	case isCommand(arguments[0], "ZADD"):
		return executeZAdd(arguments, store)
	case isCommand(arguments[0], "ZREM"):
		return executeZRem(arguments, store)
	case isCommand(arguments[0], "ZRANK"):
		return executeZRank(arguments, store)
	case isCommand(arguments[0], "ZCARD"):
		return executeZCard(arguments, store)
	case isCommand(arguments[0], "ZSCORE"):
		return executeZScore(arguments, store)
	case isCommand(arguments[0], "ZRANGE"):
		return executeZRange(arguments, store)
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
	case isCommand(arguments[0], "KEYS"):
		return executeKeys(arguments, store)
	case isCommand(arguments[0], "SUBSCRIBE"):
		return executeSubscribe(arguments, nil)
	case isCommand(arguments[0], "UNSUBSCRIBE"):
		return executeUnsubscribe(arguments, nil, nil)
	case isCommand(arguments[0], "PUBLISH"):
		return executePublish(arguments, nil)
	case isCommand(arguments[0], "CONFIG"):
		return executeConfig(arguments)
	case isCommand(arguments[0], "INFO"):
		return executeInfo(arguments, store)
	case isCommand(arguments[0], "REPLCONF"):
		return executeReplConf(arguments)
	case isCommand(arguments[0], "PSYNC"):
		return executePSync(arguments)
	case isCommand(arguments[0], "WAIT"):
		return executeWait(arguments, replicaCount)
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
	if _, _, err := setExpiration(arguments); err != nil {
		return []byte("-ERR " + err.Error() + "\r\n")
	}

	store[string(arguments[1])] = redisValue{
		kind:   stringKind,
		string: cloneBytes(arguments[2]),
	}
	return []byte("+OK\r\n")
}

func executeDel(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) < 2 {
		return []byte("-ERR wrong number of arguments for 'del' command\r\n")
	}
	deleted := 0
	for _, argument := range arguments[1:] {
		key := string(argument)
		if _, exists := store[key]; exists {
			delete(store, key)
			deleted++
		}
	}
	return integerResponse(deleted)
}

func setExpiration(arguments [][]byte) (time.Duration, bool, error) {
	if len(arguments) == 3 {
		return 0, false, nil
	}
	if len(arguments) != 5 {
		return 0, false, errors.New("syntax error")
	}

	multiplier := time.Second
	switch {
	case isCommand(arguments[3], "EX"):
		multiplier = time.Second
	case isCommand(arguments[3], "PX"):
		multiplier = time.Millisecond
	default:
		return 0, false, errors.New("syntax error")
	}

	amount, err := strconv.ParseInt(string(arguments[4]), 10, 64)
	if err != nil || amount <= 0 {
		return 0, false, errors.New("invalid expire time in 'set' command")
	}
	if amount > int64((time.Duration(1<<63-1))/multiplier) {
		return 0, false, errors.New("invalid expire time in 'set' command")
	}
	return time.Duration(amount) * multiplier, true, nil
}

func scheduleExpiration(arguments [][]byte, response []byte, versions map[string]uint64, events chan<- commandEvent) {
	if events == nil || len(arguments) < 3 || !isCommand(arguments[0], "SET") || len(response) == 0 || response[0] == '-' {
		return
	}
	duration, hasExpiration, err := setExpiration(arguments)
	if err != nil || !hasExpiration {
		return
	}
	expiration := &expirationEvent{key: string(arguments[1]), version: versions[string(arguments[1])]}
	time.AfterFunc(duration, func() {
		events <- commandEvent{expiration: expiration}
	})
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

func executeIncr(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) != 2 {
		return []byte("-ERR wrong number of arguments for 'incr' command\r\n")
	}

	key := string(arguments[1])
	value, ok := store[key]
	if ok && value.kind != stringKind {
		return wrongTypeError()
	}

	current := int64(0)
	if ok {
		var err error
		current, err = strconv.ParseInt(string(value.string), 10, 64)
		if err != nil || current == 1<<63-1 {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}
	}

	next := current + 1
	store[key] = redisValue{
		kind:   stringKind,
		string: []byte(strconv.FormatInt(next, 10)),
	}
	return integer64Response(next)
}

func executeSetBit(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) != 4 {
		return []byte("-ERR wrong number of arguments for 'setbit' command\r\n")
	}

	byteIndex, mask, ok := parseBitPosition(arguments[2])
	if !ok {
		return []byte("-ERR bit offset is not an integer or out of range\r\n")
	}
	bit, err := strconv.Atoi(string(arguments[3]))
	if err != nil || (bit != 0 && bit != 1) {
		return []byte("-ERR bit is not an integer or out of range\r\n")
	}

	key := string(arguments[1])
	value, ok := store[key]
	if ok && value.kind != stringKind {
		return wrongTypeError()
	}
	if !ok {
		value.kind = stringKind
	}

	oldBit := 0
	if byteIndex < len(value.string) && value.string[byteIndex]&mask != 0 {
		oldBit = 1
	}
	if byteIndex >= len(value.string) {
		extended := make([]byte, byteIndex+1)
		copy(extended, value.string)
		value.string = extended
	}
	if bit == 1 {
		value.string[byteIndex] |= mask
	} else {
		value.string[byteIndex] &^= mask
	}
	store[key] = value
	return integerResponse(oldBit)
}

func executeGetBit(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) != 3 {
		return []byte("-ERR wrong number of arguments for 'getbit' command\r\n")
	}

	byteIndex, mask, ok := parseBitPosition(arguments[2])
	if !ok {
		return []byte("-ERR bit offset is not an integer or out of range\r\n")
	}

	value, exists := store[string(arguments[1])]
	if !exists {
		return integerResponse(0)
	}
	if value.kind != stringKind {
		return wrongTypeError()
	}
	if byteIndex >= len(value.string) {
		return integerResponse(0)
	}
	if value.string[byteIndex]&mask != 0 {
		return integerResponse(1)
	}
	return integerResponse(0)
}

func executeStrLen(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) != 2 {
		return []byte("-ERR wrong number of arguments for 'strlen' command\r\n")
	}

	value, ok := store[string(arguments[1])]
	if !ok {
		return integerResponse(0)
	}
	if value.kind != stringKind {
		return wrongTypeError()
	}
	return integerResponse(len(value.string))
}

func parseBitPosition(raw []byte) (int, byte, bool) {
	offset, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || offset < 0 {
		return 0, 0, false
	}

	byteIndex64 := offset / 8
	maxInt := int(^uint(0) >> 1)
	if byteIndex64 >= int64(maxInt) {
		return 0, 0, false
	}
	return int(byteIndex64), byte(1 << (7 - uint(offset%8))), true
}

func executeZAdd(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) < 4 || (len(arguments)-2)%2 != 0 {
		return []byte("-ERR wrong number of arguments for 'zadd' command\r\n")
	}

	type entry struct {
		score  float64
		member string
	}

	entries := make([]entry, 0, (len(arguments)-2)/2)
	for i := 2; i < len(arguments); i += 2 {
		score, err := strconv.ParseFloat(string(arguments[i]), 64)
		if err != nil || math.IsNaN(score) {
			return []byte("-ERR value is not a valid float\r\n")
		}
		entries = append(entries, entry{score: score, member: string(arguments[i+1])})
	}

	key := string(arguments[1])
	value, ok := store[key]
	if ok && value.kind != zsetKind {
		return wrongTypeError()
	}
	if !ok {
		value.kind = zsetKind
	}
	if value.zset == nil {
		value.zset = make(map[string]float64)
	}

	added := 0
	for _, item := range entries {
		if _, exists := value.zset[item.member]; !exists {
			added++
		}
		value.zset[item.member] = item.score
	}
	store[key] = value
	return integerResponse(added)
}

func executeZRem(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) < 3 {
		return []byte("-ERR wrong number of arguments for 'zrem' command\r\n")
	}

	key := string(arguments[1])
	value, ok := store[key]
	if !ok {
		return integerResponse(0)
	}
	if value.kind != zsetKind {
		return wrongTypeError()
	}

	removed := 0
	for _, argument := range arguments[2:] {
		member := string(argument)
		if _, exists := value.zset[member]; !exists {
			continue
		}
		delete(value.zset, member)
		removed++
	}
	if len(value.zset) == 0 {
		delete(store, key)
	} else {
		store[key] = value
	}
	return integerResponse(removed)
}

type sortedSetMember struct {
	member string
	score  float64
}

func sortedSetMembers(zset map[string]float64) []sortedSetMember {
	members := make([]sortedSetMember, 0, len(zset))
	for member, score := range zset {
		members = append(members, sortedSetMember{member: member, score: score})
	}
	slices.SortFunc(members, func(left, right sortedSetMember) int {
		if left.score < right.score {
			return -1
		}
		if left.score > right.score {
			return 1
		}
		return strings.Compare(left.member, right.member)
	})
	return members
}

func executeZRank(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) != 3 {
		return []byte("-ERR wrong number of arguments for 'zrank' command\r\n")
	}

	value, ok := store[string(arguments[1])]
	if !ok {
		return []byte("$-1\r\n")
	}
	if value.kind != zsetKind {
		return wrongTypeError()
	}

	members := sortedSetMembers(value.zset)

	for rank, member := range members {
		if member.member == string(arguments[2]) {
			return integerResponse(rank)
		}
	}
	return []byte("$-1\r\n")
}

func executeZCard(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) != 2 {
		return []byte("-ERR wrong number of arguments for 'zcard' command\r\n")
	}

	value, ok := store[string(arguments[1])]
	if !ok {
		return integerResponse(0)
	}
	if value.kind != zsetKind {
		return wrongTypeError()
	}
	return integerResponse(len(value.zset))
}

func executeZScore(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) != 3 {
		return []byte("-ERR wrong number of arguments for 'zscore' command\r\n")
	}

	value, ok := store[string(arguments[1])]
	if !ok {
		return []byte("$-1\r\n")
	}
	if value.kind != zsetKind {
		return wrongTypeError()
	}

	score, ok := value.zset[string(arguments[2])]
	if !ok {
		return []byte("$-1\r\n")
	}
	return bulkString([]byte(strconv.FormatFloat(score, 'g', -1, 64)))
}

func executeZRange(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) != 4 {
		return []byte("-ERR wrong number of arguments for 'zrange' command\r\n")
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
	if value.kind != zsetKind {
		return wrongTypeError()
	}

	members := sortedSetMembers(value.zset)
	start, stop, ok = listRange(start, stop, len(members))
	if !ok {
		return arrayResponse(nil)
	}

	result := make([][]byte, 0, stop-start+1)
	for _, member := range members[start : stop+1] {
		result = append(result, []byte(member.member))
	}
	return arrayResponse(result)
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

func executeKeys(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) != 2 {
		return []byte("-ERR wrong number of arguments for 'keys' command\r\n")
	}
	if !bytes.Equal(arguments[1], []byte("*")) {
		return []byte("-ERR only the '*' pattern is supported\r\n")
	}

	keys := make([]string, 0, len(store))
	for key := range store {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	values := make([][]byte, 0, len(keys))
	for _, key := range keys {
		values = append(values, []byte(key))
	}
	return arrayResponse(values)
}

func executeSubscribe(arguments [][]byte, client *clientState) []byte {
	if len(arguments) < 2 {
		return []byte("-ERR wrong number of arguments for 'subscribe' command\r\n")
	}
	if client == nil {
		client = &clientState{}
	}
	if client.subscriptions == nil {
		client.subscriptions = make(map[string]struct{})
	}
	client.subscribed = true

	response := make([]byte, 0, len(arguments)*32)
	for _, argument := range arguments[1:] {
		channel := string(argument)
		client.subscriptions[channel] = struct{}{}
		response = append(response, rawArrayResponse([][]byte{
			bulkString([]byte("subscribe")),
			bulkString(argument),
			integerResponse(len(client.subscriptions)),
		})...)
	}
	return response
}

func registerClientSubscriptions(client *clientState, channels [][]byte, subscribers map[string]map[*clientState]struct{}) {
	if client == nil {
		return
	}
	for _, channel := range channels {
		name := string(channel)
		if subscribers[name] == nil {
			subscribers[name] = make(map[*clientState]struct{})
		}
		subscribers[name][client] = struct{}{}
	}
}

func executeUnsubscribe(arguments [][]byte, client *clientState, subscribers map[string]map[*clientState]struct{}) []byte {
	if client == nil {
		client = &clientState{}
	}
	channels := make([]string, 0, len(arguments)-1)
	if len(arguments) == 1 {
		for channel := range client.subscriptions {
			channels = append(channels, channel)
		}
		slices.Sort(channels)
		if len(channels) == 0 {
			channels = append(channels, "")
		}
	} else {
		for _, argument := range arguments[1:] {
			channels = append(channels, string(argument))
		}
	}

	response := make([]byte, 0, len(channels)*32)
	for _, channel := range channels {
		delete(client.subscriptions, channel)
		if subscribers != nil {
			clients := subscribers[channel]
			delete(clients, client)
			if len(clients) == 0 {
				delete(subscribers, channel)
			}
		}
		response = append(response, rawArrayResponse([][]byte{
			bulkString([]byte("unsubscribe")),
			bulkString([]byte(channel)),
			integerResponse(len(client.subscriptions)),
		})...)
	}
	client.subscribed = len(client.subscriptions) > 0
	return response
}

func unregisterClientSubscriptions(client *clientState, subscribers map[string]map[*clientState]struct{}) {
	if client == nil {
		return
	}
	for channel := range client.subscriptions {
		clients := subscribers[channel]
		delete(clients, client)
		if len(clients) == 0 {
			delete(subscribers, channel)
		}
	}
}

func executePublish(arguments [][]byte, subscribers map[string]map[*clientState]struct{}) []byte {
	if len(arguments) != 3 {
		return []byte("-ERR wrong number of arguments for 'publish' command\r\n")
	}
	clients := subscribers[string(arguments[1])]
	for client := range clients {
		enqueueClientOutput(client, publishedMessage(arguments[1], arguments[2]))
	}
	return integerResponse(len(clients))
}

func publishedMessage(channel, message []byte) []byte {
	return rawArrayResponse([][]byte{
		bulkString([]byte("message")),
		bulkString(channel),
		bulkString(message),
	})
}

func enqueueClientOutput(client *clientState, output []byte) {
	if client == nil || client.outbound == nil {
		return
	}
	if client.done == nil {
		client.outbound <- output
		return
	}
	select {
	case client.outbound <- output:
	case <-client.done:
	}
}

func allowedInSubscribedMode(command []byte) bool {
	return isCommand(command, "SUBSCRIBE") ||
		isCommand(command, "UNSUBSCRIBE") ||
		isCommand(command, "PSUBSCRIBE") ||
		isCommand(command, "PUNSUBSCRIBE") ||
		isCommand(command, "PING") ||
		isCommand(command, "QUIT")
}

func subscribedModeError(command string) []byte {
	return []byte("-ERR Can't execute '" + command + "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n")
}

func subscribedPing(arguments [][]byte) []byte {
	if len(arguments) > 2 {
		return []byte("-ERR wrong number of arguments for 'ping' command\r\n")
	}
	message := []byte{}
	if len(arguments) == 2 {
		message = arguments[1]
	}
	return rawArrayResponse([][]byte{bulkString([]byte("pong")), bulkString(message)})
}

const (
	rdbOpcodeAux          = 0xFA
	rdbOpcodeResizeDB     = 0xFB
	rdbOpcodeExpireTimeMS = 0xFC
	rdbOpcodeExpireTime   = 0xFD
	rdbOpcodeSelectDB     = 0xFE
	rdbOpcodeEOF          = 0xFF
)

func loadRDBStore(path string) (map[string]redisValue, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return make(map[string]redisValue), nil
	}
	if err != nil {
		return nil, err
	}
	return parseRDBStore(data)
}

func parseRDBStore(data []byte) (map[string]redisValue, error) {
	store := make(map[string]redisValue)
	if len(data) < 9 || !bytes.Equal(data[:5], []byte("REDIS")) {
		return nil, errors.New("invalid RDB header")
	}

	position := 9
	var expiryMilliseconds int64
	for position < len(data) {
		opcode := data[position]
		position++

		switch opcode {
		case rdbOpcodeEOF:
			return store, nil
		case rdbOpcodeAux:
			var err error
			if _, position, err = readRDBString(data, position); err != nil {
				return nil, err
			}
			if _, position, err = readRDBString(data, position); err != nil {
				return nil, err
			}
			continue
		case rdbOpcodeSelectDB:
			var err error
			_, _, _, position, err = readRDBLength(data, position)
			if err != nil {
				return nil, err
			}
			continue
		case rdbOpcodeResizeDB:
			var err error
			_, _, _, position, err = readRDBLength(data, position)
			if err != nil {
				return nil, err
			}
			_, _, _, position, err = readRDBLength(data, position)
			if err != nil {
				return nil, err
			}
			continue
		case rdbOpcodeExpireTimeMS:
			if position+8 > len(data) {
				return nil, errors.New("truncated RDB millisecond expiration")
			}
			expiryMilliseconds = int64(binary.LittleEndian.Uint64(data[position : position+8]))
			position += 8
			continue
		case rdbOpcodeExpireTime:
			if position+4 > len(data) {
				return nil, errors.New("truncated RDB expiration")
			}
			seconds := binary.LittleEndian.Uint32(data[position : position+4])
			expiryMilliseconds = int64(seconds) * 1000
			position += 4
			continue
		case 0xF8: // IDLE
			var err error
			_, _, _, position, err = readRDBLength(data, position)
			if err != nil {
				return nil, err
			}
			continue
		case 0xF9: // LFU frequency
			if position >= len(data) {
				return nil, errors.New("truncated RDB frequency")
			}
			position++
			continue
		}

		key, nextPosition, err := readRDBString(data, position)
		if err != nil {
			return nil, err
		}
		position = nextPosition
		if opcode != 0 {
			return nil, fmt.Errorf("unsupported RDB value type 0x%02x", opcode)
		}
		value, nextPosition, err := readRDBString(data, position)
		if err != nil {
			return nil, err
		}
		position = nextPosition
		if expiryMilliseconds > 0 && expiryMilliseconds <= time.Now().UnixMilli() {
			expiryMilliseconds = 0
			continue
		}
		store[string(key)] = redisValue{kind: stringKind, string: cloneBytes(value)}
		expiryMilliseconds = 0
	}
	return store, nil
}

func readRDBLength(data []byte, position int) (length int, special bool, encoding byte, nextPosition int, err error) {
	if position >= len(data) {
		return 0, false, 0, position, errors.New("truncated RDB length")
	}
	first := data[position]
	position++
	switch first >> 6 {
	case 0:
		return int(first & 0x3F), false, 0, position, nil
	case 1:
		if position >= len(data) {
			return 0, false, 0, position, errors.New("truncated RDB 14-bit length")
		}
		return int(first&0x3F)<<8 | int(data[position]), false, 0, position + 1, nil
	case 2:
		if position+4 > len(data) {
			return 0, false, 0, position, errors.New("truncated RDB 32-bit length")
		}
		value := binary.BigEndian.Uint32(data[position : position+4])
		if uint64(value) > uint64(^uint(0)>>1) {
			return 0, false, 0, position, errors.New("RDB length is too large")
		}
		return int(value), false, 0, position + 4, nil
	default:
		return 0, true, first & 0x3F, position, nil
	}
}

func readRDBString(data []byte, position int) ([]byte, int, error) {
	length, special, encoding, position, err := readRDBLength(data, position)
	if err != nil {
		return nil, position, err
	}
	if special {
		switch encoding {
		case 0:
			if position >= len(data) {
				return nil, position, errors.New("truncated RDB 8-bit integer")
			}
			return []byte(strconv.FormatInt(int64(int8(data[position])), 10)), position + 1, nil
		case 1:
			if position+2 > len(data) {
				return nil, position, errors.New("truncated RDB 16-bit integer")
			}
			value := int16(binary.LittleEndian.Uint16(data[position : position+2]))
			return []byte(strconv.FormatInt(int64(value), 10)), position + 2, nil
		case 2:
			if position+4 > len(data) {
				return nil, position, errors.New("truncated RDB 32-bit integer")
			}
			value := int32(binary.LittleEndian.Uint32(data[position : position+4]))
			return []byte(strconv.FormatInt(int64(value), 10)), position + 4, nil
		default:
			return nil, position, fmt.Errorf("unsupported RDB string encoding %d", encoding)
		}
	}
	if length < 0 || position+length > len(data) {
		return nil, position, errors.New("truncated RDB string")
	}
	return data[position : position+length], position + length, nil
}

func executeConfig(arguments [][]byte) []byte {
	if len(arguments) < 3 || !isCommand(arguments[1], "GET") {
		return []byte("-ERR wrong number of arguments for 'config get' command\r\n")
	}

	values := make([][]byte, 0, (len(arguments)-2)*2)
	for _, argument := range arguments[2:] {
		key := strings.ToLower(string(argument))
		switch key {
		case "dir":
			values = append(values, []byte("dir"), []byte(configuredDir))
		case "dbfilename":
			values = append(values, []byte("dbfilename"), []byte(configuredDBFilename))
		case "appendonly":
			values = append(values, []byte("appendonly"), []byte(configuredAppendOnly))
		case "appenddirname":
			values = append(values, []byte("appenddirname"), []byte(configuredAppendDir))
		case "appendfilename":
			values = append(values, []byte("appendfilename"), []byte(configuredAppendFile))
		case "appendfsync":
			values = append(values, []byte("appendfsync"), []byte(configuredAppendSync))
		case "*":
			values = append(values,
				[]byte("dir"), []byte(configuredDir),
				[]byte("dbfilename"), []byte(configuredDBFilename),
				[]byte("appendonly"), []byte(configuredAppendOnly),
				[]byte("appenddirname"), []byte(configuredAppendDir),
				[]byte("appendfilename"), []byte(configuredAppendFile),
				[]byte("appendfsync"), []byte(configuredAppendSync),
			)
		}
	}
	return arrayResponse(values)
}

func executeInfo(arguments [][]byte, store map[string]redisValue) []byte {
	if len(arguments) > 2 {
		return []byte("-ERR wrong number of arguments for 'info' command\r\n")
	}

	section := "default"
	if len(arguments) == 2 {
		section = string(arguments[1])
	}

	server := fmt.Sprintf(
		"# Server\r\nredis_version:7.2.0\r\nredis_mode:standalone\r\nos:%s\r\narch_bits:%d\r\nprocess_id:%d\r\n\r\n",
		runtime.GOOS,
		strconv.IntSize,
		os.Getpid(),
	)
	keyspace := fmt.Sprintf("# Keyspace\r\ndb0:keys=%d,expires=0,avg_ttl=0\r\n\r\n", len(store))
	stats := "# Stats\r\ntotal_commands_processed:0\r\n\r\n"
	replication := "# Replication\r\n" +
		"role:" + serverRole + "\r\n" +
		"connected_slaves:0\r\n" +
		"master_replid:" + masterReplicationID + "\r\n" +
		"master_repl_offset:0\r\n" +
		"second_repl_offset:-1\r\n" +
		"repl_backlog_active:0\r\n" +
		"repl_backlog_size:1048576\r\n" +
		"repl_backlog_first_byte_offset:0\r\n" +
		"repl_backlog_histlen:0\r\n\r\n"

	switch {
	case isCommand([]byte(section), "SERVER"):
		return bulkString([]byte(server))
	case isCommand([]byte(section), "KEYSPACE"):
		return bulkString([]byte(keyspace))
	case isCommand([]byte(section), "STATS"):
		return bulkString([]byte(stats))
	case isCommand([]byte(section), "REPLICATION"):
		return bulkString([]byte(replication))
	case isCommand([]byte(section), "ALL"), isCommand([]byte(section), "DEFAULT"):
		return bulkString([]byte(server + stats + replication + keyspace))
	default:
		return bulkString(nil)
	}
}

func executeReplConf(arguments [][]byte) []byte {
	if len(arguments) < 2 {
		return []byte("-ERR wrong number of arguments for 'replconf' command\r\n")
	}
	return []byte("+OK\r\n")
}

func executePSync(arguments [][]byte) []byte {
	if len(arguments) != 3 {
		return []byte("-ERR wrong number of arguments for 'psync' command\r\n")
	}
	response := simpleString("FULLRESYNC " + masterReplicationID + " 0")
	return append(response, rdbBulkString(emptyRDB)...)
}

func executeWait(arguments [][]byte, connectedReplicaCounts ...int) []byte {
	_, _, errorResponse := parseWaitArguments(arguments)
	if errorResponse != nil {
		return errorResponse
	}
	connectedReplicas := 0
	if len(connectedReplicaCounts) > 0 {
		connectedReplicas = connectedReplicaCounts[0]
	}
	return integerResponse(connectedReplicas)
}

func rdbBulkString(value []byte) []byte {
	response := make([]byte, 0, len(value)+24)
	response = append(response, '$')
	response = strconv.AppendInt(response, int64(len(value)), 10)
	response = append(response, '\r', '\n')
	return append(response, value...)
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

func integer64Response(value int64) []byte {
	response := make([]byte, 0, 24)
	response = append(response, ':')
	response = strconv.AppendInt(response, value, 10)
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

func rawArrayResponse(values [][]byte) []byte {
	response := appendArrayHeader(nil, len(values))
	for _, value := range values {
		response = append(response, value...)
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

func readRESPFrame(reader *bufio.Reader) ([]byte, error) {
	arrayHeader, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(arrayHeader) < 4 || arrayHeader[0] != '*' || !bytes.HasSuffix(arrayHeader, []byte("\r\n")) {
		return nil, errors.New("invalid RESP array header")
	}
	count, err := strconv.Atoi(string(arrayHeader[1 : len(arrayHeader)-2]))
	if err != nil || count < 1 {
		return nil, errors.New("invalid RESP array length")
	}

	frame := append([]byte(nil), arrayHeader...)
	for range count {
		bulkHeader, err := reader.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		if len(bulkHeader) < 4 || bulkHeader[0] != '$' || !bytes.HasSuffix(bulkHeader, []byte("\r\n")) {
			return nil, errors.New("invalid RESP bulk string header")
		}
		length, err := strconv.Atoi(string(bulkHeader[1 : len(bulkHeader)-2]))
		if err != nil || length < 0 {
			return nil, errors.New("invalid RESP bulk string length")
		}
		frame = append(frame, bulkHeader...)
		contents := make([]byte, length+2)
		if _, err := io.ReadFull(reader, contents); err != nil {
			return nil, err
		}
		if !bytes.HasSuffix(contents, []byte("\r\n")) {
			return nil, errors.New("invalid RESP bulk string terminator")
		}
		frame = append(frame, contents...)
	}
	return frame, nil
}

func handleConn(conn net.Conn, events chan<- commandEvent) {
	client := &clientState{
		outbound: make(chan []byte, 64),
		done:     make(chan struct{}),
	}
	go func() {
		for {
			select {
			case output := <-client.outbound:
				if len(output) == 0 {
					continue
				}
				if _, err := conn.Write(output); err != nil {
					_ = conn.Close()
					return
				}
			case <-client.done:
				return
			}
		}
	}()
	defer func() {
		events <- commandEvent{connection: conn, client: client, disconnect: true}
		close(client.done)
		_ = conn.Close()
	}()

	// Buffer one response so the event loop does not wait for the connection
	// goroutine to receive before it can process another client.
	response := make(chan []byte, 1)
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
			command:    buf[:n],
			response:   response,
			client:     client,
			connection: conn,
		}

		select {
		case client.outbound <- <-response:
		case <-client.done:
			return
		}
	}
}
