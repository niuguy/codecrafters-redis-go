package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func testRESPCommand(arguments ...string) []byte {
	var command []byte
	command = append(command, '*')
	command = append(command, testIntegerASCII(len(arguments))...)
	command = append(command, '\r', '\n')
	for _, argument := range arguments {
		command = append(command, '$')
		command = append(command, testIntegerASCII(len(argument))...)
		command = append(command, '\r', '\n')
		command = append(command, argument...)
		command = append(command, '\r', '\n')
	}
	return command
}

func testIntegerASCII(value int) []byte {
	return strconv.AppendInt(nil, int64(value), 10)
}

func TestParsePort(t *testing.T) {
	tests := []struct {
		name      string
		arguments []string
		wantPort  int
		wantError bool
	}{
		{name: "default", wantPort: defaultPort},
		{name: "custom", arguments: []string{"--port", "6380"}, wantPort: 6380},
		{name: "minimum", arguments: []string{"--port", "1"}, wantPort: 1},
		{name: "maximum", arguments: []string{"--port", "65535"}, wantPort: 65535},
		{name: "missing value", arguments: []string{"--port"}, wantError: true},
		{name: "unknown flag", arguments: []string{"--host", "6380"}, wantError: true},
		{name: "not a number", arguments: []string{"--port", "redis"}, wantError: true},
		{name: "zero", arguments: []string{"--port", "0"}, wantError: true},
		{name: "too large", arguments: []string{"--port", "65536"}, wantError: true},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			port, err := parsePort(testCase.arguments)
			if testCase.wantError {
				if err == nil {
					t.Fatalf("parsePort(%v) returned port %d without an error", testCase.arguments, port)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePort(%v): %v", testCase.arguments, err)
			}
			if port != testCase.wantPort {
				t.Fatalf("parsePort(%v) = %d, want %d", testCase.arguments, port, testCase.wantPort)
			}
		})
	}
}

func TestParseServerConfig(t *testing.T) {
	master, err := parseServerConfig(nil)
	if err != nil {
		t.Fatalf("parse default config: %v", err)
	}
	if master.port != defaultPort || master.dir != currentWorkingDirectory() || master.dbfilename != defaultDBFilename ||
		master.appendOnly != defaultAppendOnly || master.appendDirName != defaultAppendDir ||
		master.appendFilename != defaultAppendFile || master.appendFsync != defaultAppendSync || master.role() != "master" {
		t.Fatalf("default config = %+v, want port %d and master role", master, defaultPort)
	}

	replica, err := parseServerConfig([]string{"--port", "6380", "--replicaof", "localhost 6379"})
	if err != nil {
		t.Fatalf("parse replica config: %v", err)
	}
	if replica.port != 6380 || replica.replicaOf != "localhost 6379" || replica.role() != "slave" {
		t.Fatalf("replica config = %+v, want port 6380, localhost 6379, slave role", replica)
	}

	separateArguments, err := parseServerConfig([]string{"--replicaof", "localhost", "6379"})
	if err != nil {
		t.Fatalf("parse separate replica arguments: %v", err)
	}
	if separateArguments.role() != "slave" {
		t.Fatalf("separate replica arguments produced role %q", separateArguments.role())
	}

	custom, err := parseServerConfig([]string{
		"--port", "6381",
		"--dir", "/tmp/redis-data",
		"--dbfilename", "redis.rdb",
	})
	if err != nil {
		t.Fatalf("parse persistence config: %v", err)
	}
	if custom.dir != "/tmp/redis-data" || custom.dbfilename != "redis.rdb" {
		t.Fatalf("persistence config = %+v", custom)
	}
}

func TestConfigGetCommand(t *testing.T) {
	previousDir := configuredDir
	previousDBFilename := configuredDBFilename
	previousAppendOnly := configuredAppendOnly
	previousAppendDir := configuredAppendDir
	previousAppendFile := configuredAppendFile
	previousAppendSync := configuredAppendSync
	configuredDir = "/tmp/redis-data"
	configuredDBFilename = "redis.rdb"
	configuredAppendOnly = defaultAppendOnly
	configuredAppendDir = defaultAppendDir
	configuredAppendFile = defaultAppendFile
	configuredAppendSync = defaultAppendSync
	defer func() {
		configuredDir = previousDir
		configuredDBFilename = previousDBFilename
		configuredAppendOnly = previousAppendOnly
		configuredAppendDir = previousAppendDir
		configuredAppendFile = previousAppendFile
		configuredAppendSync = previousAppendSync
	}()

	store := make(map[string]redisValue)
	if got := execute(testRESPCommand("CONFIG", "GET", "dir"), store); string(got) != "*2\r\n$3\r\ndir\r\n$15\r\n/tmp/redis-data\r\n" {
		t.Fatalf("CONFIG GET dir response = %q", got)
	}
	if got := execute(testRESPCommand("CONFIG", "GET", "DBFILENAME"), store); string(got) != "*2\r\n$10\r\ndbfilename\r\n$9\r\nredis.rdb\r\n" {
		t.Fatalf("CONFIG GET dbfilename response = %q", got)
	}
	if got := execute(testRESPCommand("CONFIG", "GET", "dir", "dbfilename"), store); string(got) != "*4\r\n$3\r\ndir\r\n$15\r\n/tmp/redis-data\r\n$10\r\ndbfilename\r\n$9\r\nredis.rdb\r\n" {
		t.Fatalf("CONFIG GET multiple response = %q", got)
	}
	if got := execute(testRESPCommand("CONFIG", "GET", "unknown"), store); string(got) != "*0\r\n" {
		t.Fatalf("CONFIG GET unknown response = %q", got)
	}
	for _, testCase := range []struct {
		name string
		want string
	}{
		{name: "appendonly", want: "no"},
		{name: "appenddirname", want: "appendonlydir"},
		{name: "appendfilename", want: "appendonly.aof"},
		{name: "appendfsync", want: "everysec"},
	} {
		got := execute(testRESPCommand("CONFIG", "GET", testCase.name), store)
		want := string(arrayResponse([][]byte{[]byte(testCase.name), []byte(testCase.want)}))
		if string(got) != want {
			t.Errorf("CONFIG GET %s response = %q, want %q", testCase.name, got, want)
		}
	}
}

func TestLoadRDBSingleStringAndKeys(t *testing.T) {
	rdb := []byte{
		'R', 'E', 'D', 'I', 'S', '0', '0', '1', '1',
		0x00, 0x03, 'f', 'o', 'o', 0x03, 'b', 'a', 'r',
		0xFF,
	}
	path := filepath.Join(t.TempDir(), "dump.rdb")
	if err := os.WriteFile(path, rdb, 0o600); err != nil {
		t.Fatalf("write RDB fixture: %v", err)
	}
	store, err := loadRDBStore(path)
	if err != nil {
		t.Fatalf("load RDB fixture: %v", err)
	}
	if got := execute(testRESPCommand("GET", "foo"), store); string(got) != "$3\r\nbar\r\n" {
		t.Fatalf("loaded GET response = %q", got)
	}
	if got := execute(testRESPCommand("KEYS", "*"), store); string(got) != "*1\r\n$3\r\nfoo\r\n" {
		t.Fatalf("KEYS response = %q", got)
	}
}

func TestKeysOnlySupportsWildcard(t *testing.T) {
	store := map[string]redisValue{
		"baz": {kind: stringKind, string: []byte("qux")},
		"foo": {kind: stringKind, string: []byte("bar")},
	}
	if got := execute(testRESPCommand("KEYS", "f*"), store); got[0] != '-' {
		t.Fatalf("unsupported KEYS pattern response = %q", got)
	}
	if got := execute(testRESPCommand("KEYS"), store); got[0] != '-' {
		t.Fatalf("invalid KEYS response = %q", got)
	}
	if got := execute(testRESPCommand("KEYS", "*"), store); string(got) != "*2\r\n$3\r\nbaz\r\n$3\r\nfoo\r\n" {
		t.Fatalf("sorted KEYS response = %q", got)
	}
}

func TestSubscribeCommand(t *testing.T) {
	client := &clientState{}
	if got := executeSubscribe(testRESPArguments("SUBSCRIBE", "mychan"), client); string(got) != "*3\r\n$9\r\nsubscribe\r\n$6\r\nmychan\r\n:1\r\n" {
		t.Fatalf("SUBSCRIBE response = %q", got)
	}
	if got := executeSubscribe(testRESPArguments("SUBSCRIBE", "other", "mychan"), client); string(got) != "*3\r\n$9\r\nsubscribe\r\n$5\r\nother\r\n:2\r\n*3\r\n$9\r\nsubscribe\r\n$6\r\nmychan\r\n:2\r\n" {
		t.Fatalf("multiple SUBSCRIBE response = %q", got)
	}
	if got := executeSubscribe(testRESPArguments("SUBSCRIBE"), client); got[0] != '-' {
		t.Fatalf("invalid SUBSCRIBE response = %q", got)
	}
	if !client.subscribed {
		t.Fatal("client did not enter subscribed mode")
	}
	for _, command := range []string{"SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT"} {
		if !allowedInSubscribedMode([]byte(command)) {
			t.Errorf("%s was rejected in subscribed mode", command)
		}
	}
	if allowedInSubscribedMode([]byte("ECHO")) {
		t.Fatal("ECHO was allowed in subscribed mode")
	}
	if got := subscribedModeError("echo"); string(got) != "-ERR Can't execute 'echo': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n" {
		t.Fatalf("subscribed mode error = %q", got)
	}
	if got := subscribedPing(testRESPArguments("PING")); string(got) != "*2\r\n$4\r\npong\r\n$0\r\n\r\n" {
		t.Fatalf("subscribed PING response = %q", got)
	}
	if got := subscribedPing(testRESPArguments("PING", "hello")); string(got) != "*2\r\n$4\r\npong\r\n$5\r\nhello\r\n" {
		t.Fatalf("subscribed PING with message response = %q", got)
	}
}

func TestUnsubscribeCommand(t *testing.T) {
	client := &clientState{}
	subscribers := make(map[string]map[*clientState]struct{})
	channels := testRESPArguments("SUBSCRIBE", "mychan", "other")
	executeSubscribe(channels, client)
	registerClientSubscriptions(client, channels[1:], subscribers)

	if got := executeUnsubscribe(testRESPArguments("UNSUBSCRIBE", "mychan"), client, subscribers); string(got) != "*3\r\n$11\r\nunsubscribe\r\n$6\r\nmychan\r\n:1\r\n" {
		t.Fatalf("UNSUBSCRIBE response = %q", got)
	}
	if len(subscribers["mychan"]) != 0 || len(client.subscriptions) != 1 {
		t.Fatalf("subscription registry after UNSUBSCRIBE = %#v, client subscriptions = %#v", subscribers, client.subscriptions)
	}
	if got := executeUnsubscribe(testRESPArguments("UNSUBSCRIBE"), client, subscribers); string(got) != "*3\r\n$11\r\nunsubscribe\r\n$5\r\nother\r\n:0\r\n" {
		t.Fatalf("UNSUBSCRIBE all response = %q", got)
	}
	if client.subscribed {
		t.Fatal("client remained in subscribed mode after removing all channels")
	}
	if got := executeUnsubscribe(testRESPArguments("UNSUBSCRIBE"), client, subscribers); string(got) != "*3\r\n$11\r\nunsubscribe\r\n$0\r\n\r\n:0\r\n" {
		t.Fatalf("UNSUBSCRIBE with no channels response = %q", got)
	}
}

func TestPublishCommandCountsSubscribers(t *testing.T) {
	first := &clientState{}
	second := &clientState{}
	subscribers := make(map[string]map[*clientState]struct{})
	firstChannels := testRESPArguments("SUBSCRIBE", "mychan")
	secondChannels := testRESPArguments("SUBSCRIBE", "mychan", "other")
	executeSubscribe(firstChannels, first)
	executeSubscribe(secondChannels, second)
	registerClientSubscriptions(first, firstChannels[1:], subscribers)
	registerClientSubscriptions(second, secondChannels[1:], subscribers)

	if got := executePublish(testRESPArguments("PUBLISH", "mychan", "hello"), subscribers); string(got) != ":2\r\n" {
		t.Fatalf("PUBLISH subscriber count = %q", got)
	}
	if got := executePublish(testRESPArguments("PUBLISH", "other", "hello"), subscribers); string(got) != ":1\r\n" {
		t.Fatalf("PUBLISH other subscriber count = %q", got)
	}
	unregisterClientSubscriptions(second, subscribers)
	if got := executePublish(testRESPArguments("PUBLISH", "mychan", "hello"), subscribers); string(got) != ":1\r\n" {
		t.Fatalf("PUBLISH count after disconnect = %q", got)
	}
	if got := executePublish(testRESPArguments("PUBLISH", "mychan"), subscribers); got[0] != '-' {
		t.Fatalf("invalid PUBLISH response = %q", got)
	}
}

func TestPublishDeliversMessage(t *testing.T) {
	client := &clientState{outbound: make(chan []byte, 1)}
	channels := testRESPArguments("SUBSCRIBE", "mychan")
	executeSubscribe(channels, client)
	subscribers := make(map[string]map[*clientState]struct{})
	registerClientSubscriptions(client, channels[1:], subscribers)

	if got := executePublish(testRESPArguments("PUBLISH", "mychan", "hello"), subscribers); string(got) != ":1\r\n" {
		t.Fatalf("PUBLISH response = %q", got)
	}
	select {
	case got := <-client.outbound:
		want := "*3\r\n$7\r\nmessage\r\n$6\r\nmychan\r\n$5\r\nhello\r\n"
		if string(got) != want {
			t.Fatalf("published message = %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for published message")
	}
}

func TestInitiateReplicaHandshakeSendsPing(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for handshake test: %v", err)
	}
	defer listener.Close()

	received := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			received <- err
			return
		}
		defer connection.Close()
		commands := [][]byte{
			replicationPing,
			encodeRESPCommand("REPLCONF", "listening-port", "6380"),
			encodeRESPCommand("REPLCONF", "capa", "psync2"),
			encodeRESPCommand("PSYNC", "?", "-1"),
		}
		responses := [][]byte{
			[]byte("+PONG\r\n"),
			[]byte("+OK\r\n"),
			[]byte("+OK\r\n"),
		}
		responses = append(responses, append([]byte("+FULLRESYNC replica-id 0\r\n"), rdbBulkString(emptyRDB)...))
		for i, command := range commands {
			frame := make([]byte, len(command))
			if _, err = io.ReadFull(connection, frame); err != nil {
				break
			}
			if !bytes.Equal(frame, command) {
				err = fmt.Errorf("received %q, want %q", frame, command)
				break
			}
			if _, err = connection.Write(responses[i]); err != nil {
				break
			}
		}
		received <- err
	}()

	port := listener.Addr().(*net.TCPAddr).Port
	events := make(chan commandEvent)
	initiateReplicaHandshakeWithEvents(fmt.Sprintf("127.0.0.1 %d", port), 6380, events)
	if err := <-received; err != nil {
		t.Fatalf("replica handshake: %v", err)
	}
	if replicaMasterConn != nil {
		_ = replicaMasterConn.Close()
		replicaMasterConn = nil
	}
}

func TestParseRESPCommand(t *testing.T) {
	arguments, err := parseRESPCommand(testRESPCommand("ECHO", "hello"))
	if err != nil {
		t.Fatalf("parse valid command: %v", err)
	}
	if got, want := string(arguments[0]), "ECHO"; got != want {
		t.Fatalf("command = %q, want %q", got, want)
	}
	if got, want := string(arguments[1]), "hello"; got != want {
		t.Fatalf("argument = %q, want %q", got, want)
	}

	for _, command := range [][]byte{
		[]byte("PING\r\n"),
		[]byte("*2\r\n$4\r\nECHO\r\n$5\r\nhel"),
		[]byte("*0\r\n"),
	} {
		if _, err := parseRESPCommand(command); err == nil {
			t.Errorf("parseRESPCommand(%q) returned nil error", command)
		}
	}
}

func TestStringCommands(t *testing.T) {
	store := make(map[string]redisValue)

	if got := execute(testRESPCommand("SET", "answer", "42"), store); string(got) != "+OK\r\n" {
		t.Fatalf("SET response = %q", got)
	}
	if got := execute(testRESPCommand("GET", "answer"), store); string(got) != "$2\r\n42\r\n" {
		t.Fatalf("GET response = %q", got)
	}
	if got := execute(testRESPCommand("INCR", "answer"), store); string(got) != ":43\r\n" {
		t.Fatalf("INCR response = %q", got)
	}
	if got := execute(testRESPCommand("GET", "answer"), store); string(got) != "$2\r\n43\r\n" {
		t.Fatalf("GET after INCR response = %q", got)
	}
	if got := execute(testRESPCommand("INCR", "answer", "extra"), store); string(got) != "-ERR wrong number of arguments for 'incr' command\r\n" {
		t.Fatalf("invalid INCR response = %q", got)
	}
}

func TestZAddCommand(t *testing.T) {
	store := make(map[string]redisValue)

	if got := execute(testRESPCommand("ZADD", "racer_scores", "8.0", "Sam"), store); string(got) != ":1\r\n" {
		t.Fatalf("first ZADD response = %q", got)
	}
	if value := store["racer_scores"]; value.kind != zsetKind || value.zset["Sam"] != 8.0 {
		t.Fatalf("stored sorted set = %#v", value)
	}

	if got := execute(testRESPCommand("ZADD", "racer_scores", "9.5", "Sam", "7.0", "Alex"), store); string(got) != ":1\r\n" {
		t.Fatalf("update and add ZADD response = %q", got)
	}
	value := store["racer_scores"]
	if len(value.zset) != 2 || value.zset["Sam"] != 9.5 || value.zset["Alex"] != 7.0 {
		t.Fatalf("updated sorted set = %#v", value.zset)
	}

	if got := execute(testRESPCommand("ZADD", "racer_scores", "bad", "Lee"), store); string(got) != "-ERR value is not a valid float\r\n" {
		t.Fatalf("invalid score response = %q", got)
	}
	if got := execute(testRESPCommand("SET", "string", "value"), store); string(got) != "+OK\r\n" {
		t.Fatalf("SET response = %q", got)
	}
	if got := execute(testRESPCommand("ZADD", "string", "1", "member"), store); string(got) != string(wrongTypeError()) {
		t.Fatalf("wrong type response = %q", got)
	}
	if got := execute(testRESPCommand("ZADD", "racer_scores", "1"), store); got[0] != '-' {
		t.Fatalf("invalid arity response = %q", got)
	}
}

func TestZRemCommand(t *testing.T) {
	store := make(map[string]redisValue)
	execute(testRESPCommand("ZADD", "scores", "8.0", "Sam", "7.0", "Alex"), store)

	if got := execute(testRESPCommand("ZREM", "scores", "Sam", "missing", "Sam"), store); string(got) != ":1\r\n" {
		t.Fatalf("ZREM response = %q", got)
	}
	if got := execute(testRESPCommand("ZCARD", "scores"), store); string(got) != ":1\r\n" {
		t.Fatalf("ZCARD after ZREM response = %q", got)
	}
	if got := execute(testRESPCommand("ZREM", "scores", "Alex"), store); string(got) != ":1\r\n" {
		t.Fatalf("ZREM final member response = %q", got)
	}
	if got := execute(testRESPCommand("ZCARD", "scores"), store); string(got) != ":0\r\n" {
		t.Fatalf("ZCARD after deleting sorted set response = %q", got)
	}
	if got := execute(testRESPCommand("ZREM", "missing-key", "member"), store); string(got) != ":0\r\n" {
		t.Fatalf("ZREM missing key response = %q", got)
	}

	if got := execute(testRESPCommand("SET", "string", "value"), store); string(got) != "+OK\r\n" {
		t.Fatalf("SET response = %q", got)
	}
	if got := execute(testRESPCommand("ZREM", "string", "member"), store); string(got) != string(wrongTypeError()) {
		t.Fatalf("ZREM wrong type response = %q", got)
	}
	if got := execute(testRESPCommand("ZREM", "scores"), store); got[0] != '-' {
		t.Fatalf("ZREM invalid arity response = %q", got)
	}
}

func TestZRankCommand(t *testing.T) {
	store := make(map[string]redisValue)
	execute(testRESPCommand("ZADD", "scores", "8.0", "Sam", "7.0", "Alex", "7.0", "Bob"), store)

	for _, testCase := range []struct {
		member string
		want   string
	}{
		{member: "Alex", want: ":0\r\n"},
		{member: "Bob", want: ":1\r\n"},
		{member: "Sam", want: ":2\r\n"},
		{member: "missing", want: "$-1\r\n"},
	} {
		if got := execute(testRESPCommand("ZRANK", "scores", testCase.member), store); string(got) != testCase.want {
			t.Errorf("ZRANK %s response = %q, want %q", testCase.member, got, testCase.want)
		}
	}
	if got := execute(testRESPCommand("ZRANK", "missing-key", "member"), store); string(got) != "$-1\r\n" {
		t.Errorf("ZRANK missing key response = %q", got)
	}
	if got := execute(testRESPCommand("SET", "string", "value"), store); string(got) != "+OK\r\n" {
		t.Fatalf("SET response = %q", got)
	}
	if got := execute(testRESPCommand("ZRANK", "string", "member"), store); string(got) != string(wrongTypeError()) {
		t.Errorf("ZRANK wrong type response = %q", got)
	}
	if got := execute(testRESPCommand("ZRANK", "scores"), store); got[0] != '-' {
		t.Errorf("ZRANK invalid arity response = %q", got)
	}
}

func TestZRangeCommand(t *testing.T) {
	store := make(map[string]redisValue)
	execute(testRESPCommand("ZADD", "scores", "8.0", "Sam", "7.0", "Alex", "7.0", "Bob", "10.0", "Zoe"), store)

	if got := execute(testRESPCommand("ZRANGE", "scores", "0", "-1"), store); string(got) != "*4\r\n$4\r\nAlex\r\n$3\r\nBob\r\n$3\r\nSam\r\n$3\r\nZoe\r\n" {
		t.Fatalf("ZRANGE all response = %q", got)
	}
	if got := execute(testRESPCommand("ZRANGE", "scores", "1", "2"), store); string(got) != "*2\r\n$3\r\nBob\r\n$3\r\nSam\r\n" {
		t.Fatalf("ZRANGE subset response = %q", got)
	}
	if got := execute(testRESPCommand("ZRANGE", "scores", "-2", "-1"), store); string(got) != "*2\r\n$3\r\nSam\r\n$3\r\nZoe\r\n" {
		t.Fatalf("ZRANGE negative range response = %q", got)
	}
	if got := execute(testRESPCommand("ZRANGE", "missing-key", "0", "-1"), store); string(got) != "*0\r\n" {
		t.Fatalf("ZRANGE missing key response = %q", got)
	}
	if got := execute(testRESPCommand("ZRANGE", "scores", "10", "20"), store); string(got) != "*0\r\n" {
		t.Fatalf("ZRANGE out-of-range response = %q", got)
	}
	if got := execute(testRESPCommand("SET", "string", "value"), store); string(got) != "+OK\r\n" {
		t.Fatalf("SET response = %q", got)
	}
	if got := execute(testRESPCommand("ZRANGE", "string", "0", "-1"), store); string(got) != string(wrongTypeError()) {
		t.Fatalf("ZRANGE wrong type response = %q", got)
	}
	if got := execute(testRESPCommand("ZRANGE", "scores", "bad", "-1"), store); string(got) != "-ERR value is not an integer or out of range\r\n" {
		t.Fatalf("ZRANGE invalid start response = %q", got)
	}
}

func TestZCardCommand(t *testing.T) {
	store := make(map[string]redisValue)
	if got := execute(testRESPCommand("ZCARD", "missing-key"), store); string(got) != ":0\r\n" {
		t.Fatalf("ZCARD missing key response = %q", got)
	}

	execute(testRESPCommand("ZADD", "scores", "8.0", "Sam", "7.0", "Alex"), store)
	if got := execute(testRESPCommand("ZCARD", "scores"), store); string(got) != ":2\r\n" {
		t.Fatalf("ZCARD response = %q", got)
	}
	execute(testRESPCommand("ZADD", "scores", "9.0", "Sam"), store)
	if got := execute(testRESPCommand("ZCARD", "scores"), store); string(got) != ":2\r\n" {
		t.Fatalf("ZCARD after score update response = %q", got)
	}

	if got := execute(testRESPCommand("SET", "string", "value"), store); string(got) != "+OK\r\n" {
		t.Fatalf("SET response = %q", got)
	}
	if got := execute(testRESPCommand("ZCARD", "string"), store); string(got) != string(wrongTypeError()) {
		t.Fatalf("ZCARD wrong type response = %q", got)
	}
	if got := execute(testRESPCommand("ZCARD"), store); got[0] != '-' {
		t.Fatalf("ZCARD invalid arity response = %q", got)
	}
}

func TestZScoreCommand(t *testing.T) {
	store := make(map[string]redisValue)
	execute(testRESPCommand("ZADD", "scores", "8.5", "Sam"), store)

	if got := execute(testRESPCommand("ZSCORE", "scores", "Sam"), store); string(got) != "$3\r\n8.5\r\n" {
		t.Fatalf("ZSCORE response = %q", got)
	}
	if got := execute(testRESPCommand("ZSCORE", "scores", "missing"), store); string(got) != "$-1\r\n" {
		t.Fatalf("ZSCORE missing member response = %q", got)
	}
	if got := execute(testRESPCommand("ZSCORE", "missing-key", "Sam"), store); string(got) != "$-1\r\n" {
		t.Fatalf("ZSCORE missing key response = %q", got)
	}
	if got := execute(testRESPCommand("SET", "string", "value"), store); string(got) != "+OK\r\n" {
		t.Fatalf("SET response = %q", got)
	}
	if got := execute(testRESPCommand("ZSCORE", "string", "member"), store); string(got) != string(wrongTypeError()) {
		t.Fatalf("ZSCORE wrong type response = %q", got)
	}
	if got := execute(testRESPCommand("ZSCORE", "scores"), store); got[0] != '-' {
		t.Fatalf("ZSCORE invalid arity response = %q", got)
	}
}

func TestInfoCommand(t *testing.T) {
	previousRole := serverRole
	defer func() { serverRole = previousRole }()
	serverRole = "master"
	store := map[string]redisValue{"answer": {kind: stringKind, string: []byte("42")}}
	response := execute(testRESPCommand("INFO"), store)
	if !bytes.HasPrefix(response, []byte("$")) {
		t.Fatalf("INFO response is not a bulk string: %q", response)
	}
	for _, expected := range []string{
		"# Server\r\n",
		"redis_version:",
		"redis_mode:standalone\r\n",
		"# Keyspace\r\n",
		"db0:keys=1,expires=0,avg_ttl=0\r\n",
	} {
		if !bytes.Contains(response, []byte(expected)) {
			t.Errorf("INFO response does not contain %q: %q", expected, response)
		}
	}

	if got := execute(testRESPCommand("INFO", "keyspace"), store); !bytes.Contains(got, []byte("db0:keys=1")) {
		t.Fatalf("INFO keyspace response = %q", got)
	}
	replication := execute(testRESPCommand("INFO", "replication"), store)
	for _, expected := range []string{
		"# Replication\r\n",
		"role:master\r\n",
		"connected_slaves:0\r\n",
		"master_replid:" + masterReplicationID + "\r\n",
		"master_repl_offset:0\r\n",
		"repl_backlog_size:1048576\r\n",
	} {
		if !bytes.Contains(replication, []byte(expected)) {
			t.Errorf("replication INFO response does not contain %q: %q", expected, replication)
		}
	}
	if len(masterReplicationID) != 40 {
		t.Fatalf("master replication ID length = %d, want 40", len(masterReplicationID))
	}
	serverRole = "slave"
	if got := execute(testRESPCommand("INFO", "replication"), store); !bytes.Contains(got, []byte("role:slave\r\n")) {
		t.Fatalf("replica INFO response = %q", got)
	}
	if got := execute(testRESPCommand("INFO", "too", "many"), store); string(got) != "-ERR wrong number of arguments for 'info' command\r\n" {
		t.Fatalf("invalid INFO response = %q", got)
	}
}

func TestReplConfCommand(t *testing.T) {
	store := make(map[string]redisValue)
	for _, command := range [][]string{
		{"REPLCONF", "listening-port", "6380"},
		{"REPLCONF", "capa", "psync2"},
	} {
		if got := execute(testRESPCommand(command...), store); string(got) != "+OK\r\n" {
			t.Errorf("REPLCONF %v response = %q", command[1:], got)
		}
	}
	if got := execute(testRESPCommand("REPLCONF"), store); string(got) != "-ERR wrong number of arguments for 'replconf' command\r\n" {
		t.Fatalf("invalid REPLCONF response = %q", got)
	}
}

func TestPSyncCommand(t *testing.T) {
	store := make(map[string]redisValue)
	response := execute(testRESPCommand("PSYNC", "?", "-1"), store)
	want := append([]byte("+FULLRESYNC "+masterReplicationID+" 0\r\n"), rdbBulkString(emptyRDB)...)
	if !bytes.Equal(response, want) {
		t.Fatalf("PSYNC response = %q, want %q", response, want)
	}
	if !bytes.Contains(response, []byte("$"+strconv.Itoa(len(emptyRDB))+"\r\n")) {
		t.Fatalf("PSYNC response does not contain RDB length: %q", response)
	}
	if got := execute(testRESPCommand("PSYNC", "?"), store); string(got) != "-ERR wrong number of arguments for 'psync' command\r\n" {
		t.Fatalf("invalid PSYNC response = %q", got)
	}
}

func TestPropagateWriteCommand(t *testing.T) {
	masterSide, replicaSide := net.Pipe()
	defer masterSide.Close()
	defer replicaSide.Close()

	command := testRESPCommand("SET", "foo", "bar")
	received := make(chan []byte, 1)
	go func() {
		frame := make([]byte, len(command))
		if _, err := io.ReadFull(replicaSide, frame); err == nil {
			received <- frame
		}
	}()

	replicas := map[net.Conn]struct{}{masterSide: {}}
	propagateCommand(command, testRESPArguments("SET", "foo", "bar"), []byte("+OK\r\n"), replicas)

	select {
	case frame := <-received:
		if !bytes.Equal(frame, command) {
			t.Fatalf("propagated command = %q, want %q", frame, command)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for propagated command")
	}
}

func TestReplConfGetAck(t *testing.T) {
	arguments := testRESPArguments("REPLCONF", "GETACK", "*")
	if !isReplConfGetAck(arguments) {
		t.Fatal("GETACK command was not recognized")
	}
	want := testRESPCommand("REPLCONF", "ACK", "123")
	if got := replConfAckResponse(123); !bytes.Equal(got, want) {
		t.Fatalf("ACK response = %q, want %q", got, want)
	}
	if isReplConfGetAck(testRESPArguments("REPLCONF", "GETACK", "0")) {
		t.Fatal("GETACK with a non-wildcard offset was recognized")
	}
}

func TestReplConfAckParsing(t *testing.T) {
	if !isReplConfAck(testRESPArguments("REPLCONF", "ACK", "123")) {
		t.Fatal("valid REPLCONF ACK was not recognized")
	}
	if got := parseReplicaAckOffset(testRESPArguments("REPLCONF", "ACK", "123")); got != 123 {
		t.Fatalf("ACK offset = %d, want 123", got)
	}
	for _, arguments := range [][]byte{
		testRESPCommand("REPLCONF", "ACK", "-1"),
		testRESPCommand("REPLCONF", "ACK", "nope"),
	} {
		parsed, err := parseRESPCommand(arguments)
		if err != nil {
			t.Fatalf("parse ACK test command: %v", err)
		}
		if isReplConfAck(parsed) {
			t.Errorf("invalid ACK %q was recognized", arguments)
		}
	}
}

func TestReplicationWaitSendsGetAckAndCompletes(t *testing.T) {
	masterSide, replicaSide := net.Pipe()
	defer masterSide.Close()
	defer replicaSide.Close()

	getAck := encodeRESPCommand("REPLCONF", "GETACK", "*")
	received := make(chan []byte, 1)
	go func() {
		frame := make([]byte, len(getAck))
		if _, err := io.ReadFull(replicaSide, frame); err == nil {
			received <- frame
		}
	}()

	replicas := map[net.Conn]struct{}{masterSide: {}}
	offsets := map[net.Conn]int64{masterSide: 0}
	requests := make([]*replicationWaitRequest, 0, 1)
	response := make(chan []byte, 1)
	events := make(chan commandEvent, 1)
	var replicationOffset int64

	handleWait(
		commandEvent{response: response},
		testRESPArguments("WAIT", "1", "1000"),
		replicas,
		offsets,
		10,
		&requests,
		events,
		&replicationOffset,
	)

	select {
	case frame := <-received:
		if !bytes.Equal(frame, getAck) {
			t.Fatalf("GETACK frame = %q, want %q", frame, getAck)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for GETACK")
	}
	if got := replicationOffset; got != int64(len(getAck)) {
		t.Fatalf("replication offset after GETACK = %d, want %d", got, len(getAck))
	}

	offsets[masterSide] = 10
	wakeReplicationWaiters(&requests, replicas, offsets)
	select {
	case got := <-response:
		if string(got) != ":1\r\n" {
			t.Fatalf("WAIT response = %q, want :1", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for WAIT response")
	}
}

func TestAdvanceReplicaOffset(t *testing.T) {
	masterSide, replicaSide := net.Pipe()
	defer masterSide.Close()
	defer replicaSide.Close()

	offsets := make(map[net.Conn]int64)
	firstGetAck := testRESPCommand("REPLCONF", "GETACK", "*")
	ping := testRESPCommand("PING")
	secondGetAck := testRESPCommand("REPLCONF", "GETACK", "*")

	advanceReplicaOffset(offsets, masterSide, firstGetAck)
	if got := offsets[masterSide]; got != int64(len(firstGetAck)) {
		t.Fatalf("offset after first GETACK = %d, want %d", got, len(firstGetAck))
	}
	advanceReplicaOffset(offsets, masterSide, ping)
	advanceReplicaOffset(offsets, masterSide, secondGetAck)
	want := len(firstGetAck) + len(ping) + len(secondGetAck)
	if got := offsets[masterSide]; got != int64(want) {
		t.Fatalf("offset after three commands = %d, want %d", got, want)
	}
}

func TestWaitCommand(t *testing.T) {
	store := make(map[string]redisValue)
	if got := execute(testRESPCommand("WAIT", "0", "1000"), store); string(got) != ":0\r\n" {
		t.Fatalf("WAIT 0 response = %q", got)
	}
	if got := execute(testRESPCommand("WAIT", "2", "1"), store); string(got) != ":0\r\n" {
		t.Fatalf("WAIT with no tracked replicas response = %q", got)
	}
	if got := execute(testRESPCommand("WAIT", "9", "500"), store, 7); string(got) != ":7\r\n" {
		t.Fatalf("WAIT with connected replicas response = %q", got)
	}
	for _, command := range [][]string{
		{"WAIT"},
		{"WAIT", "nope", "1000"},
		{"WAIT", "0", "-1"},
	} {
		if got := execute(testRESPCommand(command...), store); got[0] != '-' {
			t.Errorf("invalid WAIT %v response = %q", command[1:], got)
		}
	}
}

func TestSetExpirationValidation(t *testing.T) {
	cases := []struct {
		name       string
		arguments  []string
		wantExpiry bool
		want       time.Duration
	}{
		{name: "without expiry", arguments: []string{"SET", "key", "value"}},
		{name: "seconds", arguments: []string{"SET", "key", "value", "EX", "2"}, wantExpiry: true, want: 2 * time.Second},
		{name: "milliseconds", arguments: []string{"SET", "key", "value", "PX", "25"}, wantExpiry: true, want: 25 * time.Millisecond},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			arguments := make([][]byte, len(testCase.arguments))
			for i, argument := range testCase.arguments {
				arguments[i] = []byte(argument)
			}
			got, hasExpiry, err := setExpiration(arguments)
			if err != nil {
				t.Fatalf("setExpiration: %v", err)
			}
			if got != testCase.want || hasExpiry != testCase.wantExpiry {
				t.Fatalf("setExpiration = (%s, %t), want (%s, %t)", got, hasExpiry, testCase.want, testCase.wantExpiry)
			}
		})
	}

	for _, arguments := range [][]string{
		{"SET", "key", "value", "EX"},
		{"SET", "key", "value", "SECONDS", "1"},
		{"SET", "key", "value", "PX", "0"},
	} {
		encoded := make([][]byte, len(arguments))
		for i, argument := range arguments {
			encoded[i] = []byte(argument)
		}
		if _, _, err := setExpiration(encoded); err == nil {
			t.Errorf("setExpiration(%v) returned nil error", arguments)
		}
	}
}

func TestApplyExpirationDoesNotDeleteReplacedValue(t *testing.T) {
	store := map[string]redisValue{
		"key": {kind: stringKind, string: []byte("new")},
	}
	versions := map[string]uint64{"key": 2}

	applyExpiration(&expirationEvent{key: "key", version: 1}, store, versions)
	if got := string(store["key"].string); got != "new" {
		t.Fatalf("stale expiration deleted replacement value %q", got)
	}
	if versions["key"] != 2 {
		t.Fatalf("stale expiration changed version to %d", versions["key"])
	}

	applyExpiration(&expirationEvent{key: "key", version: 2}, store, versions)
	if _, ok := store["key"]; ok {
		t.Fatal("current expiration did not delete key")
	}
	if versions["key"] != 3 {
		t.Fatalf("current expiration did not advance version: %d", versions["key"])
	}
}

func TestListCommands(t *testing.T) {
	store := make(map[string]redisValue)
	if got := execute(testRESPCommand("RPUSH", "list", "one", "two"), store); string(got) != ":2\r\n" {
		t.Fatalf("RPUSH response = %q", got)
	}
	if got := execute(testRESPCommand("LPUSH", "list", "zero"), store); string(got) != ":3\r\n" {
		t.Fatalf("LPUSH response = %q", got)
	}
	if got := execute(testRESPCommand("LRANGE", "list", "0", "-1"), store); string(got) != "*3\r\n$4\r\nzero\r\n$3\r\none\r\n$3\r\ntwo\r\n" {
		t.Fatalf("LRANGE response = %q", got)
	}
	if got := execute(testRESPCommand("LPOP", "list", "2"), store); string(got) != "*2\r\n$4\r\nzero\r\n$3\r\none\r\n" {
		t.Fatalf("LPOP response = %q", got)
	}
	if got := execute(testRESPCommand("LLEN", "list"), store); string(got) != ":1\r\n" {
		t.Fatalf("LLEN response = %q", got)
	}
}

func TestStreamCommands(t *testing.T) {
	store := make(map[string]redisValue)
	if got := execute(testRESPCommand("XADD", "events", "1-0", "temperature", "36"), store); string(got) != "$3\r\n1-0\r\n" {
		t.Fatalf("XADD response = %q", got)
	}
	if got := execute(testRESPCommand("XADD", "events", "2-*", "humidity", "95"), store); string(got) != "$3\r\n2-0\r\n" {
		t.Fatalf("XADD generated sequence response = %q", got)
	}
	if got := execute(testRESPCommand("XRANGE", "events", "-", "+"), store); string(got) != "*2\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$11\r\ntemperature\r\n$2\r\n36\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$8\r\nhumidity\r\n$2\r\n95\r\n" {
		t.Fatalf("XRANGE response = %q", got)
	}
	if got := execute(testRESPCommand("XREAD", "STREAMS", "events", "1-0"), store); !bytes.Contains(got, []byte("2-0")) {
		t.Fatalf("XREAD response %q does not contain second entry", got)
	}
}

func TestWatchAbortsChangedTransaction(t *testing.T) {
	store := make(map[string]redisValue)
	versions := make(map[string]uint64)
	client := &clientState{}

	if got := watchKeys(testRESPArguments("WATCH", "key"), client, versions); string(got) != "+OK\r\n" {
		t.Fatalf("WATCH response = %q", got)
	}
	if got := beginTransaction(client); string(got) != "+OK\r\n" {
		t.Fatalf("MULTI response = %q", got)
	}
	client.queue = append(client.queue, testRESPCommand("SET", "key", "client-a"))
	otherResponse := execute(testRESPCommand("SET", "key", "client-b"), store)
	touchModifiedKeys(testRESPArguments("SET", "key", "client-b"), otherResponse, versions)

	if got := executeTransaction(client, store, versions, nil, nil, nil); string(got) != "*-1\r\n" {
		t.Fatalf("EXEC response = %q", got)
	}
}

func testRESPArguments(arguments ...string) [][]byte {
	result := make([][]byte, len(arguments))
	for i, argument := range arguments {
		result[i] = []byte(argument)
	}
	return result
}
