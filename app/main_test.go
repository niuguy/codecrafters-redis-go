package main

import (
	"bytes"
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

func TestInfoCommand(t *testing.T) {
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
	if got := execute(testRESPCommand("INFO", "too", "many"), store); string(got) != "-ERR wrong number of arguments for 'info' command\r\n" {
		t.Fatalf("invalid INFO response = %q", got)
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
