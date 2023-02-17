package logging

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/golang/snappy"
	"github.com/machbase/neo-logging/logproto"
	"google.golang.org/protobuf/proto"
)

type LogVaultWriter struct {
	serverAddress string
	labelString   string
	labels        map[string]string
	bufferSize    int
	bufferLimit   int
	bufferTimeout time.Duration
	entryEncoder  string
	entryCh       chan *logproto.Entry
}

func NewLogVaultWriter(serverAddress string, labels map[string]string) *LogVaultWriter {
	lbl := make([]string, 0)
	for k, v := range labels {
		lbl = append(lbl, fmt.Sprintf("%s=\"%s\"", k, v))
	}
	lblStr := "{" + strings.Join(lbl, ", ") + "}"

	bufferSize := 100
	bufferLimit := int(float32(bufferSize) * 0.9)

	return &LogVaultWriter{
		serverAddress: serverAddress,
		labels:        labels,
		labelString:   lblStr,
		bufferSize:    bufferSize,
		bufferLimit:   bufferLimit,
		bufferTimeout: 10 * time.Millisecond,
		entryCh:       make(chan *logproto.Entry, bufferSize),
		entryEncoder:  "json",
	}
}

func (lvw *LogVaultWriter) Start() {
	if lvw.entryEncoder == "protobuf" {
		go lvw.loop_protobuf()
	} else {
		go lvw.loop()
	}
}

func (lvw *LogVaultWriter) Stop() {
	close(lvw.entryCh)
}

type PushJson struct {
	Streams []StreamJson `json:"streams"`
}

type StreamJson struct {
	Labels map[string]string `json:"stream"`
	Values []*logproto.Entry `json:"values"`
}

func (lvw *LogVaultWriter) loop() {
	for h := range lvw.entryCh {
		count := len(lvw.entryCh)
		entries := make([]*logproto.Entry, count+1)
		entries[0] = h
		for i := 0; i < count; i++ {
			entry := <-lvw.entryCh
			entries[i+1] = entry
		}

		pushReq := &PushJson{
			Streams: []StreamJson{
				{
					Labels: lvw.labels,
					Values: entries,
				},
			},
		}

		data, err := json.Marshal(pushReq)
		if err != nil {
			fmt.Fprintln(os.Stderr, "log vaulter marshal", err.Error())
			continue
		}

		req, err := http.NewRequest(http.MethodPost, lvw.serverAddress, bytes.NewBuffer(data))
		if err != nil {
			fmt.Fprintln(os.Stderr, "log vaulter new request", err.Error())
			continue
		}
		req.Header.Set("Content-type", `application/json`)
		rsp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, "log vaulter post", err.Error())
			continue
		}
		if rsp.StatusCode != http.StatusNoContent && rsp.StatusCode != http.StatusOK {
			fmt.Fprintln(os.Stderr, "log vaulter loki", rsp.Status)
			// fmt.Fprintf(os.Stderr, "%s\n", string(data))
			continue
		}
	}
}

func (lvw *LogVaultWriter) loop_protobuf() {
	for h := range lvw.entryCh {
		count := len(lvw.entryCh)
		entries := make([]*logproto.Entry, count+1)
		entries[0] = h
		for i := 0; i < count; i++ {
			entry := <-lvw.entryCh
			entries[i+1] = entry
		}

		stream := &logproto.Stream{
			Labels:  lvw.labelString,
			Entries: entries,
		}
		pushReq := &logproto.PushRequest{
			Streams: []*logproto.Stream{stream},
		}

		data, err := proto.Marshal(pushReq)
		//data, err := pushReq.Marshal()
		if err != nil {
			fmt.Fprintln(os.Stderr, "log vaulter marshal", err.Error())
			continue
		}

		pushReqBody := snappy.Encode(nil, data)

		req, err := http.NewRequest(http.MethodPost, lvw.serverAddress, bytes.NewBuffer(pushReqBody))
		if err != nil {
			fmt.Fprintln(os.Stderr, "log vaulter new request", err.Error())
			continue
		}
		req.Header.Set("Content-type", `application/x-protobuf`)
		rsp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, "log vaulter post", err.Error())
			continue
		}
		if rsp.StatusCode != http.StatusNoContent && rsp.StatusCode != http.StatusOK {
			fmt.Fprintln(os.Stderr, "log vaulter loki", rsp.Status)
			continue
		}
	}
}

func (lvw *LogVaultWriter) Write(timestamp time.Time, line string) {
	// channel에 적체된 log entry가 buffer limit을 넘을 경우
	// 더 이상 enqueue를 하지 않는다.
	if len(lvw.entryCh) >= lvw.bufferLimit {
		// drop a line
		return
	}

	entry := &logproto.Entry{
		Timestamp: uint64(timestamp.UnixNano()),
		Line:      line,
	}

	select {
	case lvw.entryCh <- entry:
	case <-time.After(lvw.bufferTimeout):
		// timeout and drop an entry
	}
}
