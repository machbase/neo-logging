package logproto

import (
	"encoding/json"
	"strconv"
)

func (e *Entry) MarshalJSON() ([]byte, error) {
	dt := []string{strconv.FormatUint(e.Timestamp, 10), e.Line}
	return json.Marshal(dt)
}
