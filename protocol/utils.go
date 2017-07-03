package protocol

import (
	"bytes"
	"errors"
	"fmt"
)

// Split the message payload
var NullChar = []byte("\x00\x01")

// ParseCommand payload to extract msgID cmd and data
func ParseCommand(payload []byte) (msgID []byte, cmd Command, data []byte) {
	parts := bytes.SplitN(payload, NullChar, 3)
	partSize := len(parts)
	var err = fmt.Errorf("InvalID %v\n", payload)
	if partSize <= 1 {
		panic(err)
	}

	msgID = parts[0]
	cmd = UNKNOWN

	if partSize == 2 {
		if len(parts[1]) != 1 {
			data = parts[1]
		} else {
			cmd = Command(parts[1][0])
		}
	}
	if partSize == 3 {
		if len(parts[1]) != 1 {
			data = bytes.Join([][]byte{parts[1], parts[2]}, NullChar)
		} else {
			cmd = Command(parts[1][0])
			data = parts[2]
		}

	}
	return
}

// MakeHeader In order to handle framing in Send/Recieve, as these give frame
// boundaries we use a very simple 4 bytes header.
func MakeHeader(data []byte) ([]byte, error) {
	header := make([]byte, 4)

	length := uint32(len(data))

	if length > 0x7fffffff {
		return nil, errors.New("Data too large")
	}

	header[0] = byte((length >> 24) & 0xff)
	header[1] = byte((length >> 16) & 0xff)
	header[2] = byte((length >> 8) & 0xff)
	header[3] = byte((length >> 0) & 0xff)

	return header, nil
}

// ParseHeader extract the pack header by MakeHeader
func ParseHeader(header []byte) uint32 {
	length := uint32(header[0])<<24 | uint32(header[1])<<16 | uint32(header[2])<<8 | uint32(header[3])
	length = length & ^uint32(0x80000000)

	return length
}
