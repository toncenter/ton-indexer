package index

import (
	"encoding/base64"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// ParseCommentFromPayload extracts a comment from a base64-encoded payload string.
// Supports two types of comments:
//   - Plain text comments (prefix 0x00000000)
//   - Encrypted comments (prefix 0x2167da4b)
//
// The function returns the extracted comment as a string pointer (nil if no comment found),
// a boolean indicating whether the comment is encrypted, and any error encountered.
// If the payload is empty or doesn't contain a recognized comment format, it returns nil, false, nil.
func ParseCommentFromPayload(payload string) (*string, bool, error) {
	if payload == "" {
		return nil, false, nil
	}
	payloadBytes, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, false, err
	}

	payloadCell, err := cell.FromBOC(payloadBytes)
	if err != nil {
		return nil, false, err
	}

	slice := payloadCell.BeginParse()
	sumType, err := slice.LoadUInt(32)
	if err != nil {
		return nil, false, nil
	}

	var commentStr string
	var isEncrypted bool

	switch sumType {
	case 0x2167da4b: // Encrypted comment
		commentBytes, err := slice.LoadBinarySnake()
		if err != nil {
			return nil, false, nil
		}
		commentStr = base64.StdEncoding.EncodeToString(commentBytes)
		isEncrypted = true

	case 0x00000000: // Plain text comment
		commentStr, err = slice.LoadStringSnake()
		if err != nil {
			return nil, false, nil
		}
		isEncrypted = false

	default:
		// Not a comment
		return nil, false, nil
	}

	return &commentStr, isEncrypted, nil
}
