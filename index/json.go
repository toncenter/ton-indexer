package index

import (
	"fmt"
	"strings"
)

// Methods
func (s *ShardId) MarshalJSON() ([]byte, error) {
	res := fmt.Sprintf("\"%X\"", uint64(*s))
	return []byte(res), nil
}

func (a *AccountAddress) MarshalJSON() ([]byte, error) {
	return []byte("\"" + strings.Trim(string(*a), " ") + "\""), nil
}

func (h *HexInt) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"0x%X\"", *h)), nil
}
