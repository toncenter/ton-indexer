package models

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestParseHashBytesAcceptsHexAndBase64(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "lowercase hex",
			input:    strings.Repeat("ab", 32),
			expected: strings.Repeat("AB", 32),
		},
		{
			name:     "uppercase hex prefix",
			input:    "0X" + strings.Repeat("cd", 32),
			expected: strings.Repeat("CD", 32),
		},
		{
			name:     "standard base64",
			input:    "//////////////////////////////////////////8=",
			expected: strings.Repeat("FF", 32),
		},
		{
			name:     "padded base64url",
			input:    "__________________________________________8=",
			expected: strings.Repeat("FF", 32),
		},
		{
			name:     "raw base64url",
			input:    "__________________________________________8",
			expected: strings.Repeat("FF", 32),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseHashBytes(tt.input)
			if err != nil {
				t.Fatalf("ParseHashBytes(%q) returned error: %v", tt.input, err)
			}
			gotHex := strings.ToUpper(hex.EncodeToString(got))
			if gotHex != tt.expected {
				t.Fatalf("ParseHashBytes(%q) = %q, want %q", tt.input, gotHex, tt.expected)
			}
		})
	}
}

func TestParseHashBytesRejectsInvalidValues(t *testing.T) {
	for _, input := range []string{"", "not-a-hash", strings.Repeat("a", 63), strings.Repeat("A", 45)} {
		t.Run(input, func(t *testing.T) {
			if got, err := ParseHashBytes(input); err == nil {
				t.Fatalf("ParseHashBytes(%q) = %q, want error", input, got)
			}
		})
	}
}

func TestHashConverterAcceptsRawBase64URL(t *testing.T) {
	got := HashConverter("__________________________________________8")
	if !got.IsValid() {
		t.Fatal("HashConverter returned invalid value")
	}
	hash, ok := got.Interface().(HashType)
	if !ok {
		t.Fatalf("HashConverter returned %T", got.Interface())
	}
	if hash.String() != "//////////////////////////////////////////8=" {
		t.Fatalf("HashConverter returned %q", hash.String())
	}
}

func TestOpcodeTypeConverterUsesSignedDatabaseRepresentation(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		expected   OpcodeType
		expectedDB bool
		expectedJS string
	}{
		{
			name:       "positive hex",
			input:      "0x7fffffff",
			expected:   OpcodeType(2147483647),
			expectedDB: false,
			expectedJS: "0x7fffffff",
		},
		{
			name:       "high bit hex",
			input:      "0x80000000",
			expected:   signedOpcode(0x80000000),
			expectedDB: true,
			expectedJS: "0x80000000",
		},
		{
			name:       "reported opcode",
			input:      "0x94826557",
			expected:   signedOpcode(0x94826557),
			expectedDB: true,
			expectedJS: "0x94826557",
		},
		{
			name:       "uppercase prefix",
			input:      "0XF14B54F3",
			expected:   signedOpcode(0xf14b54f3),
			expectedDB: true,
			expectedJS: "0xf14b54f3",
		},
		{
			name:       "signed decimal",
			input:      "-1",
			expected:   OpcodeType(-1),
			expectedDB: true,
			expectedJS: "0xffffffff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := OpcodeTypeConverter(tt.input)
			if !got.IsValid() {
				t.Fatalf("OpcodeTypeConverter(%q) returned invalid value", tt.input)
			}
			opcode, ok := got.Interface().(OpcodeType)
			if !ok {
				t.Fatalf("OpcodeTypeConverter(%q) returned %T", tt.input, got.Interface())
			}
			if opcode != tt.expected {
				t.Fatalf("OpcodeTypeConverter(%q) = %d, want %d", tt.input, opcode, tt.expected)
			}
			if (opcode < 0) != tt.expectedDB {
				t.Fatalf("OpcodeTypeConverter(%q) negative = %t, want %t", tt.input, opcode < 0, tt.expectedDB)
			}
			if opcode.String() != tt.expectedJS {
				t.Fatalf("OpcodeType.String() = %q, want %q", opcode.String(), tt.expectedJS)
			}
		})
	}
}

func TestOpcodeTypeConverterRejectsOutOfRangeValues(t *testing.T) {
	tests := []string{"0x100000000", "4294967296", "-2147483649", "not-an-opcode"}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			if got := OpcodeTypeConverter(input); got.IsValid() {
				t.Fatalf("OpcodeTypeConverter(%q) returned valid value %v", input, got.Interface())
			}
		})
	}
}

func signedOpcode(v uint32) OpcodeType {
	return OpcodeType(int32(v))
}
