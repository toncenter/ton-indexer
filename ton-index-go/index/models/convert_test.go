package models

import "testing"

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
