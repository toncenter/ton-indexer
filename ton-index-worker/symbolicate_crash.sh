#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <binary_path> <stack_trace_file>"
    exit 1
fi

BINARY_PATH="$1"
STACK_TRACE_FILE="$2"

if [ ! -f "$BINARY_PATH" ]; then
    echo "Error: Binary file not found at $BINARY_PATH"
    exit 1
fi

if [ ! -f "$STACK_TRACE_FILE" ]; then
    echo "Error: Stack trace file not found at $STACK_TRACE_FILE"
    exit 1
fi

symbolicate_address() {
    ADDRESS="$1"
    gdb -batch -ex "file $BINARY_PATH" -ex "info symbol $ADDRESS"
}

while IFS= read -r line; do
    if [[ $line =~ \(\+0x([0-9a-fA-F]+)\) ]]; then
        symbolicate_address "0x${BASH_REMATCH[1]}"
    fi
done < "$STACK_TRACE_FILE"
