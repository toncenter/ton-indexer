package index

/*
#cgo CXXFLAGS: -I${SRCDIR}/../../ton-index-worker/ton-marker/src
#cgo LDFLAGS: -L${SRCDIR}/../../build/ton-index-worker/ton-marker -lton-marker-core -lton-marker -Wl,-rpath,${SRCDIR}/../../build/ton-index-worker/ton-marker

#include "wrapper.h"
#include <stdlib.h>
*/
import "C"
import (
	"encoding/json"
	"errors"
	"fmt"
	"unsafe"
)

// check if library is loaded and initialized
var isLibraryInitialized bool

// just a simple check
func init() {
	fmt.Println("initializing ton-marker library...")
	result := C.ton_marker_decode_opcode(C.uint(0))
	if result != nil {
		isLibraryInitialized = true
		C.free(unsafe.Pointer(result))
		fmt.Println("ton-marker library initialized successfully")
	} else {
		fmt.Println("warning: ton-marker library initialization failed")
	}
}

// batch request to a c++ library
func MarkerRequest(opcodesList []uint32, bocBase64List []string, methodIdsList [][]uint32) ([]string, []string, []string, error) {
	if !isLibraryInitialized {
		return nil, nil, nil, errors.New("ton-marker library is not initialized")
	}
	// allocate memory for opcodes array
	opcodesPtr := C.malloc(C.size_t(len(opcodesList) * int(unsafe.Sizeof(C.uint(0)))))
	if opcodesPtr == nil {
		panic("Failed to allocate memory for opcodes")
	}
	defer C.free(opcodesPtr)

	// copy opcodes
	opcodesSlice := unsafe.Slice((*C.uint)(opcodesPtr), len(opcodesList))
	for i, op := range opcodesList {
		opcodesSlice[i] = C.uint(op)
	}

	// allocate memory for boc strings and create array of pointers
	bocStrPtrs := make([]*C.char, len(bocBase64List))
	bocListPtr := C.malloc(C.size_t(len(bocBase64List) * int(unsafe.Sizeof(uintptr(0)))))
	if bocListPtr == nil {
		panic("Failed to allocate memory for boc list")
	}
	defer C.free(bocListPtr)

	// convert strings and set up pointers
	bocPtrSlice := unsafe.Slice((**C.char)(bocListPtr), len(bocBase64List))
	for i, boc := range bocBase64List {
		bocStrPtrs[i] = C.CString(boc)
		bocPtrSlice[i] = bocStrPtrs[i]
	}
	// run cleanup of strings after return
	defer func() {
		for _, ptr := range bocStrPtrs {
			C.free(unsafe.Pointer(ptr))
		}
	}()

	// allocate memory for method ids arrays
	methodIdsArrayPtr := C.malloc(C.size_t(len(methodIdsList) * int(unsafe.Sizeof(uintptr(0)))))
	if methodIdsArrayPtr == nil {
		panic("Failed to allocate memory for method ids array")
	}
	defer C.free(methodIdsArrayPtr)

	methodCountsPtr := C.malloc(C.size_t(len(methodIdsList) * int(unsafe.Sizeof(C.int(0)))))
	if methodCountsPtr == nil {
		panic("Failed to allocate memory for method counts")
	}
	defer C.free(methodCountsPtr)

	// allocate and fill method ids arrays
	methodIdsPtrs := make([]unsafe.Pointer, len(methodIdsList))
	methodIdsSlice := unsafe.Slice((**C.uint)(methodIdsArrayPtr), len(methodIdsList))
	methodCountsSlice := unsafe.Slice((*C.int)(methodCountsPtr), len(methodIdsList))

	for i, methods := range methodIdsList {
		// allocate memory for this method ids array
		methodIdsPtr := C.malloc(C.size_t(len(methods) * int(unsafe.Sizeof(C.uint(0)))))
		if methodIdsPtr == nil {
			panic("Failed to allocate memory for method ids")
		}
		methodIdsPtrs[i] = methodIdsPtr // save for cleanup

		// copy method ids
		methodsSlice := unsafe.Slice((*C.uint)(methodIdsPtr), len(methods))
		for j, id := range methods {
			methodsSlice[j] = C.uint(id)
		}

		// set up pointers and counts
		methodIdsSlice[i] = (*C.uint)(methodIdsPtr)
		methodCountsSlice[i] = C.int(len(methods))
	}
	// defer cleanup of method ids arrays
	defer func() {
		for _, ptr := range methodIdsPtrs {
			C.free(ptr)
		}
	}()

	// create batch request
	request := C.struct_TonMarkerBatchRequest{
		opcodes:         (*C.uint)(opcodesPtr),
		opcode_count:    C.int(len(opcodesList)),
		boc_base64_list: (**C.char)(bocListPtr),
		boc_count:       C.int(len(bocBase64List)),
		method_ids:      (**C.uint)(methodIdsArrayPtr),
		method_counts:   (*C.int)(methodCountsPtr),
		interface_count: C.int(len(methodIdsList)),
	}

	// call batch function
	response := C.ton_marker_process_batch(&request)
	if response == nil {
		return nil, nil, nil, errors.New("ton_marker_process_batch failed or returned nil")
	}

	defer func() {
		if response != nil {
			C.ton_marker_free_batch_response(response)
		}
	}()

	// get opcode results
	var opcodeResults []string = make([]string, 0)
	if response.opcode_count > 0 && response.opcode_results != nil {
		results := unsafe.Slice(response.opcode_results, response.opcode_count)
		for _, res := range results {
			opcodeResults = append(opcodeResults, C.GoString(res))
		}
	}

	// get boc results
	var bocResults []string = make([]string, 0)
	if response.boc_count > 0 && response.boc_results != nil {
		results := unsafe.Slice(response.boc_results, response.boc_count)
		for _, res := range results {
			bocResults = append(bocResults, C.GoString(res))
		}
	}

	// get interface results
	var interfaceResults []string = make([]string, 0)
	if response.interface_count > 0 && response.interface_results != nil {
		results := unsafe.Slice(response.interface_results, response.interface_count)
		for _, res := range results {
			interfaceResults = append(interfaceResults, C.GoString(res))
		}
	}
	return opcodeResults, bocResults, interfaceResults, nil
}

type messagesRefs struct {
	opcodeRefs map[uint32][]*string         // key: opcode, value: list of pointers where to write decoded opcode
	bodyRefs   map[string][]*DecodedContent // key: body, value: list of pointers where to write decoded body
	opcodes    []uint32
	bodies     []string
}

func MarkMessages(messages []Message) error {
	refs := collectMessagesRefs(messages)
	return markWithRefs(refs)
}

func MarkTransactions(transactions []*Transaction) error {
	refs := collectTransactionRefs(transactions)
	return markWithRefs(refs)
}

func collectMessagesRefs(messages []Message) *messagesRefs {
	refs := &messagesRefs{
		opcodeRefs: make(map[uint32][]*string),
		bodyRefs:   make(map[string][]*DecodedContent),
		opcodes:    make([]uint32, 0, len(messages)),
		bodies:     make([]string, 0, len(messages)),
	}
	for i := range messages {
		collectSingleMessageRefs(&messages[i], refs)
	}
	return refs
}

// collects references from a single message
func collectSingleMessageRefs(msg *Message, refs *messagesRefs) {
	if msg == nil {
		return
	}
	// collect opcodes
	if msg.Opcode != nil {
		if msg.DecodedOpcode == nil {
			msg.DecodedOpcode = new(string)
		}
		refs.opcodeRefs[uint32(*msg.Opcode)] = append(refs.opcodeRefs[uint32(*msg.Opcode)], msg.DecodedOpcode)
		refs.opcodes = append(refs.opcodes, uint32(*msg.Opcode))
	}
	// collect message bodies
	if msg.MessageContent != nil && msg.MessageContent.Body != nil {
		if msg.MessageContent.Decoded == nil {
			msg.MessageContent.Decoded = new(DecodedContent)
		}
		refs.bodyRefs[*msg.MessageContent.Body] = append(refs.bodyRefs[*msg.MessageContent.Body], msg.MessageContent.Decoded)
		refs.bodies = append(refs.bodies, *msg.MessageContent.Body)
	}
}

// collects all references from messages in transactions
func collectTransactionRefs(transactions []*Transaction) *messagesRefs {
	refs := &messagesRefs{
		opcodeRefs: make(map[uint32][]*string),
		bodyRefs:   make(map[string][]*DecodedContent),
	}

	for _, tx := range transactions {
		if tx == nil {
			continue
		}
		// process InMsg
		if tx.InMsg != nil {
			collectSingleMessageRefs(tx.InMsg, refs)
		}
		// process OutMsgs
		for _, msg := range tx.OutMsgs {
			collectSingleMessageRefs(msg, refs)
		}
	}
	return refs
}

func markWithRefs(refs *messagesRefs) error {
	opcodes := refs.opcodes
	bodies := refs.bodies

	if len(opcodes) == 0 && len(bodies) == 0 {
		return nil
	}

	decodedOpcodes, decodedBodies, _, err := MarkerRequest(opcodes, bodies, nil)
	if err != nil {
		return err
	}

	// fill in opcode results
	for i, opcode := range opcodes {
		if decodedValue := decodedOpcodes[i]; decodedValue != "unknown" {
			for _, ref := range refs.opcodeRefs[opcode] {
				*ref = decodedValue
			}
		}
	}

	// fill in message body results
	for i, body := range bodies {
		if decodedValue := decodedBodies[i]; decodedValue != "unknown" {
			for _, ref := range refs.bodyRefs[body] {
				var tmpResult map[string]interface{}
				if err := json.Unmarshal([]byte(decodedValue), &tmpResult); err != nil {
					return fmt.Errorf("failed to decode message body: %w", err)
				}
				for msgType, msgData := range tmpResult {
					ref.Type = msgType
					ref.Data = msgData
					// there's only one key in the map
					break
				}
			}
		}
	}

	return nil
}
