package index

/*
#cgo CPPFLAGS: -I${SRCDIR}/../../ton-index-worker/ton-marker/src
#cgo LDFLAGS: -L${SRCDIR}/../../build/ton-index-worker/ton-marker -lton-marker-core -lton-marker -Wl,-rpath,${SRCDIR}/../../build/ton-index-worker/ton-marker

#include "wrapper.h"
#include <stdlib.h>
*/
import "C"
import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
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
		C.ton_marker_free_string(result)
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
	var opcodesPtr unsafe.Pointer
	if len(opcodesList) > 0 {
		opcodesPtr = C.malloc(C.size_t(len(opcodesList) * int(unsafe.Sizeof(C.uint(0)))))
		if opcodesPtr == nil {
			panic("Failed to allocate memory for opcodes")
		}
		defer C.free(opcodesPtr)

		// copy opcodes
		opcodesSlice := unsafe.Slice((*C.uint)(opcodesPtr), len(opcodesList))
		for i, op := range opcodesList {
			opcodesSlice[i] = C.uint(op)
		}
	}

	// allocate memory for boc strings and create array of pointers
	bocStrPtrs := make([]*C.char, len(bocBase64List))
	var bocListPtr unsafe.Pointer
	if len(bocBase64List) > 0 {
		bocListPtr = C.malloc(C.size_t(len(bocBase64List) * int(unsafe.Sizeof(uintptr(0)))))
		if bocListPtr == nil {
			panic("Failed to allocate memory for boc list")
		}
		defer C.free(bocListPtr)
	}

	// convert strings and set up pointers
	if len(bocBase64List) > 0 {
		bocPtrSlice := unsafe.Slice((**C.char)(bocListPtr), len(bocBase64List))
		for i, boc := range bocBase64List {
			bocStrPtrs[i] = C.CString(boc)
			bocPtrSlice[i] = bocStrPtrs[i]
		}
	}
	// run cleanup of strings after return
	defer func() {
		for _, ptr := range bocStrPtrs {
			C.free(unsafe.Pointer(ptr))
		}
	}()

	// allocate memory for method ids arrays
	var methodIdsArrayPtr unsafe.Pointer
	var methodCountsPtr unsafe.Pointer
	var methodIdsSlice []*C.uint
	var methodCountsSlice []C.int

	if len(methodIdsList) > 0 {
		methodIdsArrayPtr = C.malloc(C.size_t(len(methodIdsList) * int(unsafe.Sizeof(uintptr(0)))))
		if methodIdsArrayPtr == nil {
			panic("Failed to allocate memory for method ids array")
		}
		defer C.free(methodIdsArrayPtr)

		methodCountsPtr = C.malloc(C.size_t(len(methodIdsList) * int(unsafe.Sizeof(C.int(0)))))
		if methodCountsPtr == nil {
			panic("Failed to allocate memory for method counts")
		}
		defer C.free(methodCountsPtr)

		methodIdsSlice = unsafe.Slice((**C.uint)(methodIdsArrayPtr), len(methodIdsList))
		methodCountsSlice = unsafe.Slice((*C.int)(methodCountsPtr), len(methodIdsList))
	}

	// allocate and fill method ids arrays
	methodIdsPtrs := make([]unsafe.Pointer, len(methodIdsList))

	if len(methodIdsList) > 0 {
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
			if res != nil {
				opcodeResults = append(opcodeResults, C.GoString(res))
			} else {
				opcodeResults = append(opcodeResults, "")
			}
		}
	}

	// get boc results
	var bocResults []string = make([]string, 0)
	if response.boc_count > 0 && response.boc_results != nil {
		results := unsafe.Slice(response.boc_results, response.boc_count)
		for _, res := range results {
			if res != nil {
				bocResults = append(bocResults, C.GoString(res))
			} else {
				bocResults = append(bocResults, "")
			}
		}
	}

	// get interface results
	var interfaceResults []string = make([]string, 0)
	if response.interface_count > 0 && response.interface_results != nil {
		results := unsafe.Slice(response.interface_results, response.interface_count)
		for _, res := range results {
			if res != nil {
				interfaceResults = append(interfaceResults, C.GoString(res))
			} else {
				interfaceResults = append(interfaceResults, "")
			}
		}
	}
	return opcodeResults, bocResults, interfaceResults, nil
}

type messagesRefs struct {
	opcodeRefs map[uint32][]*string            // key: opcode, value: list of pointers where to write decoded opcode
	bodyRefs   map[string][]*(*DecodedContent) // key: body, value: list of pointers where to write decoded body
}

func MarkMessages(messages []Message) error {
	refs := collectMessagesRefs(messages)
	return markWithRefs(refs)
}

func MarkJettonTransfers(transfers []JettonTransfer) error {
	refs := collectJettonTransfersRefs(transfers)
	return markWithRefs(refs)
}

func MarkJettonBurns(burns []JettonBurn) error {
	refs := collectJettonBurnsRefs(burns)
	return markWithRefs(refs)
}

func MarkNFTTransfers(transfers []NFTTransfer) error {
	refs := collectNFTTransfersRefs(transfers)
	return markWithRefs(refs)
}

func collectMessagesRefs(messages []Message) *messagesRefs {
	refs := &messagesRefs{
		opcodeRefs: make(map[uint32][]*string),
		bodyRefs:   make(map[string][]*(*DecodedContent)),
	}
	for i := range messages {
		collectSingleMessageRefs(&messages[i], refs)
	}
	return refs
}

func collectJettonTransfersRefs(transfers []JettonTransfer) *messagesRefs {
	refs := &messagesRefs{
		opcodeRefs: make(map[uint32][]*string),
		bodyRefs:   make(map[string][]*(*DecodedContent)),
	}
	for i := range transfers {
		transfer := &transfers[i]
		customPayload := transfer.CustomPayload
		if customPayload != nil && transfer.DecodedCustomPayload == nil { // skip if already decoded (as text comment)
			refs.bodyRefs[*customPayload] = append(refs.bodyRefs[*customPayload], &transfer.DecodedCustomPayload)
		}
		forwardPayload := transfer.ForwardPayload
		if forwardPayload != nil && transfer.DecodedForwardPayload == nil {
			refs.bodyRefs[*forwardPayload] = append(refs.bodyRefs[*forwardPayload], &transfer.DecodedForwardPayload)
		}
	}
	return refs
}

func collectJettonBurnsRefs(burns []JettonBurn) *messagesRefs {
	refs := &messagesRefs{
		opcodeRefs: make(map[uint32][]*string),
		bodyRefs:   make(map[string][]*(*DecodedContent)),
	}
	for i := range burns {
		burn := &burns[i]
		customPayload := burn.CustomPayload
		if customPayload != nil && burn.DecodedCustomPayload == nil {
			refs.bodyRefs[*customPayload] = append(refs.bodyRefs[*customPayload], &burn.DecodedCustomPayload)
		}
	}
	return refs
}

func collectNFTTransfersRefs(transfers []NFTTransfer) *messagesRefs {
	refs := &messagesRefs{
		opcodeRefs: make(map[uint32][]*string),
		bodyRefs:   make(map[string][]*(*DecodedContent)),
	}
	for i := range transfers {
		transfer := &transfers[i]
		customPayload := transfer.CustomPayload
		if customPayload != nil && transfer.DecodedCustomPayload == nil {
			refs.bodyRefs[*customPayload] = append(refs.bodyRefs[*customPayload], &transfer.DecodedCustomPayload)
		}
		forwardPayload := transfer.ForwardPayload
		if forwardPayload != nil && transfer.DecodedForwardPayload == nil {
			refs.bodyRefs[*forwardPayload] = append(refs.bodyRefs[*forwardPayload], &transfer.DecodedForwardPayload)
		}
	}
	return refs
}

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
	}
	// collect message bodies
	if msg.MessageContent != nil && msg.MessageContent.Body != nil && msg.MessageContent.Decoded == nil {
		refs.bodyRefs[*msg.MessageContent.Body] = append(refs.bodyRefs[*msg.MessageContent.Body], &msg.MessageContent.Decoded)
	}
}

func markWithRefs(refs *messagesRefs) error {
	opcodes := make([]uint32, 0, len(refs.opcodeRefs))
	for opcode := range refs.opcodeRefs {
		opcodes = append(opcodes, opcode)
	}

	bodies := make([]string, 0, len(refs.bodyRefs))
	for body := range refs.bodyRefs {
		bodies = append(bodies, body)
	}

	if len(opcodes) == 0 && len(bodies) == 0 {
		return nil
	}

	decodedOpcodes, decodedBodies, _, err := MarkerRequest(opcodes, bodies, nil)
	if err != nil {
		return err
	}

	// fill in opcode results
	for i, opcode := range opcodes {
		if decodedValue := decodedOpcodes[i]; !strings.HasPrefix(decodedValue, "unknown") {
			for _, ref := range refs.opcodeRefs[opcode] {
				*ref = decodedValue
			}
		}
	}

	// fill in message body results
	for i, body := range bodies {
		decodedValue := decodedBodies[i]
		if strings.HasPrefix(decodedValue, "unknown") {
			for _, ref := range refs.bodyRefs[body] {
				(*ref) = nil
			}
			continue
		}
		for _, ref := range refs.bodyRefs[body] {
			var tmpResult map[string]interface{}
			if err := json.Unmarshal([]byte(decodedValue), &tmpResult); err != nil {
				tmpResult = map[string]interface{}{"failed_to_decode_json": decodedValue}
				log.Printf("Error: failed to decode message body %s, got json %v", body, decodedValue)
			}
			for msgType, msgData := range tmpResult {
				// there's only one key in tmpResult map
				if msgType == "text_comment" {
					// back compatibility with scheme for text comment
					if data, ok := msgData.(map[string]interface{}); ok {
						if text, ok := data["text"].(string); ok {
							*ref = &DecodedContent{Type: msgType, Comment: &text}
							break
						}
					}
				}
				*ref = &DecodedContent{Type: msgType, Data: msgData}
				break
			}
		}
	}

	return nil
}

func MarkAccountStates(states []AccountStateFull) error {
	methodIds := make([][]uint32, len(states))
	for i := range states {
		if states[i].ContractMethods != nil {
			methodIds[i] = *states[i].ContractMethods
		} else {
			methodIds[i] = []uint32{}
		}
	}
	_, _, recognizedInterfaces, err := MarkerRequest(nil, nil, methodIds)
	if err != nil {
		return err
	}
	for i := range states {
		if recognizedInterfaces[i] == "" {
			states[i].Interfaces = &[]string{}
			continue
		}
		interfaces := strings.Split(recognizedInterfaces[i], ",")
		states[i].Interfaces = &interfaces
	}
	return nil
}
