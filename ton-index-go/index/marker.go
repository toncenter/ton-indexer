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
func MarkerRequest(opcodesList []uint32, bocBase64List []string) ([]string, []string, error) {
	if !isLibraryInitialized {
		return nil, nil, errors.New("ton-marker library is not initialized")
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

	// create batch request
	request := C.struct_TonMarkerBatchRequest{
		opcodes:         (*C.uint)(opcodesPtr),
		opcode_count:    C.int(len(opcodesList)),
		boc_base64_list: (**C.char)(bocListPtr),
		boc_count:       C.int(len(bocBase64List)),
	}

	// call batch function
	response := C.ton_marker_process_batch(&request)
	if response == nil {
		return nil, nil, errors.New("ton_marker_process_batch failed or returned nil")
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

	return opcodeResults, bocResults, nil
}

type bodyRef struct {
	content *(*DecodedContent)
	body    *(*json.RawMessage)
}
type messagesRefs struct {
	opcodeRefs map[uint32][]*(*string) // key: opcode, value: list of pointers to string pointers
	bodyRefs   map[string][]bodyRef    // key: body, value: list of pointers where to write decoded body
}

func MarkMessagesByPtr(messages []*Message) error {
	refs := &messagesRefs{
		opcodeRefs: make(map[uint32][]*(*string)),
		bodyRefs:   make(map[string][]bodyRef),
	}
	for i := range messages {
		collectSingleMessageRefs(messages[i], refs)
	}
	return markWithRefs(refs)
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
		opcodeRefs: make(map[uint32][]*(*string)),
		bodyRefs:   make(map[string][]bodyRef),
	}
	for i := range messages {
		collectSingleMessageRefs(&messages[i], refs)
	}
	return refs
}

func collectJettonTransfersRefs(transfers []JettonTransfer) *messagesRefs {
	refs := &messagesRefs{
		opcodeRefs: make(map[uint32][]*(*string)),
		bodyRefs:   make(map[string][]bodyRef),
	}
	for i := range transfers {
		transfer := &transfers[i]
		customPayload := transfer.CustomPayload
		if customPayload != nil && transfer.DecodedCustomPayload == nil { // skip if already decoded
			bodyref := bodyRef{nil, &transfer.DecodedCustomPayload}
			refs.bodyRefs[*customPayload] = append(refs.bodyRefs[*customPayload], bodyref)
		}
		forwardPayload := transfer.ForwardPayload
		if forwardPayload != nil && transfer.DecodedForwardPayload == nil {
			bodyref := bodyRef{nil, &transfer.DecodedForwardPayload}
			refs.bodyRefs[*forwardPayload] = append(refs.bodyRefs[*forwardPayload], bodyref)
		}
	}
	return refs
}

func collectJettonBurnsRefs(burns []JettonBurn) *messagesRefs {
	refs := &messagesRefs{
		opcodeRefs: make(map[uint32][]*(*string)),
		bodyRefs:   make(map[string][]bodyRef),
	}
	for i := range burns {
		burn := &burns[i]
		customPayload := burn.CustomPayload
		if customPayload != nil && burn.DecodedCustomPayload == nil {
			bodyref := bodyRef{nil, &burn.DecodedCustomPayload}
			refs.bodyRefs[*customPayload] = append(refs.bodyRefs[*customPayload], bodyref)
		}
	}
	return refs
}

func collectNFTTransfersRefs(transfers []NFTTransfer) *messagesRefs {
	refs := &messagesRefs{
		opcodeRefs: make(map[uint32][]*(*string)),
		bodyRefs:   make(map[string][]bodyRef),
	}
	for i := range transfers {
		transfer := &transfers[i]
		customPayload := transfer.CustomPayload
		if customPayload != nil && transfer.DecodedCustomPayload == nil {
			bodyref := bodyRef{nil, &transfer.DecodedCustomPayload}
			refs.bodyRefs[*customPayload] = append(refs.bodyRefs[*customPayload], bodyref)
		}
		forwardPayload := transfer.ForwardPayload
		if forwardPayload != nil && transfer.DecodedForwardPayload == nil {
			bodyref := bodyRef{nil, &transfer.DecodedForwardPayload}
			refs.bodyRefs[*forwardPayload] = append(refs.bodyRefs[*forwardPayload], bodyref)
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
		refs.opcodeRefs[uint32(*msg.Opcode)] = append(refs.opcodeRefs[uint32(*msg.Opcode)], &msg.DecodedOpcode)
	}
	// collect message bodies
	if msg.MessageContent != nil && msg.MessageContent.Body != nil && msg.MessageContent.DecodedTlb == nil {
		bodyref := bodyRef{&msg.MessageContent.Decoded, &msg.MessageContent.DecodedTlb}
		refs.bodyRefs[*msg.MessageContent.Body] = append(refs.bodyRefs[*msg.MessageContent.Body], bodyref)
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

	decodedOpcodes, decodedBodies, err := MarkerRequest(opcodes, bodies)
	if err != nil {
		return err
	}

	// fill in opcode results
	for i, opcode := range opcodes {
		if decodedValue := decodedOpcodes[i]; !strings.HasPrefix(decodedValue, "unknown") {
			for _, ref := range refs.opcodeRefs[opcode] {
				*ref = &decodedValue
			}
		} else {
			for _, ref := range refs.opcodeRefs[opcode] {
				*ref = nil
			}
		}
	}

	// fill in message body results
	for i, body := range bodies {
		decodedValue := decodedBodies[i]
		if strings.HasPrefix(decodedValue, "unknown") {
			for _, ref := range refs.bodyRefs[body] {
				(*ref.content) = nil
				(*ref.body) = nil
			}
			continue
		}
		for _, ref := range refs.bodyRefs[body] {
			// unmarshal as JSON RawMessage to preserve order
			var rawData json.RawMessage
			if err := json.Unmarshal([]byte(decodedValue), &rawData); err != nil {
				log.Printf("Error: failed to decode message body %s, got json %v", body, decodedValue)
				continue
			}

			// unmarshal as map only to get message type and maybe process text_comment
			var tmpResult map[string]interface{}
			if err := json.Unmarshal([]byte(decodedValue), &tmpResult); err != nil {
				log.Printf("Error: failed to unmarshal message body to map %s, got json %v", body, decodedValue)
				continue
			}
			*ref.body = &rawData
			// back compatibility with old scheme for text_comment, many clients rely on it
			msgType, hasType := tmpResult["@type"].(string)
			text, hasText := tmpResult["text"].(string)
			if hasType && msgType == "text_comment" && hasText {
				content := DecodedContent{"text_comment", text}
				*ref.content = &content
			} else {
				*ref.content = nil
			}
		}
	}

	return nil
}
