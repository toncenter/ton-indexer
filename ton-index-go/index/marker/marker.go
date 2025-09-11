package marker

/*
#cgo CXXFLAGS: -I/Users/victor/Projects/dimin-indexer/ton-index-worker/ton-boc-marker/src
#cgo LDFLAGS: -L/Users/victor/Projects/dimin-indexer/ton-index-worker/build/ton-boc-marker -lton-marker-core -lton-marker -Wl,-rpath,/Users/victor/Projects/dimin-indexer/ton-index-worker/build/ton-boc-marker

#include "wrapper_cgo.h"
#include <stdlib.h>
*/
import "C"
import (
	"errors"
	"fmt"
	"unsafe"
)

func MarkerRequest(opcodesList []uint32, bocBase64List []string, methodIdsList [][]uint32) ([]string, []string, []string, error) {
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
	// defer cleanup of strings
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
		return nil, nil, nil, errors.New("ton_marker_process_batch returned a nil response")
	}
	defer C.ton_marker_free_batch_response(response)

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
		fmt.Println("boc length")
		fmt.Println(len(results))
		fmt.Println(response.boc_count)
		fmt.Println(response.boc_results)
		for _, res := range results {
			bocResults = append(bocResults, C.GoString(res))
		}
	}

	// get interface results
	var interfaceResults []string = make([]string, 0)
	if response.interface_count > 0 && response.interface_results != nil {
		results := unsafe.Slice(response.interface_results, 0)
		fmt.Println("interface length")
		fmt.Println(len(results))
		fmt.Println(response.interface_count)
		fmt.Println(response.interface_results)
		for _, res := range results {
			interfaceResults = append(interfaceResults, C.GoString(res))
		}
	}
	return opcodeResults, bocResults, interfaceResults, nil
}
