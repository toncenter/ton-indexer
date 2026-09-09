set(ACTON_CATALOG_DIR "${CMAKE_CURRENT_SOURCE_DIR}/index/acton/catalog")
set(ACTON_ABI_STAMP "${CMAKE_CURRENT_BINARY_DIR}/ton-index-go-abi.timestamp")
file(GLOB_RECURSE ACTON_ABI_SOURCES CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/index/acton/*.go")

# Track additions/removals too, without making checked-in Go files cleanable outputs.
file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/ton-index-go-abi.inputs"
  CONTENT "${ACTON_ABI_SOURCES}\n")
add_custom_command(OUTPUT "${ACTON_ABI_STAMP}"
  COMMAND "${CMAKE_COMMAND}" -E env CGO_ENABLED=0 "${CMAKE_Go_COMPILER}" run
    ./index/acton/cmd/tolk-abi-to-go
    --catalog "${ACTON_CATALOG_DIR}/catalog.json"
    --output-dir "${ACTON_CATALOG_DIR}" --package catalog
  COMMAND "${CMAKE_COMMAND}" -E touch "${ACTON_ABI_STAMP}"
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  DEPENDS ${ACTON_ABI_SOURCES}
    "${ACTON_CATALOG_DIR}/catalog.json"
    "${CMAKE_CURRENT_SOURCE_DIR}/go.mod"
    "${CMAKE_CURRENT_SOURCE_DIR}/go.sum"
    "${CMAKE_CURRENT_BINARY_DIR}/ton-index-go-abi.inputs"
    "${CMAKE_CURRENT_LIST_FILE}"
  COMMENT "Generating native Go bindings from the pinned Acton catalog"
  VERBATIM)
add_custom_target(ton-index-go-abi DEPENDS "${ACTON_ABI_STAMP}")

set(SOURCE_FILE main.go)
add_go_executable(ton-index-go ${SOURCE_FILE})
add_custom_command(OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/ton-index-go.timestamp" APPEND
  DEPENDS "${ACTON_ABI_STAMP}")
generate_swagger(ton-index-go)
add_dependencies(ton-index-go-swagger ton-index-go-abi)

add_dependencies(ton-index-go ton-marker ton-index-go-abi)
