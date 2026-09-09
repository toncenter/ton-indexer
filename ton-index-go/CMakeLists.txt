set(ACTON_CATALOG_DIR "${CMAKE_CURRENT_SOURCE_DIR}/index/acton/catalog")

# Always run so any missing binding is repaired without glob/reconfigure loops.
# Acton's generator preserves contents and mtimes when outputs are unchanged.
add_custom_target(ton-index-go-abi
  COMMAND "${CMAKE_COMMAND}" -E env CGO_ENABLED=0 "${CMAKE_Go_COMPILER}" run
    github.com/ton-blockchain/acton/packages/abi-go/cmd/tolk-abi-to-go
    --catalog "${ACTON_CATALOG_DIR}/catalog.json"
    --output-dir "${ACTON_CATALOG_DIR}" --package catalog
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  DEPENDS "${ACTON_CATALOG_DIR}/catalog.json"
    "${CMAKE_CURRENT_SOURCE_DIR}/go.mod"
    "${CMAKE_CURRENT_SOURCE_DIR}/go.sum"
  COMMENT "Generating native Go bindings from the pinned Acton catalog"
  VERBATIM)

set(SOURCE_FILE main.go)
add_go_executable(ton-index-go ${SOURCE_FILE})
generate_swagger(ton-index-go)
add_dependencies(ton-index-go-swagger ton-index-go-abi)

add_dependencies(ton-index-go ton-marker ton-index-go-abi)
