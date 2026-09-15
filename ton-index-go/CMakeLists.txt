# The catalog is a pinned ton-blockchain/abis release. A file that already has the
# expected hash is not downloaded again.
set(ABI_CATALOG_VERSION v0.1.0)
set(ABI_CATALOG_SHA256 9f023acf918493dfc908c66cfd38200e99d393beaaf27296da4563479638fea5)
file(DOWNLOAD
  "https://github.com/ton-blockchain/abis/releases/download/${ABI_CATALOG_VERSION}/abi-catalog.json"
  "${CMAKE_CURRENT_SOURCE_DIR}/index/acton/catalog/catalog.json"
  EXPECTED_HASH SHA256=${ABI_CATALOG_SHA256}
  TLS_VERIFY ON)

# Always run so any missing binding is repaired without glob/reconfigure loops.
# The generator preserves contents and mtimes when outputs are unchanged.
add_custom_target(ton-index-go-abi
  COMMAND "${CMAKE_COMMAND}" -E env CGO_ENABLED=0 "${CMAKE_Go_COMPILER}" tool tolk-abi-to-go
          --catalog index/acton/catalog/catalog.json --output-dir index/acton/catalog --package catalog
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  COMMENT "Generating native Go bindings from the pinned Acton catalog"
  VERBATIM)

set(SOURCE_FILE main.go)
add_go_executable(ton-index-go ${SOURCE_FILE})
generate_swagger(ton-index-go)
add_dependencies(ton-index-go-swagger ton-index-go-abi)

add_dependencies(ton-index-go ton-marker ton-index-go-abi)
