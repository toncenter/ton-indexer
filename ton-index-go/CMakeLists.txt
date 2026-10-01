# The catalog is the ton-blockchain/abis release pinned in catalog.lock. A file that
# already has the expected hash is not downloaded again.
set(ABI_CATALOG_LOCK "${CMAKE_CURRENT_SOURCE_DIR}/index/acton/catalog/catalog.lock")
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${ABI_CATALOG_LOCK}")
file(STRINGS "${ABI_CATALOG_LOCK}" ABI_CATALOG_ENTRIES REGEX "^ABI_CATALOG_[A-Z0-9_]+=")
foreach(ENTRY IN LISTS ABI_CATALOG_ENTRIES)
  string(REGEX MATCH "^([A-Z0-9_]+)=(.*)$" MATCHED "${ENTRY}")
  set(${CMAKE_MATCH_1} "${CMAKE_MATCH_2}")
endforeach()
if(NOT ABI_CATALOG_URL OR NOT ABI_CATALOG_SHA256)
  message(FATAL_ERROR "${ABI_CATALOG_LOCK} must set ABI_CATALOG_URL and ABI_CATALOG_SHA256")
endif()
file(DOWNLOAD
  "${ABI_CATALOG_URL}"
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
