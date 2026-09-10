# Always run so any missing binding is repaired without glob/reconfigure loops.
# Acton's generator preserves contents and mtimes when outputs are unchanged.
add_custom_target(ton-index-go-abi
  COMMAND "${CMAKE_COMMAND}" -E env CGO_ENABLED=0 "${CMAKE_Go_COMPILER}" generate ./index/acton/catalog
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  COMMENT "Generating native Go bindings from the pinned Acton catalog"
  VERBATIM)

set(SOURCE_FILE main.go)
add_go_executable(ton-index-go ${SOURCE_FILE})
generate_swagger(ton-index-go)
add_dependencies(ton-index-go-swagger ton-index-go-abi)

add_dependencies(ton-index-go ton-marker ton-index-go-abi)
