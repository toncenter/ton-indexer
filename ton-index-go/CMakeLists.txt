add_custom_target(ton-index-go-abi
    COMMAND sh index/acton/catalog/prepare.sh
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    COMMENT "Preparing the shared pinned TON ABI catalog"
    VERBATIM)
set(SOURCE_FILE main.go)
add_go_executable(ton-index-go ${SOURCE_FILE})
generate_swagger(ton-index-go)

add_dependencies(ton-index-go ton-marker ton-index-go-abi)
add_dependencies(ton-index-go-swagger ton-index-go-abi)
