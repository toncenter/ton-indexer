set(SOURCE_FILE main.go)
add_go_executable(ton-index-go ${SOURCE_FILE})
generate_swagger(ton-index-go)
