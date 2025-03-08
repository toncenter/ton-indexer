set(SOURCE_FILE main.go)
add_go_executable(ton-emulate-go ${SOURCE_FILE})
generate_swagger(ton-emulate-go)
