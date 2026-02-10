set(SOURCE_FILE main.go healthz.go)
add_go_executable(ton-streaming-go ${SOURCE_FILE})
add_dependencies(ton-streaming-go ton-marker)
