set(SOURCE_FILE main.go)
add_go_executable(ton-emulate-go ${SOURCE_FILE})
add_dependencies(ton-emulate-go ton-marker)