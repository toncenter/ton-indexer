set(GOPATH "${CMAKE_CURRENT_BINARY_DIR}/go")
file(MAKE_DIRECTORY ${GOPATH})

function(ExternalGoProject_Add TARG)
  add_custom_target(${TARG} env GOPATH=${GOPATH} ${CMAKE_Go_COMPILER} get ${ARGN})
endfunction(ExternalGoProject_Add)

function(add_go_executable NAME)
  file(GLOB GO_SOURCE RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "*.go")
  add_custom_command(OUTPUT ${OUTPUT_DIR}/.timestamp 
    COMMAND env GOPATH=${GOPATH} ${CMAKE_Go_COMPILER} build
    -o "${CMAKE_CURRENT_BINARY_DIR}/${NAME}"
    ${CMAKE_GO_FLAGS} ${GO_SOURCE}
    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
    COMMENT "Building Go executable ${NAME}")

  add_custom_target(${NAME} ALL DEPENDS ${OUTPUT_DIR}/.timestamp ${ARGN})
  install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/${NAME} DESTINATION bin)
endfunction(add_go_executable)

add_custom_command(
    OUTPUT ${GOPATH}/bin/swag
    COMMAND env GOPATH=${GOPATH} ${CMAKE_Go_COMPILER} install github.com/swaggo/swag/cmd/swag@latest
    COMMENT "Installing github.com/swaggo/swag/cmd/swag@latest"
)

add_custom_target(install_swag DEPENDS ${GOPATH}/bin/swag)

function(generate_swagger NAME)
  add_custom_command(OUTPUT ${OUTPUT_DIR}/swagger.timestamp
    COMMAND ${GOPATH}/bin/swag init
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Generating Swagger docs with swag init"
    DEPENDS install_swag)

  add_custom_target(${NAME}-swagger ALL DEPENDS ${OUTPUT_DIR}/swagger.timestamp ${ARGN})
  add_dependencies(${NAME} ${NAME}-swagger)
endfunction(generate_swagger)

function(ADD_GO_LIBRARY NAME BUILD_TYPE)
  if(BUILD_TYPE STREQUAL "STATIC")
    set(BUILD_MODE -buildmode=c-archive)
    set(LIB_NAME "lib${NAME}.a")
  else()
    set(BUILD_MODE -buildmode=c-shared)
    if(APPLE)
      set(LIB_NAME "lib${NAME}.dylib")
    else()
      set(LIB_NAME "lib${NAME}.so")
    endif()
  endif()

  file(GLOB GO_SOURCE RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "*.go")
  add_custom_command(OUTPUT ${OUTPUT_DIR}/.timestamp
    COMMAND env GOPATH=${GOPATH} ${CMAKE_Go_COMPILER} build ${BUILD_MODE}
    -o "${CMAKE_CURRENT_BINARY_DIR}/${LIB_NAME}"
    ${CMAKE_GO_FLAGS} ${GO_SOURCE}
    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR})

  add_custom_target(${NAME} ALL DEPENDS ${OUTPUT_DIR}/.timestamp ${ARGN})

  if(NOT BUILD_TYPE STREQUAL "STATIC")
    install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/${LIB_NAME} DESTINATION bin)
  endif()
endfunction(ADD_GO_LIBRARY)
