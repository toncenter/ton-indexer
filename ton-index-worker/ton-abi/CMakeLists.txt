cmake_minimum_required(VERSION 3.16)

# --- ton-abi: standalone C++ implementation of Tolk ABI message
# serialization and deserialization. This library deliberately has no
# ton-mch-engine dependency. Parsers are generated in the build tree from
# tracked ABI declarations, with AbiValue retained as their protocol-neutral
# dump format.

# Runtime: leaf codecs + AbiValue. This is the only part production links.
add_library(ton-abi STATIC
    src/AbiLeavesScalar.cpp
    src/AbiLeavesAddress.cpp
    src/AbiLeavesRef.cpp
    src/AbiLeavesContainer.cpp
    src/AbiLeavesDict.cpp
    src/AbiLeavesPrefix.cpp
    src/AbiValue.cpp
)
target_include_directories(ton-abi PUBLIC src)
target_compile_features(ton-abi PUBLIC cxx_std_20)
target_link_libraries(ton-abi PUBLIC tdutils ton_crypto_core)

# Codegen: JSON loader, resolution kernel, C++ emitter. Linked by the
# generator tool and tests only; keeps loader/kernel headers out of the
# production compile surface.
add_library(ton-abi-codegen STATIC
    codegen/AbiLoader.cpp
    codegen/AbiKernel.cpp
    codegen/AbiEmit.cpp
)
target_include_directories(ton-abi-codegen PUBLIC codegen)
target_link_libraries(ton-abi-codegen PUBLIC ton-abi)

# ton-abi-gen emits build-tree generated/<contract>_gen.{h,cpp} pairs using the
# model, loader, resolution kernel, and emitter library.
add_executable(ton-abi-gen tools/AbiGenMain.cpp)
target_compile_features(ton-abi-gen PRIVATE cxx_std_20)
target_link_libraries(ton-abi-gen ton-abi-codegen tdutils)

# Generate every loadable fixture and production declaration in one batch.
# The two err-invalid-map-key fixtures are intentional hard-fail cases and are
# exercised directly by AbiEmitTest instead of through the CLI.
file(GLOB TON_ABI_CHECK_ABIS CONFIGURE_DEPENDS
     ${CMAKE_CURRENT_SOURCE_DIR}/testdata/fixtures/*.abi.json
     ${CMAKE_CURRENT_SOURCE_DIR}/abi/*.abi.json)
list(FILTER TON_ABI_CHECK_ABIS EXCLUDE REGEX "err-invalid-map-key")

set(TON_ABI_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(TON_ABI_GENERATED_HEADERS)
set(TON_ABI_GENERATED_SOURCES)
foreach(TON_ABI_INPUT IN LISTS TON_ABI_CHECK_ABIS)
    get_filename_component(TON_ABI_INPUT_NAME "${TON_ABI_INPUT}" NAME)
    string(REGEX REPLACE "\\.abi\\.json$" "" TON_ABI_STEM "${TON_ABI_INPUT_NAME}")
    string(REPLACE "-" "_" TON_ABI_STEM "${TON_ABI_STEM}")
    list(APPEND TON_ABI_GENERATED_HEADERS "${TON_ABI_GENERATED_DIR}/${TON_ABI_STEM}_gen.h")
    list(APPEND TON_ABI_GENERATED_SOURCES "${TON_ABI_GENERATED_DIR}/${TON_ABI_STEM}_gen.cpp")
endforeach()
set(TON_ABI_GENERATED ${TON_ABI_GENERATED_HEADERS} ${TON_ABI_GENERATED_SOURCES})

file(MAKE_DIRECTORY "${TON_ABI_GENERATED_DIR}")
add_custom_command(
    OUTPUT ${TON_ABI_GENERATED}
    COMMAND ton-abi-gen ${TON_ABI_CHECK_ABIS} --out-dir "${TON_ABI_GENERATED_DIR}"
    DEPENDS ton-abi-gen ${TON_ABI_CHECK_ABIS}
    COMMENT "Generate deterministic ton-abi C++ parsers"
    VERBATIM
)
add_custom_target(ton_abi_generate_sources DEPENDS ${TON_ABI_GENERATED})

add_executable(ton-abi-test
    test/smoke_test.cpp
    test/AbiLoaderTest.cpp
    test/AbiKernelTest.cpp
    test/AbiLeavesScalarTest.cpp
    test/AbiLeavesAddressTest.cpp
    test/AbiLeavesRefTest.cpp
    test/AbiLeavesContainerTest.cpp
    test/AbiLeavesDictTest.cpp
    test/AbiLeavesPrefixTest.cpp
    test/AbiValueTest.cpp
    test/AbiW10Test.cpp
    test/AbiEmitTest.cpp
    test/AbiABGateTest.cpp
)
target_include_directories(ton-abi-test PRIVATE test "${TON_ABI_GENERATED_DIR}")

# Every build-time generated pair is compiled into the test executable. This
# covers all loadable fixtures and production declarations, and supplies the
# generated-code comparison gate with its inputs.
# --check (add_test below) covers byte-repro for all of them.
target_sources(ton-abi-test PRIVATE ${TON_ABI_GENERATED_SOURCES})
target_compile_features(ton-abi-test PRIVATE cxx_std_20)
target_compile_definitions(ton-abi-test PRIVATE
    TON_ABI_VECTORS_FILE="${CMAKE_CURRENT_SOURCE_DIR}/testdata/abi_vectors.json"
    TON_ABI_FIXTURES_DIR="${CMAKE_CURRENT_SOURCE_DIR}/testdata/fixtures"
    TON_ABI_GEN_EXE="$<TARGET_FILE:ton-abi-gen>"
    TON_ABI_TEST_TMP="${CMAKE_CURRENT_BINARY_DIR}/gen_cli_tmp"
)
add_dependencies(ton-abi-test ton-abi-gen)  # Cli-analog test runs the exe
add_dependencies(ton-abi-test ton_abi_generate_sources)
target_link_libraries(ton-abi-test ton-abi-codegen tdutils)

enable_testing()
add_test(NAME ton_abi_smoke COMMAND ton-abi-test)

# --check build-time determinism gate: regenerate every generated pair in memory
# and byte-compare it with the primary generation output in the build tree.
add_test(NAME ton_abi_gen_check
         COMMAND ton-abi-gen ${TON_ABI_CHECK_ABIS} --out-dir ${TON_ABI_GENERATED_DIR} --check)

# Source-declaration repro gate: compile every production .tolk declaration and
# byte-compare its committed .abi.json before ton_abi_gen_check consumes it.
add_test(NAME ton_abi_prod_json_check
         COMMAND node tools/gen_prod_abi.mjs --check)
set_tests_properties(ton_abi_prod_json_check PROPERTIES
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
