# A directory cannot be opened as the report. Fail before touching the DB or
# initializing secondary logs, even when the database does not exist.
file(MAKE_DIRECTORY "${WORK}")
execute_process(
  COMMAND "${REPLAY}" --db "${WORK}/missing-db" --working-dir "${WORK}/secondary"
          --seqnos 1 --out "${WORK}"
  RESULT_VARIABLE result
  ERROR_VARIABLE error
  TIMEOUT 10)
if (NOT result STREQUAL "1" OR NOT error MATCHES "cannot open replay output")
  message(FATAL_ERROR "Expected an output error, got ${result}: ${error}")
endif()
if (EXISTS "${WORK}/secondary")
  message(FATAL_ERROR "Replay initialized the database before checking its output")
endif()
