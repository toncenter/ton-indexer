if(NOT DEFINED PATCH_FILE)
  message(FATAL_ERROR "PATCH_FILE is not set")
endif()
if(NOT DEFINED PATCH_WORKING_DIRECTORY)
  message(FATAL_ERROR "PATCH_WORKING_DIRECTORY is not set")
endif()
if(NOT DEFINED GIT_EXECUTABLE)
  message(FATAL_ERROR "GIT_EXECUTABLE is not set")
endif()

if(NOT EXISTS "${PATCH_FILE}")
  message(FATAL_ERROR "Patch file does not exist: ${PATCH_FILE}")
endif()
if(NOT EXISTS "${PATCH_WORKING_DIRECTORY}")
  message(FATAL_ERROR "Patch working directory does not exist: ${PATCH_WORKING_DIRECTORY}")
endif()

execute_process(
  COMMAND "${GIT_EXECUTABLE}" -C "${PATCH_WORKING_DIRECTORY}" apply --check "${PATCH_FILE}"
  RESULT_VARIABLE patch_can_apply
  OUTPUT_QUIET
  ERROR_VARIABLE patch_check_error
)

if(patch_can_apply EQUAL 0)
  execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${PATCH_WORKING_DIRECTORY}" apply "${PATCH_FILE}"
    RESULT_VARIABLE patch_apply_result
    OUTPUT_VARIABLE patch_apply_output
    ERROR_VARIABLE patch_apply_error
  )
  if(NOT patch_apply_result EQUAL 0)
    message(FATAL_ERROR
      "Failed to apply patch ${PATCH_FILE}\n"
      "${patch_apply_output}\n"
      "${patch_apply_error}"
    )
  endif()
  message(STATUS "Applied patch: ${PATCH_FILE}")
  return()
endif()

execute_process(
  COMMAND "${GIT_EXECUTABLE}" -C "${PATCH_WORKING_DIRECTORY}" apply --reverse --check "${PATCH_FILE}"
  RESULT_VARIABLE patch_already_applied
  OUTPUT_QUIET
  ERROR_VARIABLE patch_reverse_check_error
)

if(patch_already_applied EQUAL 0)
  message(STATUS "Patch already applied: ${PATCH_FILE}")
  return()
endif()

message(FATAL_ERROR
  "Patch cannot be applied cleanly and is not already applied: ${PATCH_FILE}\n"
  "git apply --check error:\n${patch_check_error}\n"
  "git apply --reverse --check error:\n${patch_reverse_check_error}"
)
