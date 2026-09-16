# Regenerates the per-fixture actions dump into a scratch dir and compares it
# against goldens/actions/. Any difference fails with the regen command.
file(REMOVE_RECURSE "${WORK_DIR}")
file(MAKE_DIRECTORY "${WORK_DIR}")
execute_process(
    COMMAND "${ENGINE}" --actions -T "${TRACES}" -O "${WORK_DIR}" --fixtures "${FIXTURES}"
    RESULT_VARIABLE run_rc ERROR_VARIABLE run_err)
if(NOT run_rc EQUAL 0)
    message(FATAL_ERROR "--actions run failed (${run_rc}):\n${run_err}")
endif()
execute_process(
    COMMAND diff -r -q "${GOLDEN_DIR}" "${WORK_DIR}"
    RESULT_VARIABLE diff_rc OUTPUT_VARIABLE diff_out)
if(NOT diff_rc EQUAL 0)
    message(FATAL_ERROR "actions golden diverged:\n${diff_out}\n"
        "Regen: ton-mch-engine --actions -T <traces> -O goldens/actions --fixtures goldens/fixtures.json")
endif()
