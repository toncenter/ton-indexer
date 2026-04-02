from __future__ import annotations

import pytest

from tests.backcompat.backcompat import (
    DataUnavailableError,
    compare_responses,
    validate_case,
)


@pytest.mark.backcompat
def test_backcompat_case(case, backcompat_context, runtime_config, swagger_spec) -> None:
    if case.skip:
        pytest.skip(f"{case.case_id} is marked as skipped")
    
    if case.unsafe and not runtime_config.allow_unsafe:
        pytest.skip(f"{case.case_id} is unsafe; rerun with --backcompat-allow-unsafe to enable it")

    try:
        request = case.resolver(backcompat_context)
    except DataUnavailableError as exc:
        pytest.skip(str(exc))

    validate_case(swagger_spec, case, request)

    reference, devel = backcompat_context.fetch_pair(
        case.method,
        case.path,
        query=request.query,
        body=request.body,
    )
    compare_responses(case, request, reference, devel)
