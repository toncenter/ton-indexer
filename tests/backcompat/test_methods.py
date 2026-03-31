from __future__ import annotations

import pytest

from tests.backcompat.backcompat import assert_backcompat

pytestmark = pytest.mark.backcompat


def test_method_with_param_from_another_request(backcompat_context, swagger_spec) -> None:
    """Example 2: derive mc_seqno from masterchainInfo, then reuse it in another call."""
    mc_seqno = _common_seqno_from_masterchain_info(backcompat_context)

    assert_backcompat(
        case_id="example_param_from_masterchain_info",
        method="GET",
        path="/api/v3/blocks",
        query={"mc_seqno": mc_seqno, "limit": 10, "offset": 0, "sort": "asc"},
        backcompat_context=backcompat_context,
        swagger_spec=swagger_spec,
    )


@pytest.mark.skip(reason="Example template. Remove skip and adjust it for your own case.")
def test_skipped_template_with_latest_reference_seqno(
    reference_latest_masterchain_seqno,
    backcompat_context,
    swagger_spec,
) -> None:
    """
    Example 2a: intentionally skipped template that uses the latest seqno from the reference instance.

    Be careful with this pattern: the latest reference seqno can be newer than what devel has indexed.
    """
    assert_backcompat(
        case_id="example_skipped_latest_reference_seqno",
        method="GET",
        path="/api/v3/transactionsByMasterchainBlock",
        query={
            "seqno": reference_latest_masterchain_seqno,
            "limit": 10,
            "offset": 0,
            "sort": "asc",
        },
        backcompat_context=backcompat_context,
        swagger_spec=swagger_spec,
    )


@pytest.mark.parametrize(
    ("case_id", "query"),
    [
        ("example_several_params_1", {"limit": 1, "offset": 0}),
        ("example_several_params_2", {"limit": 3, "offset": 0}),
        ("example_several_params_3", {"limit": 5, "offset": 2}),
    ],
)
def test_method_with_several_parameter_sets(case_id, query, backcompat_context, swagger_spec) -> None:
    """Example 3: run the same method against several query combinations."""
    assert_backcompat(
        case_id=case_id,
        method="GET",
        path="/api/v3/topAccountsByBalance",
        query=query,
        backcompat_context=backcompat_context,
        swagger_spec=swagger_spec,
    )


def _common_seqno_from_masterchain_info(backcompat_context) -> int:
    reference, devel = backcompat_context.fetch_pair("GET", "/api/v3/masterchainInfo")

    if reference.status_code != 200 or devel.status_code != 200:
        pytest.skip(
            "masterchainInfo must return 200 on both environments before chaining its output"
        )
    if reference.json_body is None or devel.json_body is None:
        pytest.skip("masterchainInfo did not return JSON on both environments")

    reference_first = int(reference.json_body["first"]["seqno"])
    reference_last = int(reference.json_body["last"]["seqno"])
    devel_first = int(devel.json_body["first"]["seqno"])
    devel_last = int(devel.json_body["last"]["seqno"])

    common_first = max(reference_first, devel_first)
    common_last = min(reference_last, devel_last)
    if common_last < common_first:
        pytest.skip("reference and devel do not overlap on masterchain seqno")

    return max(common_first, common_last - 2)
