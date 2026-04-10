from __future__ import annotations

import os

import pytest

from tests.backcompat.backcompat import (
    ApiClient,
    BackcompatContext,
    DEFAULT_SWAGGER_URL,
    DEFAULT_TIMEOUT_SECONDS,
    RuntimeConfig,
    default_cases,
    fetch_swagger,
    load_custom_cases,
)


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("backcompat")
    group.addoption(
        "--reference-base-url",
        action="store",
        default=os.getenv("BACKCOMPAT_REFERENCE_BASE_URL"),
        help="Reference environment base URL. Accepts either host root or /api/v3 prefix.",
    )
    group.addoption(
        "--devel-base-url",
        action="store",
        default=os.getenv("BACKCOMPAT_DEVEL_BASE_URL"),
        help="Devel environment base URL. Accepts either host root or /api/v3 prefix.",
    )
    group.addoption(
        "--api-key",
        action="store",
        default=os.getenv("BACKCOMPAT_API_KEY"),
        help="Shared API key used for both environments unless overridden.",
    )
    group.addoption(
        "--reference-api-key",
        action="store",
        default=os.getenv("BACKCOMPAT_REFERENCE_API_KEY"),
        help="Reference API key override.",
    )
    group.addoption(
        "--devel-api-key",
        action="store",
        default=os.getenv("BACKCOMPAT_DEVEL_API_KEY"),
        help="Devel API key override.",
    )
    group.addoption(
        "--backcompat-swagger-url",
        action="store",
        default=os.getenv("BACKCOMPAT_SWAGGER_URL", DEFAULT_SWAGGER_URL),
        help="Swagger document URL used to validate generated and custom cases.",
    )
    group.addoption(
        "--backcompat-timeout",
        action="store",
        type=float,
        default=float(os.getenv("BACKCOMPAT_TIMEOUT", DEFAULT_TIMEOUT_SECONDS)),
        help="Per-request timeout in seconds.",
    )
    group.addoption(
        "--backcompat-custom-cases",
        action="store",
        default=os.getenv("BACKCOMPAT_CUSTOM_CASES"),
        help="Optional path to a JSON file with additional static request cases.",
    )
    group.addoption(
        "--backcompat-allow-unsafe",
        action="store_true",
        default=_env_flag("BACKCOMPAT_ALLOW_UNSAFE"),
        help="Allow explicitly unsafe cases such as POST /api/v3/message.",
    )
    group.addoption(
        "--backcompat-no-verify-tls",
        action="store_true",
        default=_env_flag("BACKCOMPAT_NO_VERIFY_TLS"),
        help="Disable TLS certificate verification for Swagger and API requests.",
    )


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "case" not in metafunc.fixturenames:
        return

    cases = default_cases() + load_custom_cases(metafunc.config.getoption("--backcompat-custom-cases"))
    metafunc.parametrize("case", cases, ids=[case.case_id for case in cases])


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "backcompat: compare reference and devel API responses")


@pytest.fixture(scope="session")
def runtime_config(pytestconfig: pytest.Config) -> RuntimeConfig:
    return RuntimeConfig(
        swagger_url=pytestconfig.getoption("--backcompat-swagger-url"),
        timeout_seconds=pytestconfig.getoption("--backcompat-timeout"),
        verify_tls=not pytestconfig.getoption("--backcompat-no-verify-tls"),
        allow_unsafe=pytestconfig.getoption("--backcompat-allow-unsafe"),
    )


@pytest.fixture(scope="session")
def swagger_spec(runtime_config: RuntimeConfig) -> dict:
    return fetch_swagger(
        runtime_config.swagger_url,
        runtime_config.timeout_seconds,
        runtime_config.verify_tls,
    )


@pytest.fixture(scope="session")
def backcompat_context(pytestconfig: pytest.Config, runtime_config: RuntimeConfig) -> BackcompatContext:
    reference_base_url = pytestconfig.getoption("--reference-base-url")
    devel_base_url = pytestconfig.getoption("--devel-base-url")

    if not reference_base_url or not devel_base_url:
        pytest.skip(
            "Set --reference-base-url and --devel-base-url "
            "(or BACKCOMPAT_REFERENCE_BASE_URL / BACKCOMPAT_DEVEL_BASE_URL)."
        )

    shared_api_key = pytestconfig.getoption("--api-key")
    reference_api_key = pytestconfig.getoption("--reference-api-key") or shared_api_key
    devel_api_key = pytestconfig.getoption("--devel-api-key") or shared_api_key

    reference_client = ApiClient(
        name="reference",
        base_url=reference_base_url,
        api_key=reference_api_key,
        timeout_seconds=runtime_config.timeout_seconds,
        verify_tls=runtime_config.verify_tls,
    )
    devel_client = ApiClient(
        name="devel",
        base_url=devel_base_url,
        api_key=devel_api_key,
        timeout_seconds=runtime_config.timeout_seconds,
        verify_tls=runtime_config.verify_tls,
    )
    return BackcompatContext(reference_client, devel_client)


@pytest.fixture(scope="session")
def reference_latest_masterchain_seqno(backcompat_context: BackcompatContext) -> int:
    response = backcompat_context.reference_client.request("GET", "/api/v3/masterchainInfo")

    if response.status_code != 200:
        pytest.skip(
            f"reference masterchainInfo must return 200 to derive the latest seqno, got {response.status_code}"
        )
    if response.json_body is None:
        pytest.skip("reference masterchainInfo did not return JSON")

    return int(response.json_body["last"]["seqno"])


def _env_flag(name: str) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return False
    return raw.strip().lower() in {"1", "true", "yes", "on"}
