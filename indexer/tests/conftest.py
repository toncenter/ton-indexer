import pytest
from pathlib import Path

from indexer.events import context
from tests.utils.repository import TestInterfaceRepository
from tests.utils.trace_deserializer import load_trace_from_file


def pytest_addoption(parser):
    parser.addoption(
        "--traces-dir",
        action="store",
        default="tests/traces",
        help="Path to traces directory relative to project root",
    )


@pytest.fixture
def traces_dir(pytestconfig):
    """Return the path to the traces directory."""
    return Path(pytestconfig.rootdir) / pytestconfig.getoption("--traces-dir")


def pytest_configure(config):
    """Add custom markers."""
    config.addinivalue_line("markers", "trace_file(filename): mark test to load a specific trace file")


@pytest.fixture
def trace_data(request, traces_dir):
    """
    Fixture that automatically loads the trace file specified by the trace_file marker.
    
    Usage with explicit filename:
        @pytest.mark.trace_file('jetton_mint_minter_success_1.lz4')
        async def test_something(trace_data):
            trace, interfaces = trace_data
    """
    marker = request.node.get_closest_marker("trace_file")
    if marker is None:
        pytest.fail("Test requires a trace_file marker")
    if len(marker.args) != 1:
        pytest.fail("trace_file marker accepts only one argument")
    # Get filename from marker args or generate from test name

    filename = marker.args[0]

    file_path = traces_dir / filename

    if not file_path.exists():
        pytest.fail(f"Trace file not found: {file_path}")

    trace, interfaces = load_trace_from_file(file_path)
    repository = TestInterfaceRepository(interfaces)
    context.interface_repository.set(repository)

    return trace, interfaces
