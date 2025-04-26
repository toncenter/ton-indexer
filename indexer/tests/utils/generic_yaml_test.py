import re
from pathlib import Path
from typing import Any, Dict

import pytest
import yaml
from jinja2 import Template

from event_classifier import process_trace
from indexer.events import context
from indexer.events.blocks.utils.block_tree_serializer import serialize_blocks
from indexer.events.event_processing import process_event_async_with_postprocessing
from tests.utils.repository import TestInterfaceRepository
from tests.utils.trace_deserializer import load_trace_from_file


def get_nested_value(obj: Any, path: str) -> Any:
    """Get a nested value from an object using dot notation."""
    # Handle array indexing with [n]
    array_index_pattern = re.compile(r'(.+?)\[(\d+)\](.*)$')

    if "." in path:
        key, rest = path.split(".", 1)
        # Check if key has array index
        match = array_index_pattern.match(key)
        if match:
            array_key, index, remaining = match.groups()
            index = int(index)
            array = getattr(obj, array_key, None) if hasattr(obj, array_key) else obj.get(array_key, None)
            if array and len(array) > index:
                return get_nested_value(array[index], remaining + ("." + rest if remaining else rest))
            return None

        # Regular object traversal
        if hasattr(obj, key):
            return get_nested_value(getattr(obj, key), rest)
        elif isinstance(obj, dict) and key in obj:
            return get_nested_value(obj[key], rest)
        return None
    else:
        # Check for array index at final level
        match = array_index_pattern.match(path)
        if match:
            array_key, index, _ = match.groups()
            index = int(index)
            array = getattr(obj, array_key, None) if hasattr(obj, array_key) else obj.get(array_key, None)
            if array and len(array) > index:
                return array[index]
            return None

        # Regular final property access
        if hasattr(obj, path):
            value = getattr(obj, path)
            # Handle potential dataclass conversion
            if hasattr(value, "as_str"):
                return value.as_str().lower()
            return value
        elif isinstance(obj, dict) and path in obj:
            value = obj[path]
            # Handle potential dataclass conversion
            if hasattr(value, "as_str"):
                return value.as_str().lower()
            return value
        return None


def load_yaml_file(file_path: Path) -> Dict[str, Dict]:
    """Load test cases from a single YAML file."""
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
        return data.get("cases", {})


def evaluate_jinja_expression(template_str: str, actual_value: Any) -> bool:
    """Evaluate a Jinja2 template string as a condition."""
    # Prepare the template with the value in context
    if not isinstance(template_str, str) or not template_str.startswith("{{") or not template_str.endswith("}}"):
        return actual_value == template_str

    template = Template(template_str)
    result = template.render(value=actual_value)
    # Convert string 'True'/'False' to boolean
    if result.lower() == 'true':
        return True
    if result.lower() == 'false':
        return False
    return bool(result)

def check_value(actual_value: Any, expected_value: Any, path: str) -> None:
    """Check if the actual value matches the expected value, with support for Jinja templates."""
    if isinstance(expected_value, str) and expected_value.startswith("{{") and expected_value.endswith("}}"):
        # This is a Jinja template expression
        result = evaluate_jinja_expression(expected_value, actual_value)
        assert result, f"Failed condition at '{path}': {expected_value}\nActual value: {actual_value}"
    elif isinstance(expected_value, list) and isinstance(actual_value, list):
        assert set(actual_value) == set(
            expected_value), f"Lists don't match at '{path}':\nExpected: {expected_value}\nActual: {actual_value}"
    elif isinstance(expected_value, dict) and (isinstance(actual_value, dict) or hasattr(actual_value, "__dict__")):
        # Handle nested dictionary comparison or dataclass
        actual_dict = actual_value if isinstance(actual_value, dict) else vars(actual_value)
        for key, nested_expected in expected_value.items():
            nested_path = f"{path}.{key}" if path else key
            if key not in actual_dict:
                assert False, f"Missing key '{key}' at '{path}'.\nExpected keys: {list(expected_value.keys())}\nActual keys: {list(actual_dict.keys())}"
            nested_actual = actual_dict[key]
            # Recursively check each nested value
            check_value(nested_actual, nested_expected, nested_path)
    else:
        # Direct comparison with clear error message
        assert actual_value == expected_value, f"Values don't match at '{path}':\nExpected: {expected_value}\nActual: {actual_value}"

def find_matching_item(items, selector_expr):
    """Find an item that matches the selector expression."""
    for item in items:
        if evaluate_jinja_expression(selector_expr, item):
            return item
    return None


async def run_test_case(case_name: str, case_data: Dict, traces_dir: Path):
    """Run a single test case."""
    # Get trace for the case
    trace_id = case_data.get("trace-id")
    filename = traces_dir / f"{trace_id}.lz4"
    assert filename.exists(), f"Trace file not found: {filename}"
    trace, interfaces = load_trace_from_file(filename)
    repository = TestInterfaceRepository(interfaces)
    context.interface_repository.set(repository)

    assert trace_id, f"Missing trace-id for case {case_name}"
    # Process the trace
    result_blocks = await process_event_async_with_postprocessing(trace)
    _, _, actions, _ = await process_trace(trace)
    # Check expected blocks
    if "expected-blocks" in case_data:
        for expected_block in case_data["expected-blocks"]:
            block_type = expected_block.get("type")
            assert block_type, f"Missing block type in expected-blocks for case {case_name}"

            # Find all blocks of the specified type
            matching_blocks = [b for b in result_blocks if b.btype.lower() == block_type]

            # If a selector is provided, use it to find the specific block
            selector = expected_block.get("selector")
            if selector and matching_blocks:
                block = find_matching_item(matching_blocks, selector)
                assert block, f"No block of type {block_type} matched selector {selector} for case {case_name}"
                matching_blocks = [block]

            assert len(
                matching_blocks) == 1, f"Expected 1 block of type {block_type} but found {len(matching_blocks)} for case {case_name}"

            # Try to match each block
            for block in matching_blocks:
                # Check all expected values
                for path, expected_value in expected_block.get("values", {}).items():
                    actual_obj = get_nested_value(block, path)
                    check_value(actual_obj, expected_value, path)

    # Check expected actions
    if "expected-actions" in case_data:
        # actions, _ = serialize_blocks(result_blocks, trace.trace_id)
        for expected_action in case_data["expected-actions"]:
            action_type = expected_action.get("type")
            assert action_type, f"Missing action type in expected-actions for case {case_name}"

            # Find all actions of the specified type
            matching_actions = [a for a in actions if a.type.lower() == action_type.lower()]

            # If a selector is provided, use it to find the specific action
            selector = expected_action.get("selector")
            if selector and matching_actions:
                action = find_matching_item(matching_actions, selector)
                assert action, f"No action of type {action_type} matched selector {selector} for case {case_name}"
                matching_actions = [action]

            assert len(
                matching_actions) == 1, f"Expected 1 action of type {action_type} but found {len(matching_actions)} for case {case_name}"

            # Try to match each action
            for action in matching_actions:
                # Check all expected values
                for path, expected_value in expected_action.get("values", {}).items():
                    actual_obj = get_nested_value(action, path)
                    check_value(actual_obj, expected_value, path)


# Base test class with common functionality
class BaseGenericActionTest:
    yaml_file = None  # Will be overridden by subclasses
    generate_test_cases = True

    @classmethod
    def pytest_generate_tests(cls, metafunc):
        if not cls.generate_test_cases:
            return
        """Generate test cases from the YAML file specified by the subclass."""
        if "case_name" in metafunc.fixturenames and "case_data" in metafunc.fixturenames:
            # Make sure yaml_file is set
            assert cls.yaml_file is not None, f"yaml_file not set for class {cls.__name__}"

            # Build the path to the YAML file
            yaml_path = Path(metafunc.config.rootdir) / "tests" / "test_cases" / cls.yaml_file

            # Load test cases from the file
            test_cases = load_yaml_file(yaml_path)

            # Parametrize the test
            metafunc.parametrize(
                "case_name,case_data",
                list(test_cases.items()),
                ids=list(test_cases.keys())
            )

    @pytest.mark.asyncio
    async def test_yaml_cases(self, pytestconfig, case_name, case_data, traces_dir):
        """Run test cases from the YAML file."""
        await run_test_case(case_name, case_data, traces_dir)
