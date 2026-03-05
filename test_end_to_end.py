"""End-to-end test using the mock Jira server and synthetic LAVA events."""

import asyncio
import json
import sys

sys.path.insert(0, ".")

from lava_event_listener.config import JiraConfig
from lava_event_listener.handler import EventHandler
from lava_event_listener.jira_client import JiraClient
from lava_event_listener.state import StateManager

JIRA_URL = "http://127.0.0.1:8089"


def setup():
    jira_config = JiraConfig(
        url=JIRA_URL,
        email="test@test.com",
        api_token="fake",
        project_key="TEST",
        issue_type="Service Request",
    )
    state = StateManager("test_state.json")
    jira = JiraClient(jira_config)
    handler = EventHandler(jira, state)
    return handler, state


async def run_tests():
    handler, state = setup()

    print("=== Test 1: Device goes Bad -> should create ticket ===")
    await handler.handle_device_event(
        "test-lab", "panda-01", "panda", "Bad", "Idle", "2026-03-05T10:00:00Z"
    )
    device_state = state.get_device("test-lab/panda-01")
    assert device_state is not None, "Device should be tracked in state"
    assert device_state.health == "Bad"
    print(f"  PASS: ticket={device_state.ticket_key}, health={device_state.health}")

    print("\n=== Test 2: Same device, same health -> should NOT create duplicate ===")
    await handler.handle_device_event(
        "test-lab", "panda-01", "panda", "Bad", "Idle", "2026-03-05T10:01:00Z"
    )
    device_state2 = state.get_device("test-lab/panda-01")
    assert device_state2.ticket_key == device_state.ticket_key, "Should reuse same ticket"
    print(f"  PASS: still ticket={device_state2.ticket_key} (no duplicate)")

    print("\n=== Test 3: Health changes Bad -> Maintenance -> should comment ===")
    await handler.handle_device_event(
        "test-lab", "panda-01", "panda", "Maintenance", "Idle", "2026-03-05T10:02:00Z"
    )
    device_state3 = state.get_device("test-lab/panda-01")
    assert device_state3.ticket_key == device_state.ticket_key, "Should reuse same ticket"
    assert device_state3.health == "Maintenance"
    print(f"  PASS: ticket={device_state3.ticket_key}, health updated to {device_state3.health}")

    print("\n=== Test 4: Device recovers (Good) -> should comment and remove from state ===")
    await handler.handle_device_event(
        "test-lab", "panda-01", "panda", "Good", "Idle", "2026-03-05T10:03:00Z"
    )
    device_state4 = state.get_device("test-lab/panda-01")
    assert device_state4 is None, "Device should be removed from state after recovery"
    print(f"  PASS: device removed from state")

    print("\n=== Test 5: Different device goes Bad -> should create separate ticket ===")
    await handler.handle_device_event(
        "test-lab", "rk3399-02", "rk3399", "Bad", "Idle", "2026-03-05T10:04:00Z"
    )
    device_state5 = state.get_device("test-lab/rk3399-02")
    assert device_state5 is not None
    assert device_state5.ticket_key != device_state.ticket_key, "Should be a different ticket"
    print(f"  PASS: ticket={device_state5.ticket_key} (different from first)")

    print("\n=== Test 6: Device goes Bad again after recovery -> should create NEW ticket ===")
    await handler.handle_device_event(
        "test-lab", "panda-01", "panda", "Bad", "Idle", "2026-03-05T10:05:00Z"
    )
    device_state6 = state.get_device("test-lab/panda-01")
    assert device_state6 is not None
    assert device_state6.ticket_key != device_state.ticket_key, "Should be a new ticket"
    print(f"  PASS: new ticket={device_state6.ticket_key} (previous was {device_state.ticket_key})")

    print("\n=== Test 7: Unknown health -> should be ignored ===")
    await handler.handle_device_event(
        "test-lab", "juno-03", "juno", "Unknown", "Idle", "2026-03-05T10:06:00Z"
    )
    device_state7 = state.get_device("test-lab/juno-03")
    assert device_state7 is None, "Unknown health should not create a ticket"
    print(f"  PASS: no ticket created for Unknown health")

    print("\n" + "=" * 50)
    print("ALL TESTS PASSED")


if __name__ == "__main__":
    import os
    # Clean up any previous test state
    if os.path.exists("test_state.json"):
        os.remove("test_state.json")
    asyncio.run(run_tests())
    os.remove("test_state.json")
