"""End-to-end test using the mock Jira server and synthetic LAVA events."""

import asyncio
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
        request_type="Service Request",
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
    ticket_1 = device_state.ticket_key
    print(f"  PASS: ticket={ticket_1}, health={device_state.health}")

    print("\n=== Test 2: Same device, same health -> should NOT create duplicate ===")
    await handler.handle_device_event(
        "test-lab", "panda-01", "panda", "Bad", "Idle", "2026-03-05T10:01:00Z"
    )
    device_state2 = state.get_device("test-lab/panda-01")
    assert device_state2.ticket_key == ticket_1, "Should reuse same ticket"
    print(f"  PASS: still ticket={device_state2.ticket_key} (no duplicate)")

    print("\n=== Test 3: Health changes Bad -> Maintenance -> should comment ===")
    await handler.handle_device_event(
        "test-lab", "panda-01", "panda", "Maintenance", "Idle", "2026-03-05T10:02:00Z"
    )
    device_state3 = state.get_device("test-lab/panda-01")
    assert device_state3.ticket_key == ticket_1, "Should reuse same ticket"
    assert device_state3.health == "Maintenance"
    print(f"  PASS: ticket={device_state3.ticket_key}, health updated to {device_state3.health}")

    print("\n=== Test 4: Device recovers (Good) -> should comment and keep in state ===")
    await handler.handle_device_event(
        "test-lab", "panda-01", "panda", "Good", "Idle", "2026-03-05T10:03:00Z"
    )
    device_state4 = state.get_device("test-lab/panda-01")
    assert device_state4 is not None, "Device should still be in state (ticket is open)"
    assert device_state4.ticket_key == ticket_1, "Should still reference same ticket"
    assert device_state4.health == "Good"
    print(f"  PASS: ticket={device_state4.ticket_key}, health={device_state4.health} (still in state)")

    print("\n=== Test 5: Device flaps Bad again -> should comment on SAME ticket ===")
    await handler.handle_device_event(
        "test-lab", "panda-01", "panda", "Bad", "Idle", "2026-03-05T10:04:00Z"
    )
    device_state5 = state.get_device("test-lab/panda-01")
    assert device_state5 is not None
    assert device_state5.ticket_key == ticket_1, "Should reuse same ticket after flap"
    assert device_state5.health == "Bad"
    print(f"  PASS: ticket={device_state5.ticket_key} (same ticket, no duplicate)")

    print("\n=== Test 6: Different device goes Bad -> should create separate ticket ===")
    await handler.handle_device_event(
        "test-lab", "rk3399-02", "rk3399", "Bad", "Idle", "2026-03-05T10:05:00Z"
    )
    device_state6 = state.get_device("test-lab/rk3399-02")
    assert device_state6 is not None
    assert device_state6.ticket_key != ticket_1, "Should be a different ticket"
    print(f"  PASS: ticket={device_state6.ticket_key} (different from first)")

    print("\n=== Test 7: Unknown health -> should be ignored ===")
    await handler.handle_device_event(
        "test-lab", "juno-03", "juno", "Unknown", "Idle", "2026-03-05T10:06:00Z"
    )
    device_state7 = state.get_device("test-lab/juno-03")
    assert device_state7 is None, "Unknown health should not create a ticket"
    print(f"  PASS: no ticket created for Unknown health")

    print("\n=== Test 8: Retired -> Bad (no Good in between) -> should comment, not new ticket ===")
    await handler.handle_device_event(
        "test-lab", "db845c-01", "dragonboard", "Retired", "Idle", "2026-03-05T11:00:00Z"
    )
    ds8a = state.get_device("test-lab/db845c-01")
    retired_ticket = ds8a.ticket_key
    await handler.handle_device_event(
        "test-lab", "db845c-01", "dragonboard", "Bad", "Idle", "2026-03-05T11:01:00Z"
    )
    ds8b = state.get_device("test-lab/db845c-01")
    assert ds8b.ticket_key == retired_ticket, "Should reuse ticket when going Retired -> Bad"
    assert ds8b.health == "Bad"
    print(f"  PASS: ticket={ds8b.ticket_key} (same ticket, Retired -> Bad)")

    print("\n" + "=" * 50)
    print("ALL TESTS PASSED")


if __name__ == "__main__":
    import os
    # Clean up any previous test state
    if os.path.exists("test_state.json"):
        os.remove("test_state.json")
    asyncio.run(run_tests())
    os.remove("test_state.json")
