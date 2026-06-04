"""End-to-end test using the mock Jira server and synthetic LAVA events."""

import asyncio
import sys

import requests

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

    print("\n=== Test 9: Closed ticket -> Good event should NOT add comment ===")
    # Create a device with a ticket, then close the ticket via mock helper
    await handler.handle_device_event(
        "test-lab", "hikey-01", "hikey", "Bad", "Idle", "2026-03-05T12:00:00Z"
    )
    ds9 = state.get_device("test-lab/hikey-01")
    closed_ticket = ds9.ticket_key
    # Close the ticket in mock Jira
    requests.put(f"{JIRA_URL}/test/close-ticket/{closed_ticket}")
    # Send a Good event — should NOT add a comment since ticket is closed
    await handler.handle_device_event(
        "test-lab", "hikey-01", "hikey", "Good", "Idle", "2026-03-05T12:01:00Z"
    )
    # Device should be removed from state since ticket is closed
    ds9_after = state.get_device("test-lab/hikey-01")
    assert ds9_after is None, "Device should be removed from state when ticket is closed"
    print(f"  PASS: device removed from state (closed ticket {closed_ticket} not commented)")

    print("\n=== Test 10: Closed ticket -> Bad event should create NEW ticket ===")
    await handler.handle_device_event(
        "test-lab", "hikey-01", "hikey", "Bad", "Idle", "2026-03-05T12:02:00Z"
    )
    ds10 = state.get_device("test-lab/hikey-01")
    assert ds10 is not None, "Should create a new ticket"
    assert ds10.ticket_key != closed_ticket, "Should be a NEW ticket, not the closed one"
    print(f"  PASS: new ticket={ds10.ticket_key} (old closed ticket was {closed_ticket})")

    print("\n" + "=" * 50)
    print("=== Worker Tests ===")

    print("\n=== Test W1: Worker goes Offline -> should create ticket ===")
    await handler.handle_worker_event(
        "test-lab", "worker-01", "Active", "Offline", "2026-06-04T10:00:00Z"
    )
    ws1 = state.get_worker("test-lab/worker-01")
    assert ws1 is not None, "Worker should be tracked in state"
    assert ws1.health == "Active"
    assert ws1.state == "Offline"
    worker_ticket_1 = ws1.ticket_key
    print(f"  PASS: ticket={worker_ticket_1}, health={ws1.health}, state={ws1.state}")

    print("\n=== Test W2: Same worker, same condition -> should NOT create duplicate ===")
    await handler.handle_worker_event(
        "test-lab", "worker-01", "Active", "Offline", "2026-06-04T10:01:00Z"
    )
    ws2 = state.get_worker("test-lab/worker-01")
    assert ws2.ticket_key == worker_ticket_1, "Should reuse same ticket"
    print(f"  PASS: still ticket={ws2.ticket_key} (no duplicate)")

    print("\n=== Test W3: Worker offline, health also -> Maintenance -> should comment ===")
    await handler.handle_worker_event(
        "test-lab", "worker-01", "Maintenance", "Offline", "2026-06-04T10:02:00Z"
    )
    ws3 = state.get_worker("test-lab/worker-01")
    assert ws3.ticket_key == worker_ticket_1, "Should reuse same ticket"
    assert ws3.health == "Maintenance"
    assert ws3.state == "Offline"
    print(f"  PASS: ticket={ws3.ticket_key}, condition updated to health={ws3.health} state={ws3.state}")

    print("\n=== Test W4: Worker recovers (Active + Online) -> should comment and keep in state ===")
    await handler.handle_worker_event(
        "test-lab", "worker-01", "Active", "Online", "2026-06-04T10:03:00Z"
    )
    ws4 = state.get_worker("test-lab/worker-01")
    assert ws4 is not None, "Worker should still be in state (ticket is open)"
    assert ws4.ticket_key == worker_ticket_1
    assert ws4.health == "Active"
    assert ws4.state == "Online"
    print(f"  PASS: ticket={ws4.ticket_key}, health={ws4.health}, state={ws4.state} (still in state)")

    print("\n=== Test W5: Worker with no state goes Online/Active -> no action ===")
    await handler.handle_worker_event(
        "test-lab", "worker-99", "Active", "Online", "2026-06-04T10:04:00Z"
    )
    ws5 = state.get_worker("test-lab/worker-99")
    assert ws5 is None, "Untracked worker recovering should not be added to state"
    print(f"  PASS: no state created for untracked recovering worker")

    print("\n=== Test W6: Worker health Retired -> should create ticket ===")
    await handler.handle_worker_event(
        "test-lab", "worker-02", "Retired", "Online", "2026-06-04T10:05:00Z"
    )
    ws6 = state.get_worker("test-lab/worker-02")
    assert ws6 is not None
    assert ws6.health == "Retired"
    worker_ticket_2 = ws6.ticket_key
    print(f"  PASS: ticket={worker_ticket_2}, health={ws6.health}")

    print("\n=== Test W7: Closed ticket -> recovery should remove worker from state ===")
    await handler.handle_worker_event(
        "test-lab", "worker-03", "Active", "Offline", "2026-06-04T11:00:00Z"
    )
    ws7 = state.get_worker("test-lab/worker-03")
    closed_worker_ticket = ws7.ticket_key
    requests.put(f"{JIRA_URL}/test/close-ticket/{closed_worker_ticket}")
    await handler.handle_worker_event(
        "test-lab", "worker-03", "Active", "Online", "2026-06-04T11:01:00Z"
    )
    ws7_after = state.get_worker("test-lab/worker-03")
    assert ws7_after is None, "Worker should be removed from state when ticket is closed"
    print(f"  PASS: worker removed from state (closed ticket {closed_worker_ticket})")

    print("\n=== Test W8: Closed ticket -> bad event should create NEW ticket ===")
    await handler.handle_worker_event(
        "test-lab", "worker-03", "Active", "Offline", "2026-06-04T11:02:00Z"
    )
    ws8 = state.get_worker("test-lab/worker-03")
    assert ws8 is not None
    assert ws8.ticket_key != closed_worker_ticket, "Should be a NEW ticket"
    print(f"  PASS: new ticket={ws8.ticket_key} (old closed ticket was {closed_worker_ticket})")

    print("\n" + "=" * 50)
    print("ALL TESTS PASSED")


if __name__ == "__main__":
    import os
    # Clean up any previous test state
    if os.path.exists("test_state.json"):
        os.remove("test_state.json")
    asyncio.run(run_tests())
    os.remove("test_state.json")
