"""Recipient settings: preferences, quiet hours, email address."""

from __future__ import annotations

from collections.abc import Callable

from notification_service.auth import SCOPE_ADMIN

from conftest import Harness, Person, auth

URL = "/api/v1/me/notification-preferences"


async def test_defaults_and_round_trip(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    me = auth(token(make_person().iam_principal_id))
    empty = await harness.client.get(URL, headers=me)
    assert empty.json() == {
        "channels": ["web", "email"],
        "preferences": [],
        "quietHours": None,
        "addresses": [],
        "mandatory": [],
    }

    await harness.client.post(
        "/api/v1/mandatory-rules",
        json={"type": "security.*", "channel": "email"},
        headers=auth(token(scopes=(SCOPE_ADMIN,))),
    )
    changed = await harness.client.patch(
        URL,
        json={
            "preferences": [{"type": "work.*", "channel": "email", "enabled": False}],
            "quietHours": {"start": "22:00", "end": "07:30", "timezone": "Europe/Moscow"},
            "email": "person@example.com",
        },
        headers=me,
    )
    body = changed.json()
    assert body["preferences"] == [{"type": "work.*", "channel": "email", "enabled": False}]
    assert body["quietHours"] == {"start": "22:00", "end": "07:30", "timezone": "Europe/Moscow"}
    assert [(a["channel"], a["address"], a["disabledAt"]) for a in body["addresses"]] == [
        ("email", "person@example.com", None)
    ]
    assert [r["type"] for r in body["mandatory"]] == ["security.*"]

    # Absent fields stay; explicit nulls clear.
    kept = await harness.client.patch(URL, json={}, headers=me)
    assert kept.json()["quietHours"] is not None
    cleared = await harness.client.patch(URL, json={"quietHours": None, "email": None}, headers=me)
    assert cleared.json()["quietHours"] is None
    assert cleared.json()["addresses"] == []
    assert cleared.json()["preferences"] == body["preferences"]


async def test_invalid_settings_are_rejected(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    me = auth(token(make_person().iam_principal_id))
    invalid = [
        {"preferences": [{"type": "work.*", "channel": "pigeon", "enabled": True}]},
        {"preferences": [{"type": "work*", "channel": "web", "enabled": True}]},
        {"quietHours": {"start": "22:00", "end": "07:00", "timezone": "Mars/Olympus"}},
        {"email": "not-an-address"},
        {"unknown": True},
    ]
    for body in invalid:
        response = await harness.client.patch(URL, json=body, headers=me)
        assert response.status_code == 422, body


async def test_settings_belong_to_the_caller(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    alice, bob = make_person(), make_person()
    await harness.client.patch(
        URL, json={"email": "alice@example.com"}, headers=auth(token(alice.iam_principal_id))
    )
    bobs = await harness.client.get(URL, headers=auth(token(bob.iam_principal_id)))
    assert bobs.json()["addresses"] == []
