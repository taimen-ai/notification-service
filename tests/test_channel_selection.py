"""Channel selection through the API: preferences, mandatory rules, quiet hours (FR-002)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from notification_service.auth import SCOPE_ADMIN, SCOPE_READ, SCOPE_SEND

from conftest import Harness, Person, auth, notification, to_principal


async def patch_prefs(harness: Harness, token: str, body: dict[str, Any]) -> Any:
    response = await harness.client.patch(
        "/api/v1/me/notification-preferences", json=body, headers=auth(token)
    )
    assert response.status_code == 200, response.text
    return response.json()


async def add_rule(harness: Harness, token: str, type_: str, channel: str) -> Any:
    response = await harness.client.post(
        "/api/v1/mandatory-rules",
        json={"type": type_, "channel": channel},
        headers=auth(token),
    )
    assert response.status_code in (200, 201), response.text
    return response.json()


async def channels_of(
    harness: Harness, token: str, person: Person, key: str, **fields: Any
) -> dict:
    response = await harness.client.post(
        "/api/v1/notifications",
        json=notification(to_principal(person), **fields),
        headers={**auth(token), "Idempotency-Key": key},
    )
    assert response.status_code == 201, response.text
    return {d["channel"]: d for d in response.json()["deliveries"]}


async def test_email_is_selected_once_an_address_is_set(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    me = token(person.iam_principal_id)

    assert set(await channels_of(harness, token(), person, "before")) == {"web"}
    await patch_prefs(harness, me, {"email": "person@example.com"})
    assert set(await channels_of(harness, token(), person, "after")) == {"web", "email"}


async def test_recipient_turns_a_type_off_per_channel(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    me = token(person.iam_principal_id)
    await patch_prefs(
        harness,
        me,
        {
            "email": "person@example.com",
            "preferences": [
                {"type": "work.*", "channel": "email", "enabled": False},
                {"type": "digest.weekly", "channel": "web", "enabled": False},
            ],
        },
    )

    assert set(await channels_of(harness, token(), person, "a")) == {"web"}
    assert set(await channels_of(harness, token(), person, "b", type="digest.weekly")) == {"email"}

    # Removing the preference returns to the default.
    await patch_prefs(
        harness, me, {"preferences": [{"type": "work.*", "channel": "email", "enabled": None}]}
    )
    assert set(await channels_of(harness, token(), person, "c")) == {"web", "email"}


async def test_mandatory_rule_delivers_despite_opt_out(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    me = token(person.iam_principal_id)
    admin = token(scopes=(SCOPE_ADMIN,))
    await patch_prefs(
        harness,
        me,
        {
            "email": "person@example.com",
            "preferences": [
                {"type": "*", "channel": "email", "enabled": False},
                {"type": "*", "channel": "web", "enabled": False},
            ],
        },
    )
    assert await channels_of(harness, token(), person, "none") == {}

    await add_rule(harness, admin, "work.*", "email")
    chosen = await channels_of(harness, token(), person, "forced")

    assert set(chosen) == {"email"}
    assert chosen["email"]["mandatory"] is True
    assert chosen["email"]["status"] == "pending"


async def test_mandatory_channel_without_address_is_journaled(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    await add_rule(harness, token(scopes=(SCOPE_ADMIN,)), "*", "email")

    chosen = await channels_of(harness, token(), person, "k")

    assert chosen["email"]["status"] == "failed"
    assert chosen["email"]["lastError"] == "recipient_unreachable"
    assert chosen["web"]["status"] == "pending"


async def test_mandatory_rules_are_per_tenant(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    import uuid

    person = make_person()
    await patch_prefs(
        harness,
        token(person.iam_principal_id),
        {"preferences": [{"type": "*", "channel": "web", "enabled": False}]},
    )
    await add_rule(harness, token(scopes=(SCOPE_ADMIN,), tenant=uuid.uuid4()), "*", "web")

    assert await channels_of(harness, token(), person, "k") == {}


async def test_quiet_hours_postpone_email_but_not_the_inbox(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    now = datetime.now(UTC)
    start = (now - timedelta(hours=1)).strftime("%H:%M")
    end = (now + timedelta(hours=1)).strftime("%H:%M")
    await patch_prefs(
        harness,
        token(person.iam_principal_id),
        {
            "email": "person@example.com",
            "quietHours": {"start": start, "end": end, "timezone": "UTC"},
        },
    )

    chosen = await channels_of(harness, token(), person, "k")
    await harness.drain()

    email_at = datetime.fromisoformat(chosen["email"]["nextAttemptAt"])
    assert email_at > now + timedelta(minutes=30)
    inbox = await harness.client.get(
        "/api/v1/me/notifications", headers=auth(token(person.iam_principal_id))
    )
    assert len(inbox.json()["items"]) == 1


async def test_mandatory_delivery_ignores_quiet_hours(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    now = datetime.now(UTC)
    await patch_prefs(
        harness,
        token(person.iam_principal_id),
        {
            "email": "person@example.com",
            "quietHours": {
                "start": (now - timedelta(hours=1)).strftime("%H:%M"),
                "end": (now + timedelta(hours=1)).strftime("%H:%M"),
                "timezone": "UTC",
            },
        },
    )
    await add_rule(harness, token(scopes=(SCOPE_ADMIN,)), "work.review_requested", "email")

    chosen = await channels_of(harness, token(), person, "k")

    assert datetime.fromisoformat(chosen["email"]["nextAttemptAt"]) <= datetime.now(UTC)


async def test_mandatory_rule_management_is_for_admins(
    harness: Harness, token: Callable[..., str]
) -> None:
    admin = token(scopes=(SCOPE_ADMIN,))
    user = token(scopes=(SCOPE_READ, SCOPE_SEND))

    denied = await harness.client.post(
        "/api/v1/mandatory-rules", json={"type": "*", "channel": "web"}, headers=auth(user)
    )
    created = await add_rule(harness, admin, "work.*", "email")
    again = await harness.client.post(
        "/api/v1/mandatory-rules", json={"type": "work.*", "channel": "email"}, headers=auth(admin)
    )
    unknown = await harness.client.post(
        "/api/v1/mandatory-rules", json={"type": "*", "channel": "pigeon"}, headers=auth(admin)
    )
    listed = await harness.client.get("/api/v1/mandatory-rules", headers=auth(admin))
    removed = await harness.client.delete(
        f"/api/v1/mandatory-rules/{created['id']}", headers=auth(admin)
    )
    removed_again = await harness.client.delete(
        f"/api/v1/mandatory-rules/{created['id']}", headers=auth(admin)
    )

    assert denied.status_code == 403
    assert again.status_code == 200 and again.json()["id"] == created["id"]
    assert unknown.status_code == 422
    assert [r["type"] for r in listed.json()] == ["work.*"]
    assert removed.status_code == 204
    assert removed_again.status_code == 404
